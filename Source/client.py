import socket
import json
import threading
import time
import os
import signal
import sys
import hashlib
import base64
import queue

# Configuration
MULTIPLIER = 8
CHUNK_SIZE = 1024 * MULTIPLIER
RECEIVE_SIZE = CHUNK_SIZE * 8
MAX_TIMEOUTS = 5
RETRY_INTERVAL = 1  # seconds between retries
MAX_PARALLEL_DOWNLOADS = 4

# Server information
server_address = ("10.250.91.1", 12345)

# Locks and shared data
progress_lock = threading.Lock()
processed_files = set()  # Track completed downloads
active_downloads = {}  # Track currently downloading files
shutdown_event = threading.Event()  # For graceful shutdown

class FileTransferProtocol:
    """Application-level protocol for reliable file transfer."""
    # Protocol constants
    START_CHUNK = 'START'
    DATA_CHUNK = 'DATA'
    END_CHUNK = 'END'
    ACK = 'ACK'
    NACK = 'NACK'
    ERROR = 'ERROR'
    
    REQUEST_CONST = ['LIST', 'DOWNLOAD']
    PROTO_CONST = [START_CHUNK, DATA_CHUNK, END_CHUNK, ACK, NACK, ERROR]

    @staticmethod
    def verify_checksum(data, expected_checksum):
        """Verify data integrity using MD5 checksum."""
        current_checksum = hashlib.md5(data).hexdigest()
        return current_checksum == expected_checksum

    @staticmethod
    def create_packet(packet_type, file_name, sequence, client_port, offset=None, data=None, checksum=None):
        """Create a structured packet for communication with server."""
        packet = {
            'type': packet_type,
            'file_name': file_name,
            'sequence': sequence,
            'client_port': client_port
        }
        
        if offset is not None:
            packet['offset'] = offset
            
        if data is not None:
            if isinstance(data, bytes):
                packet['data'] = base64.b64encode(data).decode('utf-8')
            else:
                packet['data'] = data
        
        if checksum is not None:
            packet['checksum'] = checksum
            
        return json.dumps(packet).encode()

def get_file_list(sock):
    """Get the list of available files from the server."""
    client_port = sock.getsockname()[1]
    request = FileTransferProtocol.create_packet('LIST', '', 0, client_port)
    sock.sendto(request, server_address)
    
    try:
        sock.settimeout(5)
        data, _ = sock.recvfrom(1024)
        print("\nAvailable files on server:")
        print("-------------------------")
        
        files = {}
        for line in data.decode().splitlines():
            if line:
                try:
                    name, size_str = line.split()
                    if size_str.endswith("MB"):
                        size = int(float(size_str.replace("MB", "")) * 1024 * 1024)
                    elif size_str.endswith("GB"):
                        size = int(float(size_str.replace("GB", "")) * 1024 * 1024 * 1024)
                    elif size_str.endswith("KB"):
                        size = int(float(size_str.replace("KB", "")) * 1024)
                    else:
                        size = int(size_str)  # Assume bytes
                    
                    files[name] = size
                    print(f"{name:<30} {size_to_human_readable(size):>10}")
                except ValueError:
                    print(f"Could not parse: {line}")
        
        print("-------------------------")
        return files
    except socket.timeout:
        print("Timeout while fetching file list. Server might be down.")
        return {}

def parse_response(data):
    """Parse the server response."""
    try:
        return json.loads(data.decode())
    except (json.JSONDecodeError, UnicodeDecodeError):
        print(f"Failed to decode response: {data[:50]}...")
        return None

def size_to_human_readable(size_bytes):
    """Convert byte size to human-readable format."""
    if size_bytes >= 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    elif size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes} B"

def receive_thread(sock, file_name, part_num, received_data, progress, total_chunks_ref, completion_event):
    """Dedicated thread for receiving data on a specific socket."""
    client_port = sock.getsockname()[1]
    timeout_count = 0
    last_sequence = -1
    
    while not completion_event.is_set() and not shutdown_event.is_set():
        try:
            sock.settimeout(5)
            rawdata, _ = sock.recvfrom(RECEIVE_SIZE)
            
            if not rawdata:
                timeout_count += 1
                if timeout_count >= MAX_TIMEOUTS:
                    print(f"Too many empty responses for {file_name} part {part_num}")
                    break
                continue
            
            packet = parse_response(rawdata)
            if not packet:
                continue
                
            # Reset timeout counter on successful packet reception
            timeout_count = 0
            
            if packet['type'] == FileTransferProtocol.START_CHUNK:
                total_chunks = packet.get('total_chunks', -1)
                total_chunks_ref[0] = total_chunks
                print(f"Received START packet for {file_name} part {part_num}: {total_chunks} chunks expected")
                
                # Send ACK for START packet
                ack_packet = FileTransferProtocol.create_packet(
                    FileTransferProtocol.ACK, 
                    file_name,
                    0,
                    client_port
                )
                sock.sendto(ack_packet, server_address)
                
            elif packet['type'] == FileTransferProtocol.DATA_CHUNK:
                sequence = packet['sequence']
                chunk_data = base64.b64decode(packet['data'])
                checksum = packet['checksum']
                
                # Verify data integrity
                if FileTransferProtocol.verify_checksum(chunk_data, checksum):
                    # Send ACK for this chunk
                    ack_packet = FileTransferProtocol.create_packet(
                        FileTransferProtocol.ACK,
                        file_name,
                        sequence,
                        client_port
                    )
                    sock.sendto(ack_packet, server_address)
                    
                    # Store the chunk data
                    with progress_lock:
                        received_data[sequence] = chunk_data
                        received_bytes = sum(len(chunk) for chunk in received_data.values())
                        progress[part_num] = (received_bytes, progress[part_num][1])
                        
                    # For large files, provide sequence feedback
                    if sequence > last_sequence + 100 or sequence % 100 == 0:
                        # print(f"Received chunk {sequence} for {file_name} part {part_num}")
                        last_sequence = sequence
                else:
                    # Send NACK for corrupted chunk
                    nack_packet = FileTransferProtocol.create_packet(
                        FileTransferProtocol.NACK,
                        file_name,
                        sequence,
                        client_port
                    )
                    sock.sendto(nack_packet, server_address)
                    print(f"Checksum mismatch for chunk {sequence} in {file_name} part {part_num}")
                    
            elif packet['type'] == FileTransferProtocol.END_CHUNK:
                print(f"Received END packet for {file_name} part {part_num}")
                
                # Send ACK for END packet
                ack_packet = FileTransferProtocol.create_packet(
                    FileTransferProtocol.ACK,
                    file_name,
                    -1,
                    client_port
                )
                sock.sendto(ack_packet, server_address)
                
                # Check if we have all chunks
                if total_chunks_ref[0] > 0 and len(received_data) >= total_chunks_ref[0]:
                    completion_event.set()
                    break
                
            elif packet['type'] == FileTransferProtocol.ERROR:
                error_msg = packet.get('data', 'Unknown error')
                print(f"Error for {file_name} part {part_num}: {error_msg}")
                completion_event.set()
                break
                
        except socket.timeout:
            timeout_count += 1
            print(f"Timeout ({timeout_count}/{MAX_TIMEOUTS}) while downloading {file_name} part {part_num}")
            if timeout_count >= MAX_TIMEOUTS:
                print(f"Download of {file_name} part {part_num} failed due to timeouts")
                break
        
        except Exception as e:
            print(f"Error in receive thread for {file_name} part {part_num}: {str(e)}")
            timeout_count += 1
            if timeout_count >= MAX_TIMEOUTS:
                break

def download_part(file_name, offset, length, output_file, part_num, progress):
    """Download a specific part of the file using enhanced UDP protocol."""
    # Create socket for this download part
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_port = 20000 + part_num  # Using different ports for each part
    
    try:
        sock.bind(("0.0.0.0", client_port))
    except OSError:
        # Port might be in use, try a different one
        client_port = 25000 + part_num + int(time.time() % 1000)
        sock.bind(("0.0.0.0", client_port))
    
    print(f"Started download of {file_name} part {part_num} on port {client_port}")
    
    # Initialize data structures
    received_data = {}
    total_chunks_ref = [0]  # Using a list to store by reference
    completion_event = threading.Event()
    
    # Set initial progress
    with progress_lock:
        progress[part_num] = (0, length)
    
    # Start dedicated receive thread
    recv_thread = threading.Thread(
        target=receive_thread,
        args=(sock, file_name, part_num, received_data, progress, total_chunks_ref, completion_event)
    )
    recv_thread.daemon = True
    recv_thread.start()
    
    # Send download request
    request_packet = FileTransferProtocol.create_packet(
        'DOWNLOAD',
        file_name,
        0,
        client_port,
        offset=offset
    )
    sock.sendto(request_packet, server_address)
    
    # Wait for completion or timeout
    max_wait_time = 60 * 2  # 5 minutes timeout for whole part
    wait_start = time.time()
    
    while not completion_event.is_set() and time.time() - wait_start < max_wait_time:
        if shutdown_event.is_set():
            break
        time.sleep(1)
    
    if not completion_event.is_set():
        print(f"Download of {file_name} part {part_num} timed out")
    
    # Close socket
    sock.close()
    
    # Write received data to file if we have all chunks
    if total_chunks_ref[0] > 0 and len(received_data) >= total_chunks_ref[0]:
        file_data = b''.join(received_data[i] for i in sorted(received_data.keys()) if i < total_chunks_ref[0])
        
        with open(output_file, "r+b") as f:
            f.seek(offset)
            f.write(file_data[:length])
        
        return True
    else:
        print(f"Incomplete download: {file_name} part {part_num} - Got {len(received_data)} of {total_chunks_ref[0]} chunks")
        return False

def download_file(file_name, size):
    """Download a file using multiple sockets in parallel."""
    # Define download parts
    num_parts = min(MAX_PARALLEL_DOWNLOADS, 4)  # Maximum 4 parallel downloads
    part_size = size // num_parts
    
    offsets = []
    lengths = []
    
    for i in range(num_parts):
        if i < num_parts - 1:
            offsets.append(i * part_size)
            lengths.append(part_size)
        else:
            # Last part might be larger to account for division remainder
            offsets.append(i * part_size)
            lengths.append(size - i * part_size)
    
    # Create a file of the right size
    try:
        # Create directory if it doesn't exist
        download_dir = "downloads"
        os.makedirs(download_dir, exist_ok=True)
        
        output_file = os.path.join(download_dir, file_name)
        with open(output_file, "wb") as f:
            f.truncate(size)
        
        print(f"Created file {output_file} of size {size_to_human_readable(size)}")
    except Exception as e:
        print(f"Error creating output file: {e}")
        return False
    
    # Track download progress
    progress = {i: (0, lengths[i]) for i in range(num_parts)}
    
    # Start download threads
    threads = []
    for i in range(num_parts):
        t = threading.Thread(
            target=download_part,
            args=(file_name, offsets[i], lengths[i], output_file, i, progress)
        )
        t.daemon = True
        t.start()
        threads.append(t)
    
    # Track and display progress
    start_time = time.time()
    complete = False
    
    print(f"\nDownloading {file_name} ({size_to_human_readable(size)})...")
    
    while any(t.is_alive() for t in threads) and not shutdown_event.is_set():
        # Calculate overall progress
        with progress_lock:
            total_received = sum(recv for recv, _ in progress.values())
            total_to_receive = sum(total for _, total in progress.values())
            percentage = (total_received / total_to_receive * 100) if total_to_receive > 0 else 0
            
            elapsed = time.time() - start_time
            speed = total_received / elapsed if elapsed > 0 else 0
            
            # Clear previous line and update progress
            sys.stdout.write("\r" + " " * 80)  # Clear line
            sys.stdout.write(f"\rProgress: {percentage:.1f}% - {size_to_human_readable(total_received)}/{size_to_human_readable(total_to_receive)} - {size_to_human_readable(speed)}/s")
            sys.stdout.flush()
        
        # Check for completion
        if total_received >= total_to_receive * 0.99:  # Allow slight margin for rounding errors
            complete = True
        
        time.sleep(0.5)
    
    # Wait for all threads to finish
    for t in threads:
        t.join()
    
    # Final status update
    elapsed = time.time() - start_time
    
    # Verify file size
    try:
        actual_size = os.path.getsize(output_file)
        if actual_size == size:
            print(f"\nDownload of {file_name} completed in {elapsed:.1f} seconds ({size_to_human_readable(size/elapsed)}/s)")
            with progress_lock:
                processed_files.add(file_name)
            return True
        else:
            print(f"\nDownload of {file_name} may be incomplete. Expected: {size} bytes, Got: {actual_size} bytes")
            return False
    except Exception as e:
        print(f"\nError verifying file size: {e}")
        return False

def progress_monitor():
    """Thread to monitor and display download progress."""
    while not shutdown_event.is_set():
        with progress_lock:
            if active_downloads:
                sys.stdout.write("\n")
                for file_name, info in active_downloads.items():
                    progress = info.get('progress', 0)
                    speed = info.get('speed', 0)
                    sys.stdout.write(f"{file_name}: {progress:.1f}% - {size_to_human_readable(speed)}/s\n")
                sys.stdout.write("\033[F" * len(active_downloads))  # Move cursor up
        
        time.sleep(1)

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    print("\nReceived shutdown signal. Closing connections...")
    shutdown_event.set()
    time.sleep(1)  # Give threads time to close
    sys.exit(0)

def watch_input_file(file_list):
    """Watch the input file for changes and start downloads as needed."""
    last_check_time = 0
    input_file = "input.txt"
    
    while not shutdown_event.is_set():
        try:
            # Check if input file exists and has been modified
            if os.path.exists(input_file):
                mod_time = os.path.getmtime(input_file)
                if mod_time > last_check_time:
                    last_check_time = mod_time
                    
                    # Read file
                    with open(input_file, "r") as f:
                        files_to_download = [line.strip() for line in f if line.strip()]
                    
                    # Process each file
                    for file_name in files_to_download:
                        if file_name in processed_files:
                            continue  # Skip already processed files
                        
                        if file_name in file_list:
                            size = file_list[file_name]
                            print(f"\nStarting download of {file_name} ({size_to_human_readable(size)})")
                            
                            # Start download in a separate thread
                            download_thread = threading.Thread(
                                target=download_file,
                                args=(file_name, size)
                            )
                            download_thread.daemon = True
                            download_thread.start()
                            
                            # Wait for this download to complete before starting the next
                            download_thread.join()
                        else:
                            print(f"\nFile {file_name} not found on server")
            
            time.sleep(5)  # Check for changes every 5 seconds
            
        except Exception as e:
            print(f"Error watching input file: {e}")
            time.sleep(5)

def main():
    """Main client logic for UDP file transfer."""
    signal.signal(signal.SIGINT, signal_handler)  # Catch Ctrl+C
    
    print("UDP File Transfer Client")
    print("======================\n")
    
    # Create download directory if it doesn't exist
    os.makedirs("downloads", exist_ok=True)
    
    # Create UDP socket for initial communication
    main_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    main_sock.bind(("0.0.0.0", 19999))
    
    # Get file list from server
    file_list = get_file_list(main_sock)
    main_sock.close()
    
    if not file_list:
        print("No files available from server or server unreachable.")
        return
    
    # Start input file watcher
    watcher_thread = threading.Thread(target=watch_input_file, args=(file_list,))
    watcher_thread.daemon = True
    watcher_thread.start()
    
    # Main loop to keep program running
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)

if __name__ == "__main__":
    main()