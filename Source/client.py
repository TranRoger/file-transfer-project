import socket
import json
import time
import os
import signal
import sys
import hashlib
import base64
import select
from collections import namedtuple

MULTIPLIER = 4
CHUNK_SIZE = 1024 * MULTIPLIER
RECEIVE_SIZE = CHUNK_SIZE * 8

# Define download states
WAITING_ACK = 0
RECEIVING_START = 1
RECEIVING_DATA = 2
RECEIVING_END = 3
COMPLETED = 4
ERROR = 5

# Structure to track download state
DownloadState = namedtuple('DownloadState', [
    'state', 'file_name', 'offset', 'length', 'part_num',
    'total_chunks', 'received_chunks', 'received_data', 'output_file',
    'last_activity', 'socket'
])

server_address = ("192.168.1.23", 12345)
processed = set()  # Track processed files

# Global variables for progress tracking
progress_data = {}
last_progress_update = 0

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    print("\nExiting due to Ctrl+C...")
    sys.exit(0)

class FileTransferProtocol:
    """Application-level protocol for reliable file transfer."""
    # Protocol constants
    START_CHUNK = 'START'
    DATA_CHUNK = 'DATA'
    END_CHUNK = 'END'
    ACK = 'ACK'
    NACK = 'NACK'

    @staticmethod
    def verify_checksum(data, expected_checksum):
        """Verify data integrity using MD5 checksum."""
        current_checksum = hashlib.md5(data).hexdigest()
        return current_checksum == expected_checksum

def get_file_list():
    """Get the list of available files from the server."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    request = json.dumps({'type': 'LIST'}).encode()
    sock.sendto(request, server_address)
    
    # Set a timeout for the response
    sock.settimeout(5)
    try:
        data, _ = sock.recvfrom(1024)
        files = {}
        for line in data.decode().splitlines():
            if line:
                name, size_str = line.split()
                if size_str.endswith("MB"):
                    size = int(size_str.replace("MB", "")) * 1024 * 1024
                elif size_str.endswith("GB"):
                    size = int(size_str.replace("GB", "")) * 1024 * 1024 * 1024
                files[name] = size
        return files
    except socket.timeout:
        print("Timeout waiting for file list")
        return {}
    finally:
        sock.close()

def parse_response(data):
    """Parse the server response."""
    try:
        packet = json.loads(data.decode())
        return packet
    except json.JSONDecodeError:
        print("Failed to decode JSON response")
        return None

def create_download_socket(part_num):
    """Create a socket for downloading a file part."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    port = 20000 + part_num
    sock.bind(("0.0.0.0", port))
    sock.setblocking(False)
    return sock

def start_download(sock, file_name, offset, length, part_num):
    """Start downloading a file part."""
    # Send download request
    request = {
        'type': "DOWNLOAD",
        'file_name': file_name,
        'offset': offset,
        'length': length,
    }
    sock.sendto(json.dumps(request).encode("utf-8"), server_address)
    
    # Initialize download state
    return DownloadState(
        state=WAITING_ACK,
        file_name=file_name,
        offset=offset,
        length=length,
        part_num=part_num,
        total_chunks=0,
        received_chunks={},
        received_data={},
        output_file=file_name,
        last_activity=time.time(),
        socket=sock
    )

def handle_ack_download(state, parsed):
    """Handle ACK response for download request."""
    if parsed['file_name'] == state.file_name and parsed['offset'] == state.offset:
        print(f"Received ACK-DOWNLOAD for {state.file_name} part {state.part_num+1}")
        return DownloadState(
            state=RECEIVING_START,
            file_name=state.file_name,
            offset=state.offset,
            length=state.length,
            part_num=state.part_num,
            total_chunks=state.total_chunks,
            received_chunks=state.received_chunks,
            received_data=state.received_data,
            output_file=state.output_file,
            last_activity=time.time(),
            socket=state.socket
        )
    return state

def handle_start_packet(state, parsed):
    """Handle START packet."""
    if parsed['file_name'] == state.file_name:
        total_chunks = parsed.get('total_chunks', 0)
        received_chunks = {i: False for i in range(total_chunks)}
        
        # Send ACK for START packet
        ack_packet = json.dumps({
            'type': FileTransferProtocol.ACK,
            'file_name': state.file_name,
            'sequence': 0,
            'offset': state.offset,
        }).encode()
        state.socket.sendto(ack_packet, server_address)
        print(f"Received START packet for {state.file_name} part {state.part_num+1}")
        
        return DownloadState(
            state=RECEIVING_DATA,
            file_name=state.file_name,
            offset=state.offset,
            length=state.length,
            part_num=state.part_num,
            total_chunks=total_chunks,
            received_chunks=received_chunks,
            received_data={},
            output_file=state.output_file,
            last_activity=time.time(),
            socket=state.socket
        )
    return state

def update_progress_display():
    """Update the progress display with a status bar."""
    global last_progress_update
    current_time = time.time()
    
    # Only update display every 0.5 seconds to avoid flickering
    if current_time - last_progress_update < 0.5:
        return
    
    last_progress_update = current_time
    
    if not progress_data:
        return
    
    # Clear the current lines
    for _ in range(len(progress_data) + 2):
        sys.stdout.write('\033[F\033[K')  # Move up and clear line
    
    # Display header
    print("Download Progress:")
    print("-" * 60)
    
    # Display progress for each file part
    for key, data in progress_data.items():
        file_name = data['file_name']
        part_num = data['part_num']
        progress = data['progress']
        speed = data.get('speed', 0)
        
        # Create progress bar
        bar_length = 30
        filled_length = int(bar_length * progress / 100)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        # Format speed
        if speed > 1024 * 1024:
            speed_str = f"{speed / (1024 * 1024):.1f} MB/s"
        elif speed > 1024:
            speed_str = f"{speed / 1024:.1f} KB/s"
        else:
            speed_str = f"{speed:.0f} B/s"
        
        print(f"{file_name} part {part_num+1}: [{bar}] {progress:5.1f}% {speed_str}")

def calculate_speed(key, bytes_received):
    """Calculate download speed for a specific download."""
    current_time = time.time()
    
    if key not in progress_data:
        progress_data[key] = {
            'last_time': current_time,
            'last_bytes': 0,
            'speed': 0
        }
    
    time_diff = current_time - progress_data[key]['last_time']
    if time_diff >= 1.0:  # Update speed every second
        bytes_diff = bytes_received - progress_data[key]['last_bytes']
        speed = bytes_diff / time_diff
        progress_data[key]['speed'] = speed
        progress_data[key]['last_time'] = current_time
        progress_data[key]['last_bytes'] = bytes_received

def handle_data_packet(state, parsed):
    """Handle DATA packet."""
    if parsed['file_name'] == state.file_name:
        sequence = parsed['sequence']
        chunk_data = base64.b64decode(parsed['data'])
        checksum = parsed['checksum']
        
        # Verify checksum
        if FileTransferProtocol.verify_checksum(chunk_data, checksum):
            # Store the sequence number that was received
            state.received_chunks[sequence-1] = True  # Adjust for 1-based sequence
            
            # ACK the chunk
            ack_packet = json.dumps({
                'type': FileTransferProtocol.ACK,
                'sequence': sequence,
                'file_name': state.file_name,
                'offset': state.offset
            }).encode()
            state.socket.sendto(ack_packet, server_address)
            
            # Store the chunk
            state.received_data[sequence-1] = chunk_data
            
            # Calculate progress and speed
            progress = len(state.received_data) / state.total_chunks * 100 if state.total_chunks > 0 else 0
            bytes_received = len(state.received_data) * CHUNK_SIZE
            
            # Update progress tracking
            key = f"{state.file_name}_part_{state.part_num}"
            calculate_speed(key, bytes_received)
            
            progress_data[key] = {
                **progress_data.get(key, {}),
                'file_name': state.file_name,
                'part_num': state.part_num,
                'progress': progress
            }
            
            # Update display
            update_progress_display()
            
            return DownloadState(
                state=RECEIVING_DATA,
                file_name=state.file_name,
                offset=state.offset,
                length=state.length,
                part_num=state.part_num,
                total_chunks=state.total_chunks,
                received_chunks=state.received_chunks,
                received_data=state.received_data,
                output_file=state.output_file,
                last_activity=time.time(),
                socket=state.socket
            )
        else:
            # Send NACK if checksum fails
            nack_packet = json.dumps({
                'type': FileTransferProtocol.NACK,
                'sequence': sequence,
                'file_name': state.file_name,
                'offset': state.offset
            }).encode()
            state.socket.sendto(nack_packet, server_address)
    
    return state

def handle_end_packet(state, parsed):
    """Handle END packet."""
    if parsed['file_name'] == state.file_name:
        # ACK the END packet
        ack_packet = json.dumps({
            'type': FileTransferProtocol.ACK,
            'file_name': state.file_name,
            'sequence': 0,
            'offset': state.offset
        }).encode()
        state.socket.sendto(ack_packet, server_address)
        
        # Check if all chunks are received
        if all(state.received_chunks.values()) and len(state.received_data) == state.total_chunks:
            # Write data to file
            try:
                # Reconstruct file data
                file_data = b''.join(state.received_data[i] for i in range(state.total_chunks))
                
                with open(state.output_file, "r+b") as f:
                    f.seek(state.offset)
                    f.write(file_data[:state.length])
                
                # Mark part as completed in progress
                key = f"{state.file_name}_part_{state.part_num}"
                if key in progress_data:
                    progress_data[key]['progress'] = 100.0
                    update_progress_display()
                
                return DownloadState(
                    state=COMPLETED,
                    file_name=state.file_name,
                    offset=state.offset,
                    length=state.length,
                    part_num=state.part_num,
                    total_chunks=state.total_chunks,
                    received_chunks=state.received_chunks,
                    received_data=state.received_data,
                    output_file=state.output_file,
                    last_activity=time.time(),
                    socket=state.socket
                )
            except Exception as e:
                print(f"\nError writing file: {e}")
                return DownloadState(
                    state=ERROR,
                    file_name=state.file_name,
                    offset=state.offset,
                    length=state.length,
                    part_num=state.part_num,
                    total_chunks=state.total_chunks,
                    received_chunks=state.received_chunks,
                    received_data=state.received_data,
                    output_file=state.output_file,
                    last_activity=time.time(),
                    socket=state.socket
                )
    return state

def check_timeouts(downloads, timeout=10):
    """Check for download timeouts and resend requests if needed."""
    current_time = time.time()
    for i, state in enumerate(downloads):
        if state and current_time - state.last_activity > timeout:
            if state.state == WAITING_ACK:
                # Resend download request
                request = {
                    'type': "DOWNLOAD",
                    'file_name': state.file_name,
                    'offset': state.offset,
                    'length': state.length,
                }
                state.socket.sendto(json.dumps(request).encode("utf-8"), server_address)
                
                # Update last activity time
                downloads[i] = DownloadState(
                    state=state.state,
                    file_name=state.file_name,
                    offset=state.offset,
                    length=state.length,
                    part_num=state.part_num,
                    total_chunks=state.total_chunks,
                    received_chunks=state.received_chunks,
                    received_data=state.received_data,
                    output_file=state.output_file,
                    last_activity=current_time,
                    socket=state.socket
                )

def download_file_with_select(file_name, file_size):
    """Download a file using select-based I/O multiplexing."""
    global progress_data
    
    # Check if file already exists and is complete
    if is_file_complete(file_name, file_size):
        print(f"File {file_name} already exists and is complete, skipping download")
        return True
    
    print(f"Starting download of {file_name} ({file_size/(1024*1024):.2f}MB)")
    
    # Initialize progress tracking for this file
    progress_data = {}
    
    # Add initial progress lines for status bar
    print("Download Progress:")
    print("-" * 60)
    for i in range(4):
        print(f"{file_name} part {i+1}: [{'░' * 30}]   0.0%   0 B/s")
    
    part_size = file_size // 4
    offsets = [0, part_size, 2 * part_size, 3 * part_size]
    lengths = [part_size, part_size, part_size, file_size - 3 * part_size]
    
    # Pre-allocate the output file
    with open(file_name, "wb") as f:
        f.truncate(file_size)
    
    # Create sockets and start downloads
    downloads = [None] * 4
    sockets = []
    
    for i in range(4):
        sock = create_download_socket(i)
        sockets.append(sock)
        downloads[i] = start_download(sock, file_name, offsets[i], lengths[i], i)
        
        # Initialize progress for this part
        key = f"{file_name}_part_{i}"
        progress_data[key] = {
            'file_name': file_name,
            'part_num': i,
            'progress': 0.0,
            'speed': 0,
            'last_time': time.time(),
            'last_bytes': 0
        }
    
    # Main event loop
    while True:
        # Check for timeouts
        check_timeouts(downloads)
        
        # Check if all downloads are completed
        all_completed = all(state and (state.state == COMPLETED or state.state == ERROR) for state in downloads)
        if all_completed:
            break
        
        # Use select to wait for incoming data
        ready_sockets, _, _ = select.select(sockets, [], [], 1.0)  # 1 second timeout
        
        for sock in ready_sockets:
            # Find which download this socket belongs to
            part_num = sockets.index(sock)
            state = downloads[part_num]
            
            if not state or state.state in [COMPLETED, ERROR]:
                continue
            
            try:
                data, _ = sock.recvfrom(RECEIVE_SIZE)
                parsed = parse_response(data)
                
                if not parsed:
                    continue
                
                # Handle different packet types
                if parsed['type'] == FileTransferProtocol.ACK and state.state == WAITING_ACK:
                    downloads[part_num] = handle_ack_download(state, parsed)
                elif parsed['type'] == FileTransferProtocol.START_CHUNK:
                    downloads[part_num] = handle_start_packet(state, parsed)
                elif parsed['type'] == FileTransferProtocol.DATA_CHUNK:
                    downloads[part_num] = handle_data_packet(state, parsed)
                elif parsed['type'] == FileTransferProtocol.END_CHUNK:
                    downloads[part_num] = handle_end_packet(state, parsed)
            except BlockingIOError:
                # No data available, continue
                continue
            except Exception as e:
                print(f"\nError handling packet for part {part_num}: {e}")
    
    # Clean up progress display
    progress_data = {}
    
    # Move cursor down past the progress bars
    for _ in range(4):
        print()
    
    # Clean up sockets
    for sock in sockets:
        sock.close()
    
    # Verify the download was successful
    success = all(state and state.state == COMPLETED for state in downloads)
    
    if success:
        # Final verification of file integrity
        if verify_file_integrity(file_name, file_size):
            print(f"File {file_name} downloaded and verified successfully")
            return True
        else:
            print(f"File {file_name} failed integrity check")
            # Remove corrupted file
            try:
                os.remove(file_name)
            except:
                pass
            return False
    else:
        # Remove incomplete file on failure
        try:
            if os.path.exists(file_name):
                os.remove(file_name)
        except:
            pass
        return False

def load_processed_files():
    """Load list of previously downloaded files from a persistent file."""
    try:
        with open("downloaded_files.txt", "r") as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()

def save_processed_file(file_name):
    """Save a successfully downloaded file to the persistent list."""
    with open("downloaded_files.txt", "a") as f:
        f.write(f"{file_name}\n")

def remove_processed_file(file_name):
    """Remove a file from the processed list."""
    try:
        with open("downloaded_files.txt", "r") as f:
            lines = f.readlines()
        
        with open("downloaded_files.txt", "w") as f:
            for line in lines:
                if line.strip() != file_name:
                    f.write(line)
    except FileNotFoundError:
        pass

def is_file_complete(file_name, expected_size):
    """Check if a file exists and has the correct size."""
    try:
        if os.path.exists(file_name):
            actual_size = os.path.getsize(file_name)
            if actual_size == expected_size:
                return True
            elif actual_size > 0:
                print(f"File {file_name} exists but size mismatch (expected: {expected_size}, actual: {actual_size})")
                # Remove incomplete file
                os.remove(file_name)
                return False
            else:
                # Empty file, remove it
                os.remove(file_name)
                return False
        return False
    except Exception as e:
        print(f"Error checking file {file_name}: {e}")
        return False

def verify_file_integrity(file_name, expected_size):
    """Verify that the downloaded file has the correct size and is readable."""
    try:
        if os.path.exists(file_name):
            actual_size = os.path.getsize(file_name)
            if actual_size == expected_size:
                # Try to read the file to ensure it's not corrupted
                with open(file_name, "rb") as f:
                    # Read first and last 1KB to check file integrity
                    f.read(1024)
                    if actual_size > 1024:
                        f.seek(-1024, 2)  # Seek to 1KB from end
                        f.read(1024)
                return True
            else:
                print(f"File {file_name} size verification failed")
                return False
        return False
    except Exception as e:
        print(f"File integrity check failed for {file_name}: {e}")
        return False

def validate_downloaded_files(processed_files, file_list):
    """Validate all processed files and remove invalid ones from the list."""
    print("Validating previously downloaded files...")
    invalid_files = set()
    
    for file_name in list(processed_files):
        if file_name in file_list:
            expected_size = file_list[file_name]
            
            # Check if file exists and has correct size
            if not is_file_complete(file_name, expected_size):
                print(f"File {file_name} is missing or incomplete, removing from processed list")
                invalid_files.add(file_name)
                continue
            
            # Verify file integrity
            if not verify_file_integrity(file_name, expected_size):
                print(f"File {file_name} failed integrity check, removing from processed list")
                invalid_files.add(file_name)
                # Remove corrupted file
                try:
                    os.remove(file_name)
                except:
                    pass
                continue
            
            print(f"✓ File {file_name} is valid")
        else:
            # File not in server list anymore, remove from processed
            print(f"File {file_name} no longer available on server, removing from processed list")
            invalid_files.add(file_name)
    
    # Remove invalid files from processed set and file
    for file_name in invalid_files:
        processed_files.discard(file_name)
        remove_processed_file(file_name)
    
    if invalid_files:
        print(f"Removed {len(invalid_files)} invalid files from processed list")
    else:
        print("All previously downloaded files are valid")
    
    return processed_files

def periodic_file_check(processed_files, file_list):
    """Perform periodic check of downloaded files during execution."""
    invalid_files = set()
    
    for file_name in list(processed_files):
        if file_name in file_list:
            expected_size = file_list[file_name]
            
            # Quick check - just verify file exists and has correct size
            if not is_file_complete(file_name, expected_size):
                print(f"\nWarning: File {file_name} became invalid, removing from processed list")
                invalid_files.add(file_name)
    
    # Remove invalid files
    for file_name in invalid_files:
        processed_files.discard(file_name)
        remove_processed_file(file_name)
    
    return len(invalid_files) > 0

def main():
    """Main client logic with select-based I/O multiplexing."""
    signal.signal(signal.SIGINT, signal_handler)  # Catch Ctrl+C
    
    # Load previously processed files
    global processed
    processed = load_processed_files()
    print(f"Loaded {len(processed)} previously downloaded files")
    
    # Get file list for validation
    print("Getting file list from server...")
    file_list = get_file_list()
    
    if not file_list:
        print("Failed to get file list from server, continuing with cached processed files")
    else:
        # Validate all previously downloaded files
        processed = validate_downloaded_files(processed, file_list)
        print(f"Valid downloaded files: {len(processed)}")
    
    execution_count = 0
    
    while True:
        execution_count += 1
        
        # Get file list (refresh every iteration)
        current_file_list = get_file_list()
        
        if current_file_list:
            file_list = current_file_list
            
            # Perform periodic file check every 3rd iteration
            if execution_count % 3 == 0:
                files_invalidated = periodic_file_check(processed, file_list)
                if files_invalidated:
                    print("Some files were invalidated and removed from processed list")
            
            print("\nAvailable files:")
            for name, size in file_list.items():
                status = "✓ Downloaded" if name in processed else "Available"
                print(f"{name} {size/(1024*1024):.1f}MB - {status}")
        else:
            print("Warning: Could not retrieve file list from server")
        
        # Read input.txt for files to download
        try:
            with open("input.txt", "r") as f:
                files_to_download = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print("input.txt not found")
            files_to_download = []
        
        # Filter out already processed files and files that exist locally
        new_files_to_download = []
        for file_name in files_to_download:
            if file_name in processed:
                # Double-check that the file still exists and is valid
                if file_name in file_list:
                    if is_file_complete(file_name, file_list[file_name]):
                        print(f"File {file_name} already downloaded and verified, skipping")
                        continue
                    else:
                        print(f"File {file_name} was marked as downloaded but is invalid, re-downloading")
                        processed.discard(file_name)
                        remove_processed_file(file_name)
                        new_files_to_download.append(file_name)
                else:
                    print(f"File {file_name} no longer available on server")
                    continue
            elif file_name in file_list and is_file_complete(file_name, file_list[file_name]):
                print(f"File {file_name} already exists locally and is complete, adding to processed list")
                processed.add(file_name)
                save_processed_file(file_name)
                continue
            else:
                new_files_to_download.append(file_name)
        
        # Download each new file
        for file_name in new_files_to_download:
            if file_name in file_list:
                size = file_list[file_name]
                
                success = download_file_with_select(file_name, size)
                
                if success:
                    # Final verification before marking as processed
                    if verify_file_integrity(file_name, size):
                        processed.add(file_name)
                        save_processed_file(file_name)
                        print(f"Successfully downloaded and verified {file_name}")
                    else:
                        print(f"Downloaded {file_name} failed final verification")
                else:
                    print(f"Failed to download {file_name}")
            else:
                print(f"File {file_name} not found on server")
        
        # Sleep before checking for new files
        if not new_files_to_download:
            print("No new files to download, waiting for new requests...")
        
        time.sleep(5)

if __name__ == "__main__":
    main()