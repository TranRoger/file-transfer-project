import socket
import json
import threading
import time
import os
import signal
import sys

server_address = ("10.250.91.1", 12345)
progress_lock = threading.Lock()
processed = set()  # Track processed files

def get_file_list(sock):
    """Get the list of available files from the server."""
    sock.sendto(b"LIST", server_address)
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

def download_part(sock, file_name, offset, length, output_file, part_num, progress):
    """Download a specific part of the file using UDP."""
    # Send download request
    request = f"DOWNLOAD {file_name} {offset} {length}".encode()
    sock.sendto(request, server_address)

    # Initialize storage for received data
    received_data = {}
    metadata = None
    total_chunks = -1

    while len(received_data) < total_chunks or total_chunks == -1:
        try:
            sock.settimeout(10)  # 10-second timeout
            data, _ = sock.recvfrom(2048)
            packet = json.loads(data.decode())

            # Check if this is metadata packet
            if "file_name" in packet:
                metadata = packet
                total_chunks = (metadata['total_length'] + 1023) // 1024
                continue

            # Chunk data packet
            if metadata and 'sequence' in packet:
                received_data[packet['sequence']] = packet['data']

                # Update progress
                with progress_lock:
                    progress[part_num] = (len(received_data) * 1024, length)

        except socket.timeout:
            print(f"Timeout while downloading {file_name} part {part_num}")
            break

    # Reconstruct and write file
    if metadata and len(received_data) == total_chunks:
        # Reconstruct file data
        file_data = b''.join(received_data[i].encode('latin-1') if isinstance(received_data[i], str) else received_data[i] 
                              for i in range(total_chunks))
        
        with open(output_file, "r+b") as f:
            f.seek(offset)
            f.write(file_data[:length])

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    print("\nExiting due to Ctrl+C...")
    sys.exit(0)

def main():
    """Main client logic for UDP file transfer."""
    signal.signal(signal.SIGINT, signal_handler)  # Catch Ctrl+C
    
    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    file_list = get_file_list(sock)
    print("Available files:")
    for name, size in file_list.items():
        print(f"{name} {size/(1024*1024)}MB")
    
    while True:
        with open("input.txt", "r") as f:
            files_to_download = [line.strip() for line in f if line.strip() and line.strip() not in processed]
        
        for file_name in files_to_download:
            if file_name in file_list:
                size = file_list[file_name]
                part_size = size // 4
                offsets = [0, part_size, 2 * part_size, 3 * part_size]
                lengths = [part_size, part_size, part_size, size - 3 * part_size]
                
                # Pre-allocate the output file
                with open(file_name, "wb") as f:
                    f.truncate(size)
                
                progress = {i: (0, lengths[i]) for i in range(4)}
                threads = []
                for i in range(4):
                    t = threading.Thread(
                        target=download_part,
                        args=(sock, file_name, offsets[i], lengths[i], file_name, i, progress)
                    )
                    t.start()
                    threads.append(t)
                
                # Display progress
                while any(t.is_alive() for t in threads):
                    time.sleep(1)
                    with progress_lock:
                        for i in range(4):
                            received, total = progress[i]
                            percent = (received / total * 100) if total > 0 else 0
                            print(f"Downloading {file_name} part {i+1} .... {percent:.2f}%")
                
                # Wait for all threads to finish
                for t in threads:
                    t.join()
                processed.add(file_name)
                print(f"Completed downloading {file_name}")
            else:
                print(f"File {file_name} not found on server")
        time.sleep(5)  # Check for new files every 5 seconds

if __name__ == "__main__":
    main()