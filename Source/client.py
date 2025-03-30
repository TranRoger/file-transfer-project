import socket
import json
import threading
import time
import os
import signal
import sys
import hashlib
import base64

CHUNK_SIZE = 1024*2
RECEIVE_SIZE = CHUNK_SIZE * 16
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

def get_file_list(sock):
    """Get the list of available files from the server."""
    # sock.sendto(b"LIST", server_address)
    # use JSON format for request
    request = json.dumps({'type': 'LIST'}).encode()
    sock.sendto(request, server_address)
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

def parse_response(data):
    """Parse the server response."""
    try:
        packet = json.loads(data.decode())
        return packet
    except json.JSONDecodeError:
        print("Failed to decode JSON response")
        return None

def download_part(sock, socket_id, file_name, offset, length, output_file, part_num, progress):
    """Download a specific part of the file using enhanced UDP protocol."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    port = 20000 + socket_id
    sock.bind(("0.0.0.0", port))

    # Send download request as JSON format
    request = {
        'type': "DOWNLOAD",
        'file_name': file_name,
        'offset': offset,
        'length': length,
        'socket_id': socket_id,
    }
    sock.sendto(json.dumps(request).encode("utf-8"), server_address)
    
    # Initialize storage for received data
    received_data = {}
    total_chunks = -1
    file_metadata = None

    while True:
        try:
            sock.settimeout(10)  # 10-second timeout
            # if receive nothing, receive again
            rawdata, _ = sock.recvfrom(RECEIVE_SIZE)
            while not rawdata:
                rawdata, _ = sock.recvfrom(RECEIVE_SIZE)
            # print(rawdata.decode())
            packet = json.loads(rawdata.decode())

            # Handle different packet types
            if packet['type'] == FileTransferProtocol.START_CHUNK:
                total_chunks = packet.get('total_chunks', -1)
                file_metadata = packet
                print(f"Received START packet for {file_name} part {part_num}")
                # Send ACK for START packet
                ack_packet = json.dumps({
                    'type': FileTransferProtocol.ACK,
                    'file_name': file_name,
                    'sequence': 0,
                    'offset': offset,
                }).encode()
                sock.sendto(ack_packet, server_address)

            elif packet['type'] == FileTransferProtocol.DATA_CHUNK:
                # Verify checksum
                # chunk_data = packet['data'].encode('latin-1')
                chunk_data = base64.b64decode(packet['data'])
                checksum = packet['checksum']
                
                if FileTransferProtocol.verify_checksum(chunk_data, checksum):
                    # ACK the chunk
                    ack_packet = json.dumps({
                        'type': FileTransferProtocol.ACK,
                        'sequence': packet['sequence'],
                        'file_name': file_name,
                        'offset': offset
                    }).encode()
                    sock.sendto(ack_packet, server_address)
                    
                    # Store the chunk
                    received_data[packet['sequence']] = chunk_data
                    
                    # Update progress
                    with progress_lock:
                        progress[part_num] = (len(received_data) * 1024, length)
                else:
                    # Send NACK if checksum fails
                    nack_packet = json.dumps({
                        'type': FileTransferProtocol.NACK,
                        'sequence': packet['sequence'],
                        'file_name': file_name,
                        'offset': offset
                    }).encode()
                    sock.sendto(nack_packet, server_address)

            elif packet['type'] == FileTransferProtocol.END_CHUNK:
                # Final chunk received, break the loop
                break

        except socket.timeout:
            print(f"Timeout while downloading {file_name} part {part_num}")

    # Reconstruct and write file
    if total_chunks > 0 and len(received_data) == total_chunks:
        # Reconstruct file data
        file_data = b''.join(received_data[i] for i in range(total_chunks))
        
        with open(output_file, "r+b") as f:
            f.seek(offset)
            f.write(file_data[:length])

server_address = ("10.250.91.1", 12345)
progress_lock = threading.Lock()
processed = set()  # Track processed files

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
    sock.close()

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
                    socket_id = i
                    t = threading.Thread(
                        target=download_part,
                        args=(sock, socket_id, file_name, offsets[i], lengths[i], file_name, i, progress)
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