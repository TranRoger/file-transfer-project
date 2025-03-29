import socket
import threading
import os
import time
import math

# Configuration
SERVER_HOST = '10.210.29.123'
SERVER_PORT = 5000
INPUT_FILE = 'input.txt'
DOWNLOAD_DIR = 'downloads'
CHUNK_SIZE = 1024
NUM_CONNECTIONS = 4
RECV_TIMEOUT = 2

# Store downloaded chunks
downloaded_chunks = {}
download_complete = {}

# Function to get file list from server
def get_file_list():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"Client: Sending GET_FILE_LIST to {SERVER_HOST}:{SERVER_PORT}")  # Add this line
    sock.settimeout(5)  # Add this line (timeout in seconds)
    sock.sendto(b"GET_FILE_LIST", (SERVER_HOST, SERVER_PORT))
    try:
        data, addr = sock.recvfrom(4096)
        print(f"Client: Received data: {data.decode('utf-8')}")  # Add this line
        file_list = data.decode('utf-8').split('\n')
        return [file.split() for file in file_list]
    except socket.timeout:
        print("Client: Timeout waiting for file list")  # Add this line
        return []

# Function to download a chunk
def download_chunk(file_name, offset, sock, chunk_id, total_chunks, event):
    sock.settimeout(RECV_TIMEOUT)
    
    try:
        sock.sendto(f"GET {file_name} {offset} {chunk_id}".encode('utf-8'), (SERVER_HOST, SERVER_PORT))
        print(f"Downloading chunk {chunk_id} of {file_name} from offset {offset}")
        
        while True:
            try:
                data, addr = sock.recvfrom(CHUNK_SIZE + 64)  # Increased size for metadata
                if data.startswith(b"CHUNK"):
                    parts = data.decode('utf-8').split(' ', 2)
                    recv_chunk_id = parts[1]
                    chunk_length = int(parts[2])
                    chunk_data = data[len(parts[0]) + len(parts[1]) + len(parts[2]) + 3:]
                    if recv_chunk_id == chunk_id and len(chunk_data) == chunk_length:
                        downloaded_chunks.setdefault(file_name, {}).setdefault(offset, b'').extend(chunk_data)
                        sock.sendto(f"ACK {chunk_id}".encode('utf-8'), (SERVER_HOST, SERVER_PORT))
                        event.set()
                        break
                    else:
                        print(f"  Mismatch: expected chunk {chunk_id}, got {recv_chunk_id}, resending request")
                        sock.sendto(f"GET {file_name} {offset} {chunk_id}".encode('utf-8'), (SERVER_HOST, SERVER_PORT))
                elif data == b"FILE_NOT_FOUND":
                    print(f"  File not found on server: {file_name}")
                    event.set()
                    break
                else:
                    print(f"  Unexpected data: {data.decode('utf-8')}")
            except socket.timeout:
                print(f"  Timeout waiting for chunk {chunk_id}, resending request")
                sock.sendto(f"GET {file_name} {offset} {chunk_id}".encode('utf-8'), (SERVER_HOST, SERVER_PORT))
    except Exception as e:
        print(f"  Error downloading chunk {chunk_id}: {e}")
    finally:
        sock.close()

# Function to handle file download
def download_file(file_name, file_size):
    print(f"Starting download of {file_name} ({file_size})")
    threads = []
    total_size_bytes = int(file_size[:-2]) * 1024 * 1024
    total_chunks = math.ceil(total_size_bytes / CHUNK_SIZE)
    chunks_per_connection = math.ceil(total_chunks / NUM_CONNECTIONS)

    for i in range(NUM_CONNECTIONS):
        start_chunk = i * chunks_per_connection
        end_chunk = min((i + 1) * chunks_per_connection, total_chunks)
        for j in range(start_chunk, end_chunk):
            offset = j * CHUNK_SIZE
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            event = threading.Event()
            thread = threading.Thread(target=download_chunk, args=(file_name, offset, sock, str(j), total_chunks, event))
            threads.append(thread)
            thread.start()

    for thread in threads:
        thread.join()  # Wait for all threads to complete

    print(f"Finished downloading {file_name}")
    reassemble_file(file_name)

# Function to reassemble the downloaded chunks
def reassemble_file(file_name):
    print(f"Reassembling {file_name}")
    try:
        with open(os.path.join(DOWNLOAD_DIR, file_name), 'wb') as f:
            for offset in sorted(downloaded_chunks.get(file_name, {}).keys()):
                f.write(downloaded_chunks[file_name][offset])
        print(f"File {file_name} reassembled successfully.")
        download_complete[file_name] = True
    except Exception as e:
        print(f"Error reassembling file: {e}")

# Function to read input.txt and manage downloads
def manage_downloads():
    file_list = get_file_list()
    print("Available files:")
    for name, size in file_list:
        print(f"- {name} ({size})")

    try:
        with open(INPUT_FILE, 'r') as f:
            files_to_download = [line.strip() for line in f]
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found. Creating an empty one.")
        open(INPUT_FILE, 'w').close()  # Create an empty file
        files_to_download = []

    files_downloaded = set()
    while True:
        try:
            with open(INPUT_FILE, 'r') as f:
                new_files_to_download = [line.strip() for line in f if line.strip() not in files_downloaded]
        except FileNotFoundError:
            new_files_to_download = []
            
        for file_name in new_files_to_download:
            file_info = next((f for f in file_list if f[0] == file_name), None)
            if file_info:
                download_file(file_info[0], file_info[1])
                files_downloaded.add(file_name)
            else:
                print(f"File {file_name} not found on the server.")
        
        # Display download progress
        for file_name in downloaded_chunks:
            if file_name not in download_complete:
                total_size_bytes = int(next((f[1] for f in file_list if f[0] == file_name), '0MB')[:-2]) * 1024 * 1024
                total_chunks = math.ceil(total_size_bytes / CHUNK_SIZE)
                chunks_downloaded = len(downloaded_chunks[file_name])
                progress = (chunks_downloaded / total_chunks) * 100
                print(f"Downloading {file_name}: {progress:.2f}% complete")
        
        time.sleep(5)  # Check for new files every 5 seconds
        
        if all(download_complete.values()):
            print("All downloads complete.")
            break

if __name__ == "__main__":
    # Create a dummy input.txt and downloads directory
    with open(INPUT_FILE, 'w') as f:
        f.write("100MB.zip\n")  # Add some files to download
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
    download_complete = {}

    manage_downloads()