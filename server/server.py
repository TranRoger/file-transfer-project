import socket
import threading
import os
import time

# Configuration
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 5000
FILE_LIST = 'file_list.txt'
CHUNK_SIZE = 1024
ACK_TIMEOUT = 2  # seconds
RETRANSMIT_LIMIT = 5

# Store client addresses and their requests
client_requests = {}

# Function to read the file list
def read_file_list():
    try:
        with open(FILE_LIST, 'r') as f:
            file_data = f.readlines()
        file_list = []
        for line in file_data:
            filename, filesize = line.strip().split()
            file_list.append((filename, filesize))
        return file_list
    except FileNotFoundError:
        print(f"Error: {FILE_LIST} not found.")
        return []

# Function to send a chunk of the file
def send_chunk(sock, addr, filename, offset, chunk_id):
    try:
        with open(filename, 'rb') as f:
            f.seek(offset)
            chunk = f.read(CHUNK_SIZE)
            sock.sendto(f"CHUNK {chunk_id} {len(chunk)}".encode('utf-8') + chunk, addr)
            return chunk
    except FileNotFoundError:
        print(f"Error: {filename} not found on server.")
        sock.sendto(b"FILE_NOT_FOUND", addr)
    except Exception as e:
        print(f"Error sending chunk: {e}")
    return None

# Function to handle client requests
def handle_client(addr, sock):
    print(f"Server: Handling client: {addr}")
    file_list = read_file_list()
    # Send the file list to the client
    send_data = "\n".join([f"{name} {size}" for name, size in file_list]).encode('utf-8')
    print(f"Server: Sending file list: {send_data.decode('utf-8')}")  # Add this line
    sock.sendto(send_data, addr)

    client_requests[addr] = {}
    
    while True:
        data, addr = sock.recvfrom(4096)
        request = data.decode('utf-8').split(' ')
        if request[0] == "GET":
            filename = request[1]
            offset = int(request[2])
            chunk_id = request[3]
            
            chunk_data = send_chunk(sock, addr, filename, offset, chunk_id)
            if chunk_data:
                client_requests[addr][chunk_id] = {
                    'data': chunk_data,
                    'retries': 0,
                    'acked': False,
                    'filename': filename,
                    'offset': offset
                }
        elif request[0] == "ACK":
            chunk_id = request[1]
            if addr in client_requests and chunk_id in client_requests[addr]:
                client_requests[addr][chunk_id]['acked'] = True
        elif not data:
            break #client disconnect

    print(f"Client {addr} disconnected")
    if addr in client_requests:
        del client_requests[addr]

def resend_chunks(sock):
    while True:
        time.sleep(1)  # Check every 1 second
        for addr, requests in client_requests.items():
            for chunk_id, chunk_info in requests.items():
                if not chunk_info['acked']:
                    if chunk_info['retries'] < RETRANSMIT_LIMIT:
                        print(f"Resending chunk {chunk_id} to {addr}")
                        send_chunk(sock, addr, chunk_info['filename'], chunk_info['offset'], chunk_id)
                        chunk_info['retries'] += 1
                    else:
                        print(f"Chunk {chunk_id} to {addr} failed after {RETRANSMIT_LIMIT} retries")
                        chunk_info['acked'] = True  # Stop retrying

# Main server function
def server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SERVER_HOST, SERVER_PORT))

    print(f"Server listening on {SERVER_HOST}:{SERVER_PORT}")

    threading.Thread(target=resend_chunks, args=(sock,), daemon=True).start()

    while True:
        data, addr = sock.recvfrom(4096)
        threading.Thread(target=handle_client, args=(addr, sock)).start()

if __name__ == "__main__":
    # Create a dummy file_list.txt for testing
    with open(FILE_LIST, 'w') as f:
        f.write("File1.zip 5MB\nFile2.zip 10MB\nFile3.zip 20MB\n100MB.zip 100MB")
    # Create dummy files for testing
    with open("File1.zip", 'wb') as f:
        f.write(b"This is File1 content " * 5000000)  # 5MB
    with open("File2.zip", 'wb') as f:
        f.write(b"This is File2 content " * 10000000) # 10MB

    server()