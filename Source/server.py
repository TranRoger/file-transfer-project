import os
import socket
import threading
import json
import hashlib

SERVER_IP = "10.250.91.1"
SERVER_PORT = 12345
class FileServer:
    def __init__(self, host=SERVER_IP, port=SERVER_PORT):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.files_info = self.load_files_info('files_list.txt')
        self.lock = threading.Lock()
        
    def load_files_info(self, filename):
        files_info = {}
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                for line in f:
                    if line.strip():
                        parts = line.strip().split()
                        if len(parts) >= 2:
                            file_name = ' '.join(parts[:-1])
                            size = parts[-1]
                            files_info[file_name] = {
                                'size': size,
                                'path': os.path.join('server_files', file_name)
                            }
        return files_info
    
    def calculate_checksum(self, data):
        return hashlib.md5(data).hexdigest()
    
    def handle_client(self, client_socket, address):
        print(f"Connection from {address} established.")
        
        try:
            # Send list of available files
            files_list = json.dumps(self.files_info).encode()
            client_socket.sendall(files_list)
            
            while True:
                # Receive request from client
                request = client_socket.recv(1024).decode()
                if not request:
                    break
                    
                try:
                    request_data = json.loads(request)
                    action = request_data.get('action')
                    
                    if action == 'download':
                        file_name = request_data['file_name']
                        chunk_index = request_data['chunk_index']
                        total_chunks = request_data['total_chunks']
                        offset = request_data['offset']
                        chunk_size = request_data['chunk_size']
                        
                        if file_name not in self.files_info:
                            response = {'status': 'error', 'message': 'File not found'}
                            client_socket.sendall(json.dumps(response).encode())
                            continue
                            
                        file_path = self.files_info[file_name]['path']
                        file_size = os.path.getsize(file_path)
                        
                        # Validate chunk request
                        if offset >= file_size:
                            response = {'status': 'error', 'message': 'Invalid offset'}
                            client_socket.sendall(json.dumps(response).encode())
                            continue
                            
                        # Read the chunk
                        with open(file_path, 'rb') as f:
                            f.seek(offset)
                            data = f.read(chunk_size)
                            checksum = self.calculate_checksum(data)
                            
                            # Prepare response
                            response = {
                                'status': 'success',
                                'file_name': file_name,
                                'chunk_index': chunk_index,
                                'total_chunks': total_chunks,
                                'offset': offset,
                                'chunk_size': len(data),
                                'data': data.hex(),
                                'checksum': checksum
                            }
                            
                            client_socket.sendall(json.dumps(response).encode())
                            
                            # Wait for ACK
                            ack = client_socket.recv(1024).decode()
                            if ack != 'ACK':
                                print(f"Did not receive ACK for chunk {chunk_index} of {file_name}")
                    
                    elif action == 'get_file_size':
                        file_name = request_data['file_name']
                        if file_name in self.files_info:
                            file_path = self.files_info[file_name]['path']
                            file_size = os.path.getsize(file_path)
                            response = {'status': 'success', 'file_size': file_size}
                            # log
                            print(f"File size for {file_name}: {file_size}")
                        else:
                            response = {'status': 'error', 'message': 'File not found'}
                        # log
                        print(f"File size request for {file_name}: {response}")
                        client_socket.sendall(json.dumps(response).encode())
                        
                except Exception as e:
                    print(f"Error processing request: {e}")
                    response = {'status': 'error', 'message': str(e)}
                    client_socket.sendall(json.dumps(response).encode())
                    
        except Exception as e:
            print(f"Error with client {address}: {e}")
        finally:
            client_socket.close()
            print(f"Connection from {address} closed.")
    
    def start(self):
        print(f"Server started on {self.host}:{self.port}")
        while True:
            client_socket, address = self.server_socket.accept()
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket, address))
            client_thread.daemon = True
            client_thread.start()

if __name__ == '__main__':
    if not os.path.exists('server_files'):
        os.makedirs('server_files')
    
    server = FileServer()
    server.start()