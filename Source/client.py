import os
import socket
import threading
import json
import time
import hashlib
import signal
import sys

SERVER_IP = "10.250.91.1"
SERVER_PORT = 12345
class FileDownloader:
    def __init__(self, server_host=SERVER_IP, server_port=SERVER_PORT):
        self.server_host = server_host
        self.server_port = server_port
        self.downloaded_files = set()
        self.active_downloads = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        
        # Register Ctrl+C handler
        signal.signal(signal.SIGINT, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        print("\nReceived Ctrl+C. Finishing current downloads...")
        self.stop_event.set()
        sys.exit(0)
    
    def calculate_checksum(self, data):
        return hashlib.md5(data).hexdigest()
    
    def connect_to_server(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.server_host, self.server_port))
            return sock
        except Exception as e:
            print(f"Error connecting to server: {e}")
            return None
    
    def get_files_list(self):
        sock = self.connect_to_server()
        if not sock:
            return None
            
        try:
            data = sock.recv(1024)
            files_info = json.loads(data.decode())
            return files_info
        except Exception as e:
            print(f"Error getting files list: {e}")
            return None
        finally:
            sock.close()
    
    def get_file_size(self, file_name):
        sock = self.connect_to_server()
        if not sock:
            return None
            
        try:
            request = {'action': 'get_file_size', 'file_name': file_name}
            sock.sendall(json.dumps(request).encode())
            
            response = sock.recv(1024).decode()
            response_data = json.loads(response)
            # log
            print(f"Received file size response: {response_data}")
            if response_data['status'] == 'success':
                return response_data['file_size']
            else:
                print(f"Error getting file size: {response_data.get('message', 'Unknown error')}")
                return None
        except Exception as e:
            print(f"Error getting file size: {e}")
            return None
        finally:
            sock.close()
    
    def download_chunk(self, file_name, chunk_index, total_chunks, offset, chunk_size, progress_dict):
        sock = self.connect_to_server()
        if not sock:
            return
            
        try:
            request = {
                'action': 'download',
                'file_name': file_name,
                'chunk_index': chunk_index,
                'total_chunks': total_chunks,
                'offset': offset,
                'chunk_size': chunk_size
            }
            
            sock.sendall(json.dumps(request).encode())
            
            # Receive response
            response = b''
            while True:
                data = sock.recv(4096)
                if not data:
                    break
                response += data
                if data.endswith(b'}'):  # Simple way to detect end of JSON
                    break
            
            response_data = json.loads(response.decode())
            
            if response_data['status'] == 'success':
                # Verify checksum
                received_data = bytes.fromhex(response_data['data'])
                received_checksum = response_data['checksum']
                calculated_checksum = self.calculate_checksum(received_data)
                
                if received_checksum == calculated_checksum:
                    # Send ACK
                    sock.sendall(b'ACK')
                    
                    # Save the chunk
                    chunk_file = f"{file_name}.part{chunk_index}"
                    with open(chunk_file, 'wb') as f:
                        f.write(received_data)
                    
                    # Update progress
                    with self.lock:
                        progress_dict['downloaded'] += len(received_data)
                        progress = (progress_dict['downloaded'] / progress_dict['total_size']) * 100
                        progress_dict['parts'][chunk_index] = progress
                else:
                    print(f"Checksum mismatch for chunk {chunk_index} of {file_name}")
                    sock.sendall(b'NACK')
            else:
                print(f"Error downloading chunk {chunk_index}: {response_data.get('message', 'Unknown error')}")
                
        except Exception as e:
            print(f"Error downloading chunk {chunk_index}: {e}")
        finally:
            sock.close()
    
    def merge_chunks(self, file_name, total_chunks):
        output_file = os.path.join('downloads', file_name)
        
        # Create downloads directory if it doesn't exist
        if not os.path.exists('downloads'):
            os.makedirs('downloads')
        
        with open(output_file, 'wb') as outfile:
            for i in range(total_chunks):
                chunk_file = f"{file_name}.part{i}"
                try:
                    with open(chunk_file, 'rb') as infile:
                        outfile.write(infile.read())
                    os.remove(chunk_file)
                except FileNotFoundError:
                    print(f"Chunk {i} not found. File may be incomplete.")
                    return False
        
        # Verify file size
        expected_size = self.get_file_size(file_name)
        actual_size = os.path.getsize(output_file)
        
        if expected_size is not None and actual_size == expected_size:
            print(f"Successfully downloaded and verified {file_name}")
            return True
        else:
            print(f"File verification failed for {file_name}")
            return False
    
    def display_progress(self, file_name, progress_dict):
        while not progress_dict.get('complete', False) and not self.stop_event.is_set():
            with self.lock:
                parts = progress_dict['parts']
                total_progress = (progress_dict['downloaded'] / progress_dict['total_size']) * 100
                
                # Clear previous output
                print('\033[F' * (len(parts) + 1), end='')
                
                print(f"Downloading {file_name} - Overall: {total_progress:.2f}%")
                for i, progress in enumerate(parts):
                    print(f"  Part {i+1}: {progress:.2f}%")
            
            time.sleep(0.5)
    
    def download_file(self, file_name):
        if file_name in self.downloaded_files:
            return
            
        print(f"Starting download for {file_name}")
        
        # Get file size
        file_size = self.get_file_size(file_name)
        if file_size is None:
            print(f"Failed to get size for {file_name}")
            return
            
        # Calculate chunk sizes (4 parts)
        num_chunks = 4
        chunk_size = file_size // num_chunks
        chunks = []
        
        for i in range(num_chunks):
            offset = i * chunk_size
            size = chunk_size if i < num_chunks - 1 else file_size - offset
            chunks.append((i, offset, size))
        
        # Initialize progress tracking
        progress_dict = {
            'total_size': file_size,
            'downloaded': 0,
            'parts': [0.0] * num_chunks,
            'complete': False
        }
        
        # Start progress display thread
        progress_thread = threading.Thread(
            target=self.display_progress,
            args=(file_name, progress_dict)
        )
        progress_thread.daemon = True
        progress_thread.start()
        
        # Start download threads
        threads = []
        for chunk_index, offset, size in chunks:
            thread = threading.Thread(
                target=self.download_chunk,
                args=(file_name, chunk_index, num_chunks, offset, size, progress_dict)
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Mark as complete
        with self.lock:
            progress_dict['complete'] = True
        
        # Wait for progress thread to finish
        progress_thread.join()
        
        # Merge chunks
        if self.merge_chunks(file_name, num_chunks):
            self.downloaded_files.add(file_name)
    
    def monitor_input_file(self, input_file='input.txt'):
        last_modified = 0
        processed_files = set()
        
        while not self.stop_event.is_set():
            try:
                current_modified = os.path.getmtime(input_file)
                
                if current_modified > last_modified:
                    # File has been modified
                    last_modified = current_modified
                    
                    with open(input_file, 'r') as f:
                        files_to_download = [line.strip() for line in f if line.strip()]
                    
                    # Process new files
                    for file_name in files_to_download:
                        if file_name not in processed_files:
                            processed_files.add(file_name)
                            self.download_file(file_name)
                
                time.sleep(1)
            except FileNotFoundError:
                print(f"Input file {input_file} not found. Waiting...")
                time.sleep(5)
            except Exception as e:
                print(f"Error monitoring input file: {e}")
                time.sleep(5)
    
    def start(self):
        # First, get list of available files from server
        files_info = self.get_files_list()
        if files_info:
            print("Available files on server:")
            for file_name, info in files_info.items():
                print(f"- {file_name} ({info['size']})")
        
        # Start monitoring input file
        self.monitor_input_file()

if __name__ == '__main__':
    downloader = FileDownloader()
    downloader.start()