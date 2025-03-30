import socket
import os
import json
import threading
import time
from collections import defaultdict

class UDPServer:
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        self.file_list = "file_list.txt"
        self.file_data = defaultdict(dict)  # Store file chunks being sent
        self.load_file_list()
        
    def load_file_list(self):
        """Load the list of available files and their sizes"""
        if not os.path.exists(self.file_list):
            # Create sample files if not exists
            sample_files = {
                "100MB.zip": "100MB",
                "50MB.zip": "50MB",
                "20MB.zip": "20MB",
                "10MB.zip": "10MB",
                "5MB.zip": "5MB",
                "1GB.zip": "1GB"
            }
            with open(self.file_list, 'w') as f:
                for name, size in sample_files.items():
                    f.write(f"{name} {size}\n")
        
        self.available_files = {}
        with open(self.file_list, 'r') as f:
            for line in f:
                if line.strip():
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        filename = parts[0]
                        size_str = parts[1].upper()
                        
                        # Convert size string to bytes
                        if size_str.endswith("GB"):
                            size = int(size_str[:-2]) * 1024 * 1024 * 1024
                        elif size_str.endswith("MB"):
                            size = int(size_str[:-2]) * 1024 * 1024
                        elif size_str.endswith("KB"):
                            size = int(size_str[:-2]) * 1024
                        else:
                            size = int(size_str)  # Assume bytes if no unit
                        
                        self.available_files[filename] = size

    def handle_request(self, data, client_addr):
        """Handle incoming requests from clients"""
        try:
            request = json.loads(data.decode())
            req_type = request.get("type")
            
            if req_type == "list":
                self.send_file_list(client_addr)
            elif req_type == "download":
                filename = request.get("filename")
                chunk_id = request.get("chunk_id", 0)
                chunk_size = request.get("chunk_size", 0)
                offset = request.get("offset", 0)
                
                if filename in self.available_files:
                    self.send_file_chunk(filename, chunk_id, chunk_size, offset, client_addr)
                else:
                    self.send_error("File not found", client_addr)
            elif req_type == "ack":
                # Handle ACK for reliable transfer
                seq_num = request.get("seq_num")
                chunk_id = request.get("chunk_id")
                filename = request.get("filename")
                
                if filename in self.file_data and chunk_id in self.file_data[filename]:
                    if seq_num in self.file_data[filename][chunk_id]["sent_packets"]:
                        self.file_data[filename][chunk_id]["acked_packets"].add(seq_num)
        except json.JSONDecodeError:
            self.send_error("Invalid request format", client_addr)
        except Exception as e:
            self.send_error(str(e), client_addr)

    def send_file_list(self, client_addr):
        """Send the list of available files to client"""
        response = {
            "type": "list",
            "files": self.available_files
        }
        self.sock.sendto(json.dumps(response).encode(), client_addr)

    def send_file_chunk(self, filename, chunk_id, chunk_size, offset, client_addr):
        """Send a chunk of the requested file with progress tracking"""
        try:
            file_size = os.path.getsize(filename)
            chunk_end = min(offset + chunk_size, file_size)
            bytes_to_send = chunk_end - offset
            bytes_sent = 0
            
            with open(filename, 'rb') as f:
                f.seek(offset)
                
                while bytes_sent < bytes_to_send:
                    packet_size = min(1024, bytes_to_send - bytes_sent)
                    data = f.read(packet_size)
                    
                    packet = {
                        "type": "data",
                        "filename": filename,
                        "chunk_id": chunk_id,
                        "seq_num": bytes_sent // 1024,
                        "data": data.hex(),
                        "bytes_sent": bytes_sent,
                        "total_bytes": bytes_to_send
                    }
                    
                    self.sock.sendto(json.dumps(packet).encode(), client_addr)
                    bytes_sent += packet_size
                    
                    # Wait for ACK
                    self.sock.settimeout(2.0)
                    try:
                        ack_data, _ = self.sock.recvfrom(1024)
                        ack = json.loads(ack_data.decode())
                        if not (ack.get("type") == "ack" and ack.get("filename") == filename and ack.get("chunk_id") == chunk_id):
                            raise socket.timeout("Invalid ACK received")
                    except socket.timeout:
                        # Resend packet if no ACK received
                        f.seek(offset + bytes_sent - packet_size)
                        bytes_sent -= packet_size
                        continue
                
                # Send end marker
                end_packet = {
                    "type": "end",
                    "filename": filename,
                    "chunk_id": chunk_id,
                    "total_bytes": bytes_to_send
                }
                self.sock.sendto(json.dumps(end_packet).encode(), client_addr)
        
        except Exception as e:
            print(f"Error sending {filename} chunk {chunk_id}: {e}")
            error_packet = {
                "type": "error",
                "filename": filename,
                "chunk_id": chunk_id,
                "message": str(e)
            }
            self.sock.sendto(json.dumps(error_packet).encode(), client_addr)

    def send_error(self, message, client_addr):
        """Send an error message to the client"""
        error = {
            "type": "error",
            "message": message
        }
        self.sock.sendto(json.dumps(error).encode(), client_addr)

    def run(self):
        print(f"Server listening on {self.host}:{self.port}")
        while True:
            try:
                data, addr = self.sock.recvfrom(1024)
                threading.Thread(target=self.handle_request, args=(data, addr)).start()
            except KeyboardInterrupt:
                print("\nServer shutting down...")
                break
            except Exception as e:
                print(f"Error: {e}")

if __name__ == "__main__":
    server = UDPServer()
    server.run()