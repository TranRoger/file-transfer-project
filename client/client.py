import socket
import os
import json
import threading
import time
from collections import defaultdict

class UDPClient:
    def __init__(self, server_ip='10.210.29.1', server_port=5000):
        self.server_ip = server_ip
        self.server_port = server_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(5.0)
        self.input_file = "input.txt"
        self.downloaded_files = set()
        self.chunk_size = 1024 * 1024  # 1MB chunk size
        self.max_connections = 4
        self.active_downloads = defaultdict(dict)
        self.lock = threading.Lock()
        
        # Create input file if not exists
        if not os.path.exists(self.input_file):
            with open(self.input_file, 'w') as f:
                f.write("# Add files to download, one per line\n")

    def get_file_list(self):
        """Request list of available files from server"""
        request = {"type": "list"}
        self.sock.sendto(json.dumps(request).encode(), (self.server_ip, self.server_port))
        
        try:
            data, _ = self.sock.recvfrom(65536)  # Large buffer for file list
            response = json.loads(data.decode())
            if response.get("type") == "list":
                return response.get("files", {})
        except socket.timeout:
            print("Timeout while waiting for file list")
        except json.JSONDecodeError:
            print("Invalid response from server")
        
        return {}

    def send_ack(self, filename, chunk_id, seq_num, server_addr):
        """Send ACK for received packet"""
        ack = {
            "type": "ack",
            "filename": filename,
            "chunk_id": chunk_id,
            "seq_num": seq_num
        }
        self.sock.sendto(json.dumps(ack).encode(), server_addr)

    def download_chunk(self, filename, chunk_id, offset, chunk_size):
        """Download a specific chunk of a file"""
        temp_filename = f"{filename}.part{chunk_id}"
        received_packets = set()
        
        request = {
            "type": "download",
            "filename": filename,
            "chunk_id": chunk_id,
            "offset": offset,
            "chunk_size": chunk_size
        }
        
        self.sock.sendto(json.dumps(request).encode(), (self.server_ip, self.server_port))
        
        with open(temp_filename, 'wb') as f:
            while True:
                try:
                    data, server_addr = self.sock.recvfrom(65536)  # Large buffer for file data
                    packet = json.loads(data.decode())
                    
                    if packet.get("type") == "data":
                        seq_num = packet.get("seq_num")
                        if seq_num not in received_packets:
                            # Write the data to file
                            f.write(bytes.fromhex(packet.get("data")))
                            received_packets.add(seq_num)
                            
                            # Update progress
                            with self.lock:
                                if filename in self.active_downloads and chunk_id in self.active_downloads[filename]:
                                    self.active_downloads[filename][chunk_id]["received"] = len(received_packets)
                                    self.active_downloads[filename][chunk_id]["total"] = packet.get("total_packets", 1)
                            
                            # Send ACK
                            self.send_ack(filename, chunk_id, seq_num, server_addr)
                    
                    elif packet.get("type") == "end":
                        # End of chunk received
                        with self.lock:
                            if filename in self.active_downloads and chunk_id in self.active_downloads[filename]:
                                self.active_downloads[filename][chunk_id]["complete"] = True
                        break
                    
                    elif packet.get("type") == "error":
                        print(f"Error downloading {filename} chunk {chunk_id}: {packet.get('message')}")
                        break
                
                except socket.timeout:
                    print(f"Timeout while downloading {filename} chunk {chunk_id}")
                    break
                except json.JSONDecodeError:
                    print("Invalid packet received")
                    continue
                except Exception as e:
                    print(f"Error processing packet: {e}")
                    break

    def download_chunk(self, filename, chunk_id, offset, chunk_size):
        """Download a specific chunk of a file with progress tracking"""
        temp_filename = f"{filename}.part{chunk_id}"
        received_bytes = 0
        total_bytes = chunk_size
        
        try:
            # Initialize progress tracking
            with self.lock:
                if filename not in self.active_downloads:
                    self.active_downloads[filename] = {}
                self.active_downloads[filename][chunk_id] = {
                    "received": 0,
                    "total": total_bytes,
                    "complete": False
                }
            
            request = {
                "type": "download",
                "filename": filename,
                "chunk_id": chunk_id,
                "offset": offset,
                "chunk_size": chunk_size
            }
            
            self.sock.sendto(json.dumps(request).encode(), (self.server_ip, self.server_port))
            
            with open(temp_filename, 'wb') as f:
                while True:
                    data, server_addr = self.sock.recvfrom(65536)
                    packet = json.loads(data.decode())
                    
                    if packet.get("type") == "data":
                        packet_data = bytes.fromhex(packet.get("data", ""))
                        f.write(packet_data)
                        received_bytes += len(packet_data)
                        
                        # Update progress
                        with self.lock:
                            self.active_downloads[filename][chunk_id]["received"] = received_bytes
                        
                        # Send ACK
                        self.send_ack(filename, chunk_id, packet.get("seq_num"), server_addr)
                    
                    elif packet.get("type") == "end":
                        with self.lock:
                            self.active_downloads[filename][chunk_id]["complete"] = True
                        break
                    
                    elif packet.get("type") == "error":
                        print(f"Error downloading {filename} chunk {chunk_id}: {packet.get('message')}")
                        break
        
        except Exception as e:
            print(f"Error in chunk {chunk_id} of {filename}: {e}")
            with self.lock:
                if filename in self.active_downloads and chunk_id in self.active_downloads[filename]:
                    self.active_downloads[filename][chunk_id]["failed"] = True

    def download_file(self, filename, file_size):
        """Download a file using multiple chunks"""
        if filename in self.active_downloads:
            print(f"{filename} is already being downloaded")
            return
        
        # Calculate chunk sizes
        chunk_size = max(file_size // self.max_connections, self.chunk_size)
        total_chunks = (file_size + chunk_size - 1) // chunk_size
        
        # Initialize download tracking
        with self.lock:
            self.active_downloads[filename] = {
                "size": file_size,
                "total_chunks": total_chunks,
                "completed_chunks": 0
            }
            
            for i in range(total_chunks):
                offset = i * chunk_size
                current_chunk_size = min(chunk_size, file_size - offset)
                self.active_downloads[filename][i] = {
                    "offset": offset,
                    "size": current_chunk_size,
                    "received": 0,
                    "total": 0,
                    "complete": False
                }
        
        # Start download threads
        threads = []
        for i in range(total_chunks):
            t = threading.Thread(
                target=self.download_chunk,
                args=(filename, i, i * chunk_size, chunk_size)
            )
            t.start()
            threads.append(t)
        
        # Display progress
        self.display_progress(filename, total_chunks)
        
        # Wait for all threads to complete
        for t in threads:
            t.join()
        
        # Check if all chunks completed
        all_complete = True
        with self.lock:
            for i in range(total_chunks):
                if not self.active_downloads[filename][i].get("complete", False):
                    all_complete = False
                    break
        
        if all_complete:
            if self.combine_chunks(filename, total_chunks):
                self.downloaded_files.add(filename)
        
        # Clean up
        with self.lock:
            del self.active_downloads[filename]

    def display_progress(self, filename, total_chunks):
        """Display download progress for all chunks of a file"""
        last_update = time.time()
        
        while True:
            with self.lock:
                # Check if download is complete
                all_complete = all(
                    self.active_downloads[filename].get(i, {}).get("complete", False)
                    for i in range(total_chunks)
                )
                if all_complete:
                    break
                
                # Calculate overall progress
                total_received = sum(
                    self.active_downloads[filename].get(i, {}).get("received", 0)
                    for i in range(total_chunks)
                )
                total_size = sum(
                    self.active_downloads[filename].get(i, {}).get("total", 0)
                    for i in range(total_chunks)
                )
                
                if total_size > 0:
                    overall_percent = (total_received / total_size) * 100
                else:
                    overall_percent = 0
            
            # Only update display every 0.2 seconds to prevent flickering
            if time.time() - last_update >= 0.2:
                os.system('cls' if os.name == 'nt' else 'clear')
                print(f"Downloading {filename} - Overall: {overall_percent:.1f}%")
                
                for i in range(total_chunks):
                    chunk_data = self.active_downloads.get(filename, {}).get(i, {})
                    chunk_received = chunk_data.get("received", 0)
                    chunk_total = chunk_data.get("total", 1)
                    chunk_percent = (chunk_received / chunk_total) * 100 if chunk_total > 0 else 0
                    status = "✓" if chunk_data.get("complete", False) else "✗" if chunk_data.get("failed", False) else " "
                    
                    print(f"  Chunk {i+1}: [{status}] {chunk_percent:.1f}% ({chunk_received/1024:.1f}KB/{chunk_total/1024:.1f}KB)")
                
                last_update = time.time()
            
            time.sleep(0.1)

    def check_new_files(self):
        """Check input.txt for new files to download"""
        current_files = set()
        new_files = set()
        
        # Read currently listed files
        if os.path.exists(self.input_file):
            with open(self.input_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        current_files.add(line)
        
        # Find new files not yet downloaded
        for filename in current_files:
            if filename not in self.downloaded_files:
                new_files.add(filename)
        
        return new_files

    def human_readable_size(self, size_bytes):
        """Convert size in bytes to human-readable format (MB, GB)"""
        if size_bytes >= 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
        elif size_bytes >= 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        elif size_bytes >= 1024:
            return f"{size_bytes / 1024:.1f} KB"
        else:
            return f"{size_bytes} bytes"

    def run(self):
        """Main client loop"""
        try:
            # Get list of available files
            available_files = self.get_file_list()
            if not available_files:
                print("No files available on server")
                return
            
            print("Available files on server:")
            for filename, size in available_files.items():
                print(f"  {filename} - {self.human_readable_size(size)}")
            
            while True:
                # Check for new files to download
                new_files = self.check_new_files()
                
                for filename in new_files:
                    if filename in available_files:
                        print(f"\nStarting download of {filename}")
                        self.download_file(filename, available_files[filename])
                    else:
                        print(f"\nFile not available on server: {filename}")
                
                time.sleep(5)  # Check for new files every 5 seconds
                
        except KeyboardInterrupt:
            print("\nClient shutting down...")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    client = UDPClient()
    client.run()