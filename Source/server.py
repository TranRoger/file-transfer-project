import socket
import json
import hashlib
import time

class FileTransferProtocol:
    """Application-level protocol for reliable file transfer."""
    # Protocol constants
    START_CHUNK = 'START'
    DATA_CHUNK = 'DATA'
    END_CHUNK = 'END'
    ACK = 'ACK'
    NACK = 'NACK'

    @staticmethod
    def create_packet(packet_type, file_name, sequence, data=None, total_chunks=None, checksum=None):
        """Create a structured packet for file transfer."""
        packet = {
            'type': packet_type,
            'file_name': file_name,
            'sequence': sequence
        }
        
        if data is not None:
            packet['data'] = data.decode('latin-1') if isinstance(data, bytes) else data
        
        if total_chunks is not None:
            packet['total_chunks'] = total_chunks
        
        if checksum is not None:
            packet['checksum'] = checksum
        
        return json.dumps(packet).encode()

    @staticmethod
    def verify_checksum(data, expected_checksum):
        """Verify data integrity using MD5 checksum."""
        current_checksum = hashlib.md5(data).hexdigest()
        return current_checksum == expected_checksum

"""
def send_file_chunk(sock, client_addr, file_name, offset, length):
    try:
        with open(file_name, "rb") as f:
            f.seek(offset)
            data = f.read(length)
            
            # Calculate total chunks and chunk size
            chunk_size = 1024
            total_chunks = (len(data) + chunk_size - 1) // chunk_size
            
            # Send START packet
            start_packet = FileTransferProtocol.create_packet(
                FileTransferProtocol.START_CHUNK, 
                file_name, 
                0, 
                total_chunks=total_chunks
            )
            sock.sendto(start_packet, client_addr)
            
            # Send data chunks
            for seq in range(total_chunks):
                chunk = data[seq*chunk_size : (seq+1)*chunk_size]
                
                # Calculate checksum for this chunk
                checksum = hashlib.md5(chunk).hexdigest()
                
                # Create and send data packet
                data_packet = FileTransferProtocol.create_packet(
                    FileTransferProtocol.DATA_CHUNK, 
                    file_name, 
                    seq, 
                    data=chunk, 
                    checksum=checksum
                )
                sock.sendto(data_packet, client_addr)
                
                # Wait for ACK with timeout
                try:
                    sock.settimeout(2)  # 2-second timeout
                    response, _ = sock.recvfrom(1024)
                    ack_packet = json.loads(response.decode())
                    
                    # If NACK received, resend chunk
                    if ack_packet['type'] == FileTransferProtocol.NACK:
                        # Resend the chunk immediately
                        sock.sendto(data_packet, client_addr)
                except socket.timeout:
                    # Timeout - resend chunk
                    sock.sendto(data_packet, client_addr)
            
            # Send END packet
            end_packet = FileTransferProtocol.create_packet(
                FileTransferProtocol.END_CHUNK, 
                file_name, 
                total_chunks
            )
            sock.sendto(end_packet, client_addr)
    
    except FileNotFoundError:
        error_packet = FileTransferProtocol.create_packet(
            'ERROR', 
            file_name, 
            0, 
            data=f"File {file_name} not found"
        )
        sock.sendto(error_packet, client_addr)
"""
        
def send_file_chunk(sock, client_addr, file_name, offset, length):
    """Send a file chunk using enhanced UDP protocol."""
    try:
        with open(file_name, "rb") as f:
            f.seek(offset)
            data = f.read(length)
            
            chunk_size = 1024
            total_chunks = (len(data) + chunk_size - 1) // chunk_size
            
            # Send START packet
            start_packet = FileTransferProtocol.create_packet(
                FileTransferProtocol.START_CHUNK, 
                file_name, 
                0, 
                total_chunks=total_chunks
            )
            sock.sendto(start_packet, client_addr)
            
            # Send data chunks
            for seq in range(total_chunks):
                chunk = data[seq*chunk_size : (seq+1)*chunk_size]
                checksum = hashlib.md5(chunk).hexdigest()
                
                data_packet = FileTransferProtocol.create_packet(
                    FileTransferProtocol.DATA_CHUNK, 
                    file_name, 
                    seq, 
                    data=chunk, 
                    checksum=checksum
                )
                sock.sendto(data_packet, client_addr)
                
                # Wait for ACK with timeout
                try:
                    sock.settimeout(2)
                    response, _ = sock.recvfrom(1024)
                    try:
                        ack_packet = json.loads(response.decode())
                        if ack_packet['type'] == FileTransferProtocol.NACK:
                            sock.sendto(data_packet, client_addr)
                    except json.JSONDecodeError:
                        # If response isn't valid JSON, treat as NACK
                        sock.sendto(data_packet, client_addr)
                except socket.timeout:
                    sock.sendto(data_packet, client_addr)
            
            # Send END packet
            end_packet = FileTransferProtocol.create_packet(
                FileTransferProtocol.END_CHUNK, 
                file_name, 
                total_chunks
            )
            sock.sendto(end_packet, client_addr)
    
    except FileNotFoundError:
        error_packet = FileTransferProtocol.create_packet(
            'ERROR', 
            file_name, 
            0, 
            data=f"File {file_name} not found"
        )
        sock.sendto(error_packet, client_addr)

def main():
    """Start the UDP server and handle file transfer requests."""
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("10.250.91.1", 12345))
    print("UDP Server listening on port 12345...")

    while True:
        # Receive request
        data, client_addr = server.recvfrom(1024)
        request = data.decode()

        if request == "LIST":
            # Send list of available files
            with open("files.txt", "r") as f:
                files_list = f.read()
            server.sendto(files_list.encode(), client_addr)
        
        elif request.startswith("DOWNLOAD"):
            # Parse download request
            parts = request.split()
            file_name = parts[1]
            offset = int(parts[2])
            length = int(parts[3])
            
            # Send file chunk
            send_file_chunk(server, client_addr, file_name, offset, length)

if __name__ == "__main__":
    main()