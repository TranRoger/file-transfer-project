import socket
import json
import hashlib
import time
import base64
import threading

CHUNK_SIZE = 1024*2
class FileTransferProtocol:
    """Application-level protocol for reliable file transfer."""
    # Protocol constants
    START_CHUNK = 'START'
    DATA_CHUNK = 'DATA'
    END_CHUNK = 'END'
    ACK = 'ACK'
    NACK = 'NACK'
    PROTO_CONST = [START_CHUNK, DATA_CHUNK, END_CHUNK, ACK, NACK]
    REQUEST_CONST = ['LIST', 'DOWNLOAD']

    @staticmethod
    def create_packet(packet_type, file_name, sequence, data=None, total_chunks=None, checksum=None):
        """Create a structured packet for file transfer."""
        packet = {
            'type': packet_type,
            'file_name': file_name,
            'sequence': sequence
        }
        
        if data is not None:
            # packet['data'] = data.decode() if isinstance(data, bytes) else data
            packet['data'] = base64.b64encode(data).decode("utf-8")
        
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
    
def send_start_packet(sock, client_addr, file_name, total_chunks):
    """Send the start packet to the client."""
    start_packet = FileTransferProtocol.create_packet(
        FileTransferProtocol.START_CHUNK, 
        file_name, 
        sequence=0, 
        total_chunks=total_chunks
    )
    sock.sendto(start_packet, client_addr)

def send_end_packet(sock, client_addr, file_name):
    """Send the end packet to the client."""
    end_packet = FileTransferProtocol.create_packet(
        FileTransferProtocol.END_CHUNK, 
        file_name, 
        0
    )
    sock.sendto(end_packet, client_addr)


def send_file_chunk(sock, client_addr, file_name, offset, length):
    """Send a file chunk using enhanced UDP protocol."""
    try:
        with open(file_name, "rb") as f:
            f.seek(offset)
            data = f.read(length)
            
            # Calculate total chunks and chunk size
            chunk_size = CHUNK_SIZE
            total_chunks = (len(data) + chunk_size - 1) // chunk_size
            
            # Send START packet
            # wait for ACK
            send_start_packet(sock, client_addr, file_name, total_chunks)
            while True:
                # Wait for ACK with timeout
                try:
                    sock.settimeout(1)  # 2-second timeout
                    response, _ = sock.recvfrom(1024*4)
                    parsed = parse_packet(response)
                    
                    # If ACK received, break the loop
                    if parsed['type'] == FileTransferProtocol.ACK and parsed['file_name'] == file_name and parsed['sequence'] == 0 and parsed['offset'] == offset:
                        break
                except socket.timeout:
                    # Timeout - resend START packet
                    print("Timeout - resending START packet")
                    send_start_packet(sock, client_addr, file_name, total_chunks)
            
            
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
                    checksum=checksum,
                    data=chunk
                )
                sock.sendto(data_packet, client_addr)
                
                # time.sleep(0.05)

                # Wait for ACK with timeout
                try:
                    sock.settimeout(1)  # 2-second timeout
                    response, _ = sock.recvfrom(CHUNK_SIZE)
                    while not response:
                        response, _ = sock.recvfrom(CHUNK_SIZE)
                    parsed = parse_packet(response)
                    
                    # If NACK received, resend chunk
                    if parsed["type"] == FileTransferProtocol.NACK :
                        # Resend the chunk immediately
                        sock.sendto(data_packet, client_addr)
                except socket.timeout:
                    # Timeout - resend chunk
                    sock.sendto(data_packet, client_addr)
            
            send_end_packet(sock, client_addr, file_name)
            while True:
                # Wait for ACK with timeout
                try:
                    sock.settimeout(1)  # 2-second timeout
                    response, _ = sock.recvfrom(CHUNK_SIZE)
                    while not response:
                        response, _ = sock.recvfrom(CHUNK_SIZE)
                    parsed = parse_packet(response)
                    
                    # If ACK received, break the loop
                    if parsed["type"] == FileTransferProtocol.ACK:
                        break
                except socket.timeout:
                    # Timeout - resend END packet
                    send_end_packet(sock, client_addr, file_name)
            
    
    except FileNotFoundError:
        error_packet = FileTransferProtocol.create_packet(
            'ERROR', 
            file_name, 
            0, 
            data=f"File {file_name} not found"
        )
        sock.sendto(error_packet, client_addr)

def parse_packet(response):
    """Parse the incoming response from the client."""
    # expecting json format
    try:
        response_data = json.loads(response.decode())
        # Extract relevant fields
        response_data["type"] = response_data.get('type')
        response_data["file_name"] = response_data.get('file_name')
        response_data["offset"] = response_data.get('offset', 0)
        response_data["length"] = response_data.get('length', 1024)
        response_data["sequence"] = response_data.get('sequence', 0)
        # return type, file_name, offset, length, sequence
        return response_data
    except json.JSONDecodeError:
        print("Invalid response format")
        return None, None, None

active_transfers = []
lock = threading.Lock()  # To safely modify active_transfers

def handle_client(server, client_addr, data):
    """Handle client requests for LIST and DOWNLOAD."""
    parsed = parse_packet(data)
    print(f"Received request from {client_addr}: {parsed}")

    if parsed["type"] == "LIST":
        # Send list of available files
        with open("files.txt", "r") as f:
            files_list = f.read()
        server.sendto(files_list.encode(), client_addr)

    elif parsed["type"] == "DOWNLOAD":
        # Start a thread for file transfer
        transfer_thread = threading.Thread(target=send_file_chunk, args=(server, client_addr, parsed["file_name"], parsed["offset"], parsed["length"]))
        with lock:
            active_transfers.append(transfer_thread)
        transfer_thread.start()
        transfer_thread.join()  # Wait for the thread to finish
        
        with lock:
            active_transfers.remove(transfer_thread)

def handle_ack_nack(server, client_addr, data):
    """Process ACK/NACK packets and direct them to the correct file transfer thread."""
    parsed = parse_packet(data)
    print(f"Received ACK/NACK from {client_addr}: {parsed}")

    with lock:
        if client_addr in active_transfers:
            transfer_thread = active_transfers[client_addr]
            if transfer_thread.is_alive():
                # Notify the transfer thread (e.g., using a queue or shared variable)
                print(f"Notifying transfer thread of ACK/NACK for {client_addr}")
                # You can implement a queue-based approach here for better control.

def main():
    """Start the UDP server."""
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("10.250.91.1", 12345))
    print("UDP Server listening on port 12345...")

    while True:
        try:
            receive_window = CHUNK_SIZE // 4
            data, client_addr = server.recvfrom(receive_window)

            # Parse the packet type
            parsed = parse_packet(data)

            if parsed["type"] in FileTransferProtocol.PROTO_CONST:
                handle_ack_nack(server, client_addr, data)  # Handle ACK/NACK within existing transfer
            else:
                client_thread = threading.Thread(target=handle_client, args=(server, client_addr, data))
                client_thread.start()
        except socket.timeout:
            print("Timeout occurred, retrying...")
            time.sleep(10)
            continue  # Prevent termination due to TimeoutError

if __name__ == "__main__":
    main()