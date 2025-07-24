import socket
import json
import hashlib
import time
import base64
import select
import os
from collections import namedtuple

MULTIPLIER = 8
CHUNK_SIZE = 1024 * MULTIPLIER

# Define client state constants
IDLE = 0
SENDING_FILE = 1
WAITING_ACK = 2

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
    def create_packet(packet_type, file_name, sequence, data=None, total_chunks=None, checksum=None, offset=None):
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

        # For ACK-DOWNLOAD
        if offset is not None:
            packet['offset'] = offset
        
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
            print(f"Sending START packet for {file_name} to {client_addr}")
            send_start_packet(sock, client_addr, file_name, total_chunks)
            while True:
                # Wait for ACK with timeout
                try:
                    sock.settimeout(1)  # 2-second timeout
                    response, _ = sock.recvfrom(1024*4)
                    parsed = parse_packet(response)
                    
                    # If ACK received, break the loop
                    if parsed['type'] == FileTransferProtocol.ACK and parsed['file_name'] == file_name and parsed['sequence'] == 0 and parsed['offset'] == offset:
                        print(f"ACK received for START packet for {file_name}")
                        break
                    else:
                        print(f"Unexpected response: {parsed}")
                        send_start_packet(sock, client_addr, file_name, total_chunks)

                except socket.timeout:
                    # Timeout - resend START packet
                    print("Timeout - resending START packet")
                    send_start_packet(sock, client_addr, file_name, total_chunks)
                    continue
            
            
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

# Client state structure
ClientState = namedtuple('ClientState', ['state', 'file_name', 'offset', 'length', 'current_seq', 'total_chunks', 'last_activity'])

def prepare_file_chunk(file_name, offset, seq, chunk_size):
    """Prepare a file chunk for sending."""
    try:
        with open(file_name, "rb") as f:
            f.seek(offset + seq * chunk_size)
            chunk = f.read(chunk_size)
            
            # Calculate checksum for this chunk
            checksum = hashlib.md5(chunk).hexdigest()
            
            return chunk, checksum
    except (FileNotFoundError, IOError) as e:
        print(f"Error reading file: {e}")
        return None, None

def handle_list_request(sock, client_addr):
    """Handle LIST request from client."""
    with open("files.txt", "r") as f:
        files_list = f.read()
    sock.sendto(files_list.encode(), client_addr)
    return True

def handle_download_request(sock, client_addr, parsed, client_states):
    """Handle DOWNLOAD request from client."""
    file_name = parsed["file_name"]
    offset = parsed["offset"]
    length = parsed["length"]
    
    # Send ACK for DOWNLOAD request
    ack_packet = FileTransferProtocol.create_packet(
        FileTransferProtocol.ACK, 
        file_name, 
        0, 
        offset=offset
    )
    sock.sendto(ack_packet, client_addr)
    
    # Calculate total chunks
    chunk_size = CHUNK_SIZE
    try:
        file_size = os.path.getsize(file_name)
        actual_length = min(length, file_size - offset)
        total_chunks = (actual_length + chunk_size - 1) // chunk_size
        
        # Initialize client state for file transfer
        client_states[client_addr] = ClientState(
            state=SENDING_FILE,
            file_name=file_name,
            offset=offset,
            length=actual_length,
            current_seq=0,  # Start with sequence 0 (START packet)
            total_chunks=total_chunks,
            last_activity=time.time()
        )
        
        # Start by sending START packet
        start_packet = FileTransferProtocol.create_packet(
            FileTransferProtocol.START_CHUNK,
            file_name,
            0,
            total_chunks=total_chunks
        )
        sock.sendto(start_packet, client_addr)
        
        # Update client state to waiting for ACK
        client_states[client_addr] = ClientState(
            state=WAITING_ACK,
            file_name=file_name,
            offset=offset,
            length=actual_length,
            current_seq=0,
            total_chunks=total_chunks,
            last_activity=time.time()
        )
        
        return True
    except FileNotFoundError:
        error_packet = FileTransferProtocol.create_packet(
            'ERROR',
            file_name,
            0,
            data=f"File {file_name} not found".encode()
        )
        sock.sendto(error_packet, client_addr)
        return False

def handle_ack(sock, client_addr, parsed, client_states):
    """Handle ACK from client."""
    if client_addr not in client_states:
        return False
    
    state = client_states[client_addr]
    if parsed["file_name"] != state.file_name:
        return False
    
    # Update last activity time
    client_states[client_addr] = ClientState(
        state=state.state,
        file_name=state.file_name,
        offset=state.offset,
        length=state.length,
        current_seq=state.current_seq,
        total_chunks=state.total_chunks,
        last_activity=time.time()
    )
    
    if state.state == WAITING_ACK and parsed["sequence"] == 0 and parsed["type"] == FileTransferProtocol.ACK:
        # ACK for START packet received, start sending data
        client_states[client_addr] = ClientState(
            state=SENDING_FILE,
            file_name=state.file_name,
            offset=state.offset,
            length=state.length,
            current_seq=1,  # Move to first data packet
            total_chunks=state.total_chunks,
            last_activity=time.time()
        )
        
        # Send first data packet
        chunk, checksum = prepare_file_chunk(state.file_name, state.offset, 0, CHUNK_SIZE)
        if chunk:
            data_packet = FileTransferProtocol.create_packet(
                FileTransferProtocol.DATA_CHUNK,
                state.file_name,
                1,
                data=chunk,
                checksum=checksum
            )
            sock.sendto(data_packet, client_addr)
            
            # Update client state
            client_states[client_addr] = ClientState(
                state=WAITING_ACK,
                file_name=state.file_name,
                offset=state.offset,
                length=state.length,
                current_seq=1,
                total_chunks=state.total_chunks,
                last_activity=time.time()
            )
    
    elif state.state == WAITING_ACK and parsed["sequence"] == state.current_seq:
        # ACK for data packet received
        next_seq = state.current_seq + 1
        
        if next_seq <= state.total_chunks:
            # Send next data packet
            chunk, checksum = prepare_file_chunk(state.file_name, state.offset, next_seq - 1, CHUNK_SIZE)
            if chunk:
                data_packet = FileTransferProtocol.create_packet(
                    FileTransferProtocol.DATA_CHUNK,
                    state.file_name,
                    next_seq,
                    data=chunk,
                    checksum=checksum
                )
                sock.sendto(data_packet, client_addr)
                
                # Update client state
                client_states[client_addr] = ClientState(
                    state=WAITING_ACK,
                    file_name=state.file_name,
                    offset=state.offset,
                    length=state.length,
                    current_seq=next_seq,
                    total_chunks=state.total_chunks,
                    last_activity=time.time()
                )
        else:
            # All data packets sent, send END packet
            end_packet = FileTransferProtocol.create_packet(
                FileTransferProtocol.END_CHUNK,
                state.file_name,
                0
            )
            sock.sendto(end_packet, client_addr)
            
            # Mark transfer as complete
            client_states[client_addr] = ClientState(
                state=IDLE,
                file_name="",
                offset=0,
                length=0,
                current_seq=0,
                total_chunks=0,
                last_activity=time.time()
            )
    
    return True

def handle_nack(sock, client_addr, parsed, client_states):
    """Handle NACK from client."""
    if client_addr not in client_states:
        return False
    
    state = client_states[client_addr]
    if parsed["file_name"] != state.file_name:
        return False
    
    # Resend the requested packet
    seq = parsed["sequence"]
    chunk, checksum = prepare_file_chunk(state.file_name, state.offset, seq - 1, CHUNK_SIZE)
    if chunk:
        data_packet = FileTransferProtocol.create_packet(
            FileTransferProtocol.DATA_CHUNK,
            state.file_name,
            seq,
            data=chunk,
            checksum=checksum
        )
        sock.sendto(data_packet, client_addr)
    
    return True

def check_timeouts(sock, client_states, timeout=5):
    """Check for client timeouts and resend packets if needed."""
    current_time = time.time()
    for client_addr, state in list(client_states.items()):
        if current_time - state.last_activity > timeout:
            if state.state == WAITING_ACK:
                # Resend the last packet
                if state.current_seq == 0:
                    # Resend START packet
                    start_packet = FileTransferProtocol.create_packet(
                        FileTransferProtocol.START_CHUNK,
                        state.file_name,
                        0,
                        total_chunks=state.total_chunks
                    )
                    sock.sendto(start_packet, client_addr)
                else:
                    # Resend data packet
                    chunk, checksum = prepare_file_chunk(state.file_name, state.offset, state.current_seq - 1, CHUNK_SIZE)
                    if chunk:
                        data_packet = FileTransferProtocol.create_packet(
                            FileTransferProtocol.DATA_CHUNK,
                            state.file_name,
                            state.current_seq,
                            data=chunk,
                            checksum=checksum
                        )
                        sock.sendto(data_packet, client_addr)
            
            # Update last activity time
            client_states[client_addr] = ClientState(
                state=state.state,
                file_name=state.file_name,
                offset=state.offset,
                length=state.length,
                current_seq=state.current_seq,
                total_chunks=state.total_chunks,
                last_activity=current_time
            )

def main():
    """Start the UDP server with select-based I/O multiplexing."""
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("192.168.1.23", 12345))
    server.setblocking(False)  # Set socket to non-blocking mode
    print("UDP Server listening on port 12345...")
    
    # Dictionary to store client states
    client_states = {}
    
    # Buffer for received data
    receive_window = 1024 * 4
    
    while True:
        # Check for timeouts
        check_timeouts(server, client_states)
        
        # Use select to wait for incoming data with timeout
        ready_sockets, _, _ = select.select([server], [], [], 1.0)  # 1 second timeout
        
        for sock in ready_sockets:
            try:
                data, client_addr = sock.recvfrom(receive_window)
                parsed = parse_packet(data)
                
                if not parsed:
                    continue
                
                print(f"Received request from {client_addr}: {parsed}")
                
                # Handle different packet types
                if parsed["type"] == "LIST":
                    handle_list_request(server, client_addr)
                elif parsed["type"] == "DOWNLOAD":
                    handle_download_request(server, client_addr, parsed, client_states)
                elif parsed["type"] == FileTransferProtocol.ACK:
                    handle_ack(server, client_addr, parsed, client_states)
                elif parsed["type"] == FileTransferProtocol.NACK:
                    handle_nack(server, client_addr, parsed, client_states)
            except BlockingIOError:
                # No data available, continue
                continue
            except Exception as e:
                print(f"Error handling client: {e}")
                continue

if __name__ == "__main__":
    main()