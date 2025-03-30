import socket
import json
import hashlib
import base64
import threading
import queue
import time
import uuid

# Global variables
MULTIPLIER = 16
CHUNK_SIZE = 1024 * MULTIPLIER  # Bytes
lock = threading.Lock()  # To ensure thread safety
client_queues = {}  # Dictionary of queues for each client
client_processors = {}  # Dictionary of processor threads for each client
active_transfers = {}  # Dictionary to store ongoing file transfers
# Store chunks for resending in case of NACKs
transfer_chunks = {}  # Dictionary to store sent chunks for each transfer_id

class FileTransferProtocol:
    """Application-level protocol for reliable file transfer."""
    START_CHUNK = 'START'
    DATA_CHUNK = 'DATA'
    END_CHUNK = 'END'
    ACK = 'ACK'
    NACK = 'NACK'
    ERROR = 'ERROR'
    PROTO_CONST = [START_CHUNK, DATA_CHUNK, END_CHUNK, ACK, NACK, ERROR]
    REQUEST_CONST = ['LIST', 'DOWNLOAD']

    @staticmethod
    def create_packet(packet_type, file_name, sequence, data=None, total_chunks=None, checksum=None, client_port=None):
        """Create a structured packet for file transfer."""
        packet = {
            'type': packet_type,
            'file_name': file_name,
            'sequence': sequence
        }
        
        if data is not None:
            packet['data'] = base64.b64encode(data).decode("utf-8") if isinstance(data, bytes) else data
        
        if total_chunks is not None:
            packet['total_chunks'] = total_chunks
        
        if checksum is not None:
            packet['checksum'] = checksum
            
        if client_port is not None:
            packet['client_port'] = client_port
        
        return json.dumps(packet).encode()

def parse_packet(response):
    """Parse the incoming response from the client."""
    try:
        return json.loads(response.decode())
    except (json.JSONDecodeError, UnicodeDecodeError):
        print("Invalid response format")
        return None

def get_client_key(client_addr, client_port=None):
    """Create a unique key for a client based on address and optional port."""
    if client_port:
        return f"{client_addr[0]}:{client_port}"
    return f"{client_addr[0]}:{client_addr[1]}"

def receiver_thread(server):
    """Continuously receives packets from clients and directs them to client-specific queues."""
    while True:
        try:
            data, client_addr = server.recvfrom(CHUNK_SIZE)
            parsed = parse_packet(data)
            
            if parsed is None:
                continue
                
            # Extract client-specified port if available
            client_port = parsed.get('client_port', client_addr[1])
            client_key = get_client_key(client_addr, client_port)
            
            with lock:
                if client_key not in client_queues:
                    # Create a new queue for this client
                    client_queues[client_key] = queue.Queue()
                    # Start a processor thread for this client
                    proc = threading.Thread(target=client_processor, args=(server, client_key, client_addr[0], client_port))
                    proc.daemon = True
                    proc.start()
                    client_processors[client_key] = proc
            
            # Add to the client's specific queue
            client_queues[client_key].put(data)
            
        except Exception as e:
            print(f"Error in receiver thread: {e}")

def client_processor(server, client_key, client_ip, client_port):
    """Process packets for a specific client."""
    print(f"Started processor for client {client_key}")
    while True:
        try:
            data = client_queues[client_key].get()
            parsed = parse_packet(data)
            
            if parsed is None:
                continue
            
            client_addr = (client_ip, client_port)
            
            if parsed["type"] in FileTransferProtocol.PROTO_CONST:
                handle_ack_nack(server, client_addr, client_key, parsed)
            elif parsed["type"] in FileTransferProtocol.REQUEST_CONST:
                handle_client_request(server, client_addr, client_key, parsed)
            else:
                print(f"Unknown packet type: {parsed['type']}")
                
        except queue.Empty:
            # Queue is empty, wait for more data
            time.sleep(0.01)
        except Exception as e:
            print(f"Error in client processor {client_key}: {e}")

def handle_client_request(server, client_addr, client_key, parsed):
    """Handle client requests for LIST and DOWNLOAD."""
    print(f"Handling request from {client_key}: {parsed['type']}")

    if parsed["type"] == "LIST":
        with open("files.txt", "r") as f:
            files_list = f.read()
        server.sendto(files_list.encode(), client_addr)

    elif parsed["type"] == "DOWNLOAD":
        # Create a unique transfer ID for this request
        transfer_id = f"{client_key}_{parsed['file_name']}_{uuid.uuid4().hex[:8]}"
        
        # Create a dictionary to store sent chunks for this transfer
        with lock:
            transfer_chunks[transfer_id] = {}
        
        # Create and start a new thread for this file transfer
        transfer_thread = threading.Thread(
            target=send_file_chunk,
            args=(server, client_addr, client_key, parsed, transfer_id)
        )
        
        with lock:
            active_transfers[transfer_id] = {
                'thread': transfer_thread,
                'client_key': client_key,
                'file_name': parsed['file_name'],
                'status': 'starting'
            }
            
        transfer_thread.daemon = True
        transfer_thread.start()
        print(f"Started transfer {transfer_id} for {client_key}")

def handle_ack_nack(server, client_addr, client_key, parsed):
    """Process ACK/NACK packets and update transfer status."""
    file_name = parsed.get("file_name", "")
    sequence = parsed.get("sequence", -1)
    
    print(f"Received {parsed['type']} from {client_key} for {file_name}, sequence {sequence}")
    
    # Find the related transfer
    with lock:
        for transfer_id, transfer_info in active_transfers.items():
            if transfer_info['client_key'] == client_key and transfer_info['file_name'] == file_name:
                if parsed["type"] == FileTransferProtocol.NACK:
                    # Handle NACK by resending the chunk
                    resend_chunk(server, client_addr, transfer_id, sequence)
                elif parsed["type"] == FileTransferProtocol.ACK:
                    # If ACK, update progress tracking
                    print(f"Received ACK for {file_name}, sequence {sequence}")
                    # We could potentially remove the ACKed chunk from memory to save space
                    # But keeping it for now in case we need to resend it later
                break

def resend_chunk(server, client_addr, transfer_id, sequence):
    """Resend a specific chunk to the client."""
    try:
        with lock:
            if transfer_id in transfer_chunks and sequence in transfer_chunks[transfer_id]:
                chunk_info = transfer_chunks[transfer_id][sequence]
                file_name = chunk_info['file_name']
                chunk_data = chunk_info['data']
                checksum = chunk_info['checksum']
                client_port = chunk_info['client_port']
                
                print(f"Resending chunk {sequence} for {file_name} to {client_addr[0]}:{client_port}")
                
                # Recreate the data packet with the cached data
                data_packet = FileTransferProtocol.create_packet(
                    FileTransferProtocol.DATA_CHUNK,
                    file_name,
                    sequence,
                    data=chunk_data,
                    checksum=checksum,
                    client_port=client_port
                )
                server.sendto(data_packet, client_addr)
                print(f"Resent chunk {sequence} for {file_name}")
            else:
                print(f"Warning: Chunk {sequence} not found in transfer {transfer_id} cache")
    except Exception as e:
        print(f"Error resending chunk {sequence} for transfer {transfer_id}: {e}")

def send_file_chunk(sock, client_addr, client_key, parsed, transfer_id):
    """Send file in chunks with status tracking."""
    file_name = parsed["file_name"]
    offset = parsed.get("offset", 0)
    client_port = parsed.get("client_port", client_addr[1])
    
    try:
        with open(file_name, "rb") as f:
            # Get file size and calculate total chunks
            f.seek(0, 2)  # Go to end of file
            file_size = f.tell()
            f.seek(offset)  # Go back to requested offset
            
            chunk_size = CHUNK_SIZE
            remaining_size = file_size - offset
            total_chunks = (remaining_size + chunk_size - 1) // chunk_size
            
            with lock:
                if transfer_id in active_transfers:
                    active_transfers[transfer_id]['status'] = 'in_progress'
                    active_transfers[transfer_id]['total_chunks'] = total_chunks
                    active_transfers[transfer_id]['current_chunk'] = 0
            
            # Send START packet
            start_packet = FileTransferProtocol.create_packet(
                FileTransferProtocol.START_CHUNK,
                file_name,
                sequence=0,
                total_chunks=total_chunks,
                client_port=client_port
            )
            sock.sendto(start_packet, client_addr)
            print(f"Sent START packet for {file_name} to {client_key}, total chunks: {total_chunks}")
            
            # Send data chunks
            for seq in range(total_chunks):
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                    
                checksum = hashlib.md5(chunk).hexdigest()
                
                with lock:
                    if transfer_id in active_transfers:
                        active_transfers[transfer_id]['current_chunk'] = seq
                    
                    # Store chunk data for potential resending
                    if transfer_id in transfer_chunks:
                        transfer_chunks[transfer_id][seq] = {
                            'file_name': file_name,
                            'data': chunk,
                            'checksum': checksum,
                            'client_port': client_port
                        }
                
                data_packet = FileTransferProtocol.create_packet(
                    FileTransferProtocol.DATA_CHUNK,
                    file_name,
                    seq,
                    data=chunk,
                    checksum=checksum,
                    client_port=client_port
                )
                sock.sendto(data_packet, client_addr)
                print(f"Sent chunk {seq}/{total_chunks} for {file_name} to {client_key}")
                
                # Small delay to prevent overwhelming network/receiver
                time.sleep(0.002)
            
            # Send END packet
            end_packet = FileTransferProtocol.create_packet(
                FileTransferProtocol.END_CHUNK,
                file_name,
                sequence=total_chunks,
                client_port=client_port
            )
            sock.sendto(end_packet, client_addr)
            print(f"Sent END packet for {file_name} to {client_key}")
            
            with lock:
                if transfer_id in active_transfers:
                    active_transfers[transfer_id]['status'] = 'completed'
    
    except FileNotFoundError:
        error_msg = f"File {file_name} not found"
        print(error_msg)
        error_packet = FileTransferProtocol.create_packet(
            FileTransferProtocol.ERROR,
            file_name,
            sequence=0,
            data=error_msg,
            client_port=client_port
        )
        sock.sendto(error_packet, client_addr)
        
        with lock:
            if transfer_id in active_transfers:
                active_transfers[transfer_id]['status'] = 'error'
                active_transfers[transfer_id]['error'] = error_msg
    
    except Exception as e:
        error_msg = f"Error transferring {file_name}: {str(e)}"
        print(error_msg)
        error_packet = FileTransferProtocol.create_packet(
            FileTransferProtocol.ERROR,
            file_name,
            sequence=0,
            data=error_msg,
            client_port=client_port
        )
        sock.sendto(error_packet, client_addr)
        
        with lock:
            if transfer_id in active_transfers:
                active_transfers[transfer_id]['status'] = 'error'
                active_transfers[transfer_id]['error'] = error_msg
    
    finally:
        # Clean up transfer after a delay to allow for any final ACKs or NACKs
        def cleanup_transfer():
            time.sleep(30)  # Wait longer before cleanup to allow for resends
            with lock:
                if transfer_id in active_transfers:
                    del active_transfers[transfer_id]
                if transfer_id in transfer_chunks:
                    del transfer_chunks[transfer_id]
                print(f"Cleaned up transfer {transfer_id}")
        
        cleanup_thread = threading.Thread(target=cleanup_transfer)
        cleanup_thread.daemon = True
        cleanup_thread.start()

def monitor_thread():
    """Monitor active transfers and clean up as needed."""
    while True:
        time.sleep(10)  # Check every 10 seconds
        with lock:
            # Report on active transfers
            if active_transfers:
                print(f"Active transfers: {len(active_transfers)}")
                for transfer_id, info in active_transfers.items():
                    print(f"  {transfer_id}: {info['file_name']} - {info['status']}")
            
            # Report on active clients
            if client_queues:
                print(f"Active clients: {len(client_queues)}")
                for client_key in client_queues:
                    print(f"  {client_key}: queue size {client_queues[client_key].qsize()}")
            
            # Report memory usage of chunk cache
            if transfer_chunks:
                total_chunks = sum(len(chunks) for chunks in transfer_chunks.values())
                print(f"Cached chunks: {total_chunks} chunks across {len(transfer_chunks)} transfers")

def main():
    """Start the UDP server with improved client handling."""
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_ip = "10.250.91.1"  # Use your actual server IP
    server_port = 12345
    server.bind((server_ip, server_port))
    print(f"UDP Server listening on {server_ip}:{server_port}...")
    
    # Start receiver thread
    recv_thread = threading.Thread(target=receiver_thread, args=(server,))
    recv_thread.daemon = True
    recv_thread.start()
    
    # Start monitor thread
    mon_thread = threading.Thread(target=monitor_thread)
    mon_thread.daemon = True
    mon_thread.start()
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Server shutting down...")
    finally:
        server.close()

if __name__ == "__main__":
    main()