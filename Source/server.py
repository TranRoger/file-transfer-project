import socket
import os
import json

def send_file_chunk(sock, client_addr, file_name, offset, length):
    """Send a file chunk using UDP with sequence numbering."""
    try:
        with open(file_name, "rb") as f:
            f.seek(offset)
            data = f.read(length)
            
            # Prepare packet metadata
            packet_metadata = {
                "file_name": file_name,
                "offset": offset,
                "total_length": length,
                "chunk_length": len(data)
            }
            
            # Send metadata first
            sock.sendto(json.dumps(packet_metadata).encode(), client_addr)
            
            # Send data in smaller chunks
            chunk_size = 1024
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i+chunk_size]
                sequence_packet = {
                    "sequence": i // chunk_size,
                    "data": chunk.decode('latin-1') if isinstance(chunk, bytes) else chunk
                }
                sock.sendto(json.dumps(sequence_packet).encode(), client_addr)
    
    except FileNotFoundError:
        error_msg = json.dumps({"error": f"File {file_name} not found"})
        sock.sendto(error_msg.encode(), client_addr)

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