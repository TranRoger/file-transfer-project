import socket
import json
import hashlib
import base64
import threading
import queue
import time

CHUNK_SIZE = 1024 * 2
NUM_WORKERS = 4  # Worker threads count

class FileTransferProtocol:
    """Application-level protocol for reliable file transfer."""
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
            packet['data'] = base64.b64encode(data).decode("utf-8")

        if total_chunks is not None:
            packet['total_chunks'] = total_chunks

        if checksum is not None:
            packet['checksum'] = checksum

        return json.dumps(packet).encode()

    @staticmethod
    def verify_checksum(data, expected_checksum):
        """Verify data integrity using MD5 checksum."""
        return hashlib.md5(data).hexdigest() == expected_checksum


class UDPServer:
    """Optimized UDP File Server with worker threads and efficient request handling."""

    def __init__(self, ip="0.0.0.0", port=12345):
        self.server_addr = (ip, port)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind(self.server_addr)

        self.packet_queue = queue.Queue()  # Packet queue for processing
        self.active_transfers = {}  # Track active file transfers
        self.lock = threading.Lock()

        self.worker_threads = []
        self.running = True

    def start(self):
        """Start server with receiver and worker threads."""
        print(f"UDP Server started on {self.server_addr}")

        receiver_thread = threading.Thread(target=self.receive_packets, daemon=True)
        receiver_thread.start()

        for _ in range(NUM_WORKERS):
            worker = threading.Thread(target=self.worker_process, daemon=True)
            worker.start()
            self.worker_threads.append(worker)

        receiver_thread.join()

    def receive_packets(self):
        """Main receiver thread that listens for incoming packets."""
        while self.running:
            try:
                data, client_addr = self.server_socket.recvfrom(CHUNK_SIZE)
                self.packet_queue.put((data, client_addr))  # Enqueue packet for workers
            except socket.error as e:
                print(f"Socket error: {e}")

    def worker_process(self):
        """Worker threads process client requests."""
        while self.running:
            try:
                data, client_addr = self.packet_queue.get(timeout=1)
                parsed = self.parse_packet(data)

                if parsed["type"] in FileTransferProtocol.PROTO_CONST:
                    self.handle_ack_nack(client_addr, parsed)
                else:
                    self.handle_client_request(client_addr, parsed)

            except queue.Empty:
                continue  # No packet to process

    def parse_packet(self, data):
        """Parse incoming packet."""
        try:
            return json.loads(data.decode())
        except json.JSONDecodeError:
            print("Invalid packet format")
            return {}

    def handle_client_request(self, client_addr, parsed):
        """Process client LIST and DOWNLOAD requests."""
        print(f"Received request from {client_addr}: {parsed}")

        if parsed["type"] == "LIST":
            self.send_file_list(client_addr)

        elif parsed["type"] == "DOWNLOAD":
            transfer_thread = threading.Thread(target=self.send_file_chunk,
                                               args=(client_addr, parsed["file_name"],
                                                     parsed["offset"], parsed["length"]),
                                               daemon=True)
            with self.lock:
                self.active_transfers[client_addr] = transfer_thread

            transfer_thread.start()

    def handle_ack_nack(self, client_addr, parsed):
        """Process ACK/NACK packets."""
        with self.lock:
            if client_addr in self.active_transfers:
                transfer_thread = self.active_transfers[client_addr]
                if transfer_thread.is_alive():
                    print(f"Notifying transfer thread of ACK/NACK for {client_addr}")

    def send_file_list(self, client_addr):
        """Send list of available files."""
        # files = {"example.txt": "5MB", "large_video.mp4": "500MB"}
        with open("files.txt", "r") as f:
            files = f.read()
        self.server_socket.sendto(files.encode(), client_addr)

    def send_file_chunk(self, client_addr, file_name, offset, length):
        """Send a file chunk with improved efficiency."""
        try:
            with open(file_name, "rb") as f:
                f.seek(offset)
                data = f.read(length)

                total_chunks = (len(data) + CHUNK_SIZE - 1) // CHUNK_SIZE

                # Send START packet
                self.send_start_packet(client_addr, file_name, total_chunks)

                for seq in range(total_chunks):
                    chunk = data[seq * CHUNK_SIZE: (seq + 1) * CHUNK_SIZE]
                    checksum = hashlib.md5(chunk).hexdigest()

                    data_packet = FileTransferProtocol.create_packet(
                        FileTransferProtocol.DATA_CHUNK,
                        file_name,
                        seq,
                        checksum=checksum,
                        data=chunk
                    )
                    self.server_socket.sendto(data_packet, client_addr)

                    # Wait for ACK/NACK with timeout
                    self.wait_for_ack(client_addr, file_name, seq)

                # Send END packet
                self.send_end_packet(client_addr, file_name)

        except FileNotFoundError:
            error_packet = FileTransferProtocol.create_packet(
                'ERROR', file_name, 0, data=f"File {file_name} not found"
            )
            self.server_socket.sendto(error_packet, client_addr)

    def wait_for_ack(self, client_addr, file_name, seq):
        """Wait for ACK and handle retransmissions."""
        try:
            self.server_socket.settimeout(1)
            response, _ = self.server_socket.recvfrom(CHUNK_SIZE)
            parsed = self.parse_packet(response)

            if parsed["type"] == FileTransferProtocol.NACK:
                print(f"Resending chunk {seq} for {client_addr}")
                self.send_file_chunk(client_addr, file_name, seq * CHUNK_SIZE, CHUNK_SIZE)

        except socket.timeout:
            print(f"Timeout on chunk {seq}, resending")
            self.send_file_chunk(client_addr, file_name, seq * CHUNK_SIZE, CHUNK_SIZE)

    def send_start_packet(self, client_addr, file_name, total_chunks):
        """Send the start packet to the client."""
        start_packet = FileTransferProtocol.create_packet(
            FileTransferProtocol.START_CHUNK,
            file_name,
            sequence=0,
            total_chunks=total_chunks
        )
        self.server_socket.sendto(start_packet, client_addr)

    def send_end_packet(self, client_addr, file_name):
        """Send the end packet to the client."""
        end_packet = FileTransferProtocol.create_packet(
            FileTransferProtocol.END_CHUNK,
            file_name,
            sequence=0
        )
        self.server_socket.sendto(end_packet, client_addr)


if __name__ == "__main__":
    server = UDPServer(ip="0.0.0.0", port=12345)
    server.start()
