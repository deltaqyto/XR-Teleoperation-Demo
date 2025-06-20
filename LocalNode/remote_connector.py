import json
import socket
import threading
import queue
import asyncio
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate
import websockets


class JSONRemoteConnector:
    def __init__(self, host: str = None, port: int = None):
        self.host = host
        self.port = port
        self.socket = None
        self.connected = False
        self.running = False

        self.incoming_queue = queue.Queue()
        self.outgoing_queue = queue.Queue()
        self.worker_thread = None

    def connect(self) -> bool:
        if self.host is None or self.port is None:
            raise ValueError(f"Tried to connect when host or port are none (host={self.host}, port={self.port})")
        """Establish TCP connection"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.connected = True
            self.running = True

            self.worker_thread = threading.Thread(target=self._worker_loop)
            self.worker_thread.start()
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def disconnect(self):
        """Close connection and cleanup"""
        self.running = False
        self.connected = False
        if self.socket:
            self.socket.close()
        if self.worker_thread:
            self.worker_thread.join()

    def reconnect(self, host: str = None, port: int = None) -> bool:
        """Connect to new host/port, clearing queues"""
        self.disconnect()
        self._clear_queues()
        self.host = host
        self.port = port
        return self.connect()

    def send_data(self, data):
        """Queue data for sending"""
        if self.connected:
            self.outgoing_queue.put(data)

    def get_received_data(self) -> list:
        """Get all received messages"""
        messages = []
        while not self.incoming_queue.empty():
            try:
                messages.append(self.incoming_queue.get_nowait())
            except queue.Empty:
                break
        return messages

    def is_connected(self) -> bool:
        return self.connected

    def _clear_queues(self):
        while not self.incoming_queue.empty():
            self.incoming_queue.get_nowait()
        while not self.outgoing_queue.empty():
            self.outgoing_queue.get_nowait()

    def _worker_loop(self):
        """Main worker thread for send/receive"""
        recv_buffer = b''

        while self.running:
            try:
                # Handle outgoing messages
                if not self.outgoing_queue.empty():
                    data = self.outgoing_queue.get_nowait()
                    json_data = json.dumps(data).encode()
                    # Length-prefix the message
                    message = len(json_data).to_bytes(4, 'big') + json_data
                    self.socket.sendall(message)

                # Handle incoming messages (non-blocking)
                self.socket.settimeout(0.1)
                try:
                    data = self.socket.recv(4096)
                    if data:
                        recv_buffer += data

                        # Process complete messages from buffer
                        while len(recv_buffer) >= 4:
                            # Read message length
                            msg_length = int.from_bytes(recv_buffer[:4], 'big')

                            # Check if we have the complete message
                            if len(recv_buffer) >= 4 + msg_length:
                                # Extract message
                                json_data = recv_buffer[4:4 + msg_length]
                                recv_buffer = recv_buffer[4 + msg_length:]

                                # Parse and queue
                                parsed_data = json.loads(json_data.decode())
                                self.incoming_queue.put(parsed_data)
                            else:
                                # Wait for more data
                                break

                except socket.timeout:
                    continue

            except Exception as e:
                print(f"Worker error: {e}")
                self.connected = False
                break
