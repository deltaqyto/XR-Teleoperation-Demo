import json
import socket
import threading
import queue
import asyncio
from aiortc import RTCPeerConnection, RTCSessionDescription
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


class RTCRemoteConnector:
    def __init__(self, host: str = None, port: int = None):
        self.host = host
        self.port = port
        self.pc = None
        self.websocket = None
        self.connected = False
        self.connection_thread = None
        self.running = False

    def connect(self) -> bool:
        """Start WebRTC connection (non-blocking)"""
        if self.host is None or self.port is None:
            raise ValueError(f"Tried to connect when host or port are none (host={self.host}, port={self.port})")

        if self.connection_thread and self.connection_thread.is_alive():
            return False

        self.running = True
        self.connection_thread = threading.Thread(target=self._connection_worker)
        self.connection_thread.start()
        return True

    def disconnect(self):
        """Close RTC connection (non-blocking)"""
        self.running = False
        self.connected = False

        if self.connection_thread and self.connection_thread.is_alive():
            # Start disconnect in background
            threading.Thread(target=self._disconnect_worker).start()

    def reconnect(self, host: str = None, port: int = None) -> bool:
        """Connect to new host/port"""
        self.disconnect()
        if host is not None:
            self.host = host
        if port is not None:
            self.port = port
        return self.connect()

    def _connection_worker(self):
        """Background thread for WebRTC connection"""
        try:
            asyncio.run(self._async_connect())
        except Exception as e:
            print(f"RTC connection failed: {e}")
            self.connected = False

    def _disconnect_worker(self):
        """Background thread for WebRTC disconnect"""
        try:
            asyncio.run(self._async_disconnect())
        except Exception as e:
            print(f"RTC disconnect error: {e}")

    async def _async_connect(self) -> bool:
        """Establish WebRTC connection"""
        try:
            uri = f"ws://{self.host}:{self.port}"
            self.websocket = await websockets.connect(uri)
            self.pc = RTCPeerConnection()

            # Create offer and negotiate
            offer = await self.pc.createOffer()
            await self.pc.setLocalDescription(offer)

            await self.websocket.send(json.dumps({
                "type": "offer",
                "sdp": self.pc.localDescription.sdp
            }))

            # Wait for answer
            async for message in self.websocket:
                data = json.loads(message)
                if data["type"] == "answer":
                    await self.pc.setRemoteDescription(RTCSessionDescription(
                        sdp=data["sdp"], type=data["type"]))
                    self.connected = True
                    return True

        except Exception as e:
            print(f"RTC connection failed: {e}")
            return False

    async def _async_disconnect(self):
        """Close RTC connection"""
        self.connected = False
        if self.pc:
            await self.pc.close()
        if self.websocket:
            await self.websocket.close()

    def get_peer_connection(self) -> RTCPeerConnection:
        """Get the RTCPeerConnection for adding tracks"""
        return self.pc

    def is_connected(self) -> bool:
        return self.connected
