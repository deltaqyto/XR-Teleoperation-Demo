import socket
import struct
import threading
import time
import cv2
import numpy as np


class UDPRemoteConnector:
    def __init__(self, chunk_size=1200, jpeg_quality=85, silent=False, log_interval=5.0, intrinsics_interval=2.0, localhost_port=None, extra_send_locations=None):
        self.chunk_size = chunk_size
        self.jpeg_quality = jpeg_quality
        self.silent = silent
        self.log_interval = log_interval
        self.intrinsics_interval = intrinsics_interval
        self.localhost_port = localhost_port  # Optional local port
        self.extra_send_locations = extra_send_locations or []

        # Connection state
        self.remote_ip = None
        self.remote_port = None
        self.socket = None
        self._connected = False
        self._socket_lock = threading.Lock()

        # Frame tracking
        self.rgb_frame_id = 0
        self.pointcloud_frame_id = 0

        # Performance tracking
        self.rgb_frame_count = 0
        self.pointcloud_frame_count = 0
        self.rgb_encode_times = []
        self.pointcloud_encode_times = []
        self.last_log_time = time.time()
        self.last_intrinsics_time = 0
        self._stats_lock = threading.Lock()

        # Camera intrinsics storage
        self.camera_intrinsics = None

        # Protocol constants
        self.MAGIC = 0xDEADBEEF
        self.MAGIC_INTRINSICS = 0xCAFEBABE
        self.FRAME_TYPE_RGB = 0
        self.FRAME_TYPE_DEPTH = 1  # Keep for compatibility
        self.FRAME_TYPE_POINTCLOUD = 2  # New frame type
        self.HEADER_SIZE = 13
        self.POINTCLOUD_HEADER_SIZE = 17  # Header + point_count(4)

    def set_camera_intrinsics(self, rgb_intrinsics, depth_intrinsics, extrinsics):
        """Store camera intrinsics for transmission"""
        self.camera_intrinsics = {
            'rgb': rgb_intrinsics,
            'depth': depth_intrinsics,
            'extrinsics': extrinsics
        }

    def connect(self, remote_ip, remote_port):
        """Connect to remote UDP endpoint"""
        with self._socket_lock:
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass

            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.remote_ip = remote_ip
                self.remote_port = remote_port
                self._connected = True

                if not self.silent:
                    destinations = f"{remote_ip}:{remote_port}"
                    if self.localhost_port:
                        destinations += f" + localhost:{self.localhost_port}"
                    print(f"UDP connector ready for {destinations}")
                return True

            except Exception as e:
                if not self.silent:
                    print(f"Failed to create UDP socket: {e}")
                self._connected = False
                return False

    def disconnect(self):
        """Disconnect from remote"""
        with self._socket_lock:
            self._connected = False
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass
                self.socket = None

    def reconnect(self, remote_ip, remote_port):
        """Reconnect to different endpoint"""
        self.disconnect()
        return self.connect(remote_ip, remote_port)

    def is_connected(self):
        """Check if connected to remote"""
        return self._connected and self.socket is not None

    def send_rgb_frame(self, rgb_array):
        """Encode and send RGB frame"""
        if not self.is_connected():
            return

        self._maybe_send_intrinsics()

        try:
            bgr_array = cv2.cvtColor(rgb_array, cv2.COLOR_RGB2BGR)

            encode_start = time.time()
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), self.jpeg_quality]
            success, jpeg_data = cv2.imencode('.jpg', bgr_array, encode_param)
            encode_time = (time.time() - encode_start) * 1000

            if not success:
                if not self.silent:
                    print("Failed to encode RGB frame to JPEG")
                return

            with self._stats_lock:
                self.rgb_frame_count += 1
                self.rgb_encode_times.append(encode_time)
                self._maybe_log_stats()

            self._send_fragmented_frame(jpeg_data.tobytes(), self.FRAME_TYPE_RGB, self.rgb_frame_id)
            self.rgb_frame_id = (self.rgb_frame_id + 1) & 0xFFFFFFFF

        except Exception as e:
            if not self.silent:
                print(f"Error processing RGB frame: {e}")

    def send_pointcloud_frame(self, pointcloud_data):
        """Send point cloud data with quantized positions (9 bytes per point)
        Format: int16 x,y,z (mm) + uint8 r,g,b = 6 + 3 = 9 bytes per point"""
        if not self.is_connected():
            return

        try:
            encode_start = time.time()

            point_count = len(pointcloud_data)
            if point_count == 0:
                return

            # Convert positions from meters to millimeters and quantize to int16
            positions_m = pointcloud_data[:, :3].astype(np.float32)  # x, y, z in meters
            positions_mm = (positions_m * 1000).astype(np.int16)  # Convert to mm and quantize to int16
            colors = pointcloud_data[:, 3:6].astype(np.uint8)  # r, g, b

            # Pack as big-endian int16 for positions
            positions_be = positions_mm.astype('>i2')  # big-endian int16

            # Create binary data more efficiently - now 9 bytes per point
            binary_data = bytearray()
            positions_bytes = positions_be.tobytes()
            colors_bytes = colors.tobytes()

            # Interleave position (6 bytes) and color (3 bytes) data
            for i in range(point_count):
                pos_offset = i * 6  # 3 int16 * 2 bytes = 6 bytes per position
                color_offset = i * 3  # 3 bytes per color
                binary_data.extend(positions_bytes[pos_offset:pos_offset + 6])
                binary_data.extend(colors_bytes[color_offset:color_offset + 3])

            encode_time = (time.time() - encode_start) * 1000

            with self._stats_lock:
                self.pointcloud_frame_count += 1
                self.pointcloud_encode_times.append(encode_time)

            self._send_fragmented_pointcloud(bytes(binary_data), self.pointcloud_frame_id, point_count)
            self.pointcloud_frame_id = (self.pointcloud_frame_id + 1) & 0xFFFFFFFF

            if not self.silent:
                pass
                #print(f"Sent quantized point cloud with {point_count} points ({len(binary_data)} bytes)")

        except Exception as e:
            if not self.silent:
                print(f"Error processing point cloud frame: {e}")

    def send_depth_frame(self, depth_array):
        """Send 16-bit depth frame as 3-channel PNG"""
        if not self.is_connected():
            return

        try:
            depth_uint16 = depth_array.astype(np.uint16)

            high_bytes = (depth_uint16 >> 8).astype(np.uint8)
            low_bytes = (depth_uint16 & 0xFF).astype(np.uint8)
            zero_bytes = np.zeros_like(high_bytes)

            three_channel_image = np.stack([zero_bytes, low_bytes, high_bytes], axis=2)

            encode_start = time.time()
            encode_param = [int(cv2.IMWRITE_PNG_COMPRESSION), 1]
            success, png_data = cv2.imencode('.png', three_channel_image, encode_param)
            encode_time = (time.time() - encode_start) * 1000

            if not success:
                if not self.silent:
                    print("Failed to encode depth frame to PNG")
                return

            self._send_fragmented_frame(png_data.tobytes(), self.FRAME_TYPE_DEPTH, self.pointcloud_frame_id)
            self.pointcloud_frame_id = (self.pointcloud_frame_id + 1) & 0xFFFFFFFF

        except Exception as e:
            if not self.silent:
                print(f"Error processing depth frame: {e}")

    def _send_packet_to_destinations(self, packet):
        """Send packet to both remote destination and localhost (if configured)"""
        try:
            with self._socket_lock:
                if self.socket and self._connected:
                    # Send to remote destination
                    self.socket.sendto(packet, (self.remote_ip, self.remote_port))

                    # Also send to localhost if configured
                    if self.localhost_port:
                        self.socket.sendto(packet, ("127.0.0.1", self.localhost_port))
                    for ip, port in self.extra_send_locations:
                        self.socket.sendto(packet, (ip, port))
        except Exception as e:
            if not self.silent:
                print(f"UDP send failed: {e}")

    def _send_fragmented_pointcloud(self, pointcloud_data, frame_id, point_count):
        """Fragment point cloud data and send via UDP"""
        if not self.is_connected():
            return

        payload_size = self.chunk_size - self.POINTCLOUD_HEADER_SIZE
        total_fragments = (len(pointcloud_data) + payload_size - 1) // payload_size

        for frag_seq in range(total_fragments):
            start_idx = frag_seq * payload_size
            end_idx = min(start_idx + payload_size, len(pointcloud_data))
            payload = pointcloud_data[start_idx:end_idx]

            header = struct.pack('>I B I H H I',
                                 self.MAGIC,
                                 self.FRAME_TYPE_POINTCLOUD,
                                 frame_id,
                                 frag_seq,
                                 total_fragments,
                                 point_count)

            packet = header + payload
            self._send_packet_to_destinations(packet)

    def _maybe_send_intrinsics(self):
        """Send intrinsics periodically"""
        current_time = time.time()
        if (self.camera_intrinsics is not None and
                current_time - self.last_intrinsics_time >= self.intrinsics_interval):
            self._send_intrinsics()
            self.last_intrinsics_time = current_time

    def _send_intrinsics(self):
        """Send camera intrinsics packet"""
        if not self.is_connected() or self.camera_intrinsics is None:
            return

        try:
            rgb_intr = self.camera_intrinsics['rgb']
            depth_intr = self.camera_intrinsics['depth']
            extr = self.camera_intrinsics['extrinsics']

            rgb_data = struct.pack('>6f',
                                   rgb_intr.fx, rgb_intr.fy, rgb_intr.ppx, rgb_intr.ppy,
                                   float(rgb_intr.width), float(rgb_intr.height))

            depth_data = struct.pack('>6f',
                                     depth_intr.fx, depth_intr.fy, depth_intr.ppx, depth_intr.ppy,
                                     float(depth_intr.width), float(depth_intr.height))

            rotation_flat = [extr.rotation[i] for i in range(9)]
            translation = extr.translation
            extr_data = struct.pack('>12f', *rotation_flat, *translation)

            header = struct.pack('>I', self.MAGIC_INTRINSICS)
            intrinsics_packet = header + rgb_data + depth_data + extr_data

            self._send_packet_to_destinations(intrinsics_packet)

        except Exception as e:
            if not self.silent:
                print(f"Failed to send intrinsics: {e}")

    def _send_fragmented_frame(self, image_data, frame_type, frame_id):
        """Fragment image data and send via UDP"""
        if not self.is_connected():
            return

        payload_size = self.chunk_size - self.HEADER_SIZE
        total_fragments = (len(image_data) + payload_size - 1) // payload_size

        for frag_seq in range(total_fragments):
            start_idx = frag_seq * payload_size
            end_idx = min(start_idx + payload_size, len(image_data))
            payload = image_data[start_idx:end_idx]

            header = struct.pack('>I B I H H',
                                 self.MAGIC,
                                 frame_type,
                                 frame_id,
                                 frag_seq,
                                 total_fragments)

            packet = header + payload
            self._send_packet_to_destinations(packet)

    def _maybe_log_stats(self):
        """Log performance stats periodically"""
        current_time = time.time()
        if current_time - self.last_log_time >= self.log_interval:
            self._log_performance_stats()
            self.last_log_time = current_time

    def _log_performance_stats(self):
        """Log current performance statistics"""
        if self.silent:
            return

        elapsed = time.time() - self.last_log_time if self.last_log_time > 0 else self.log_interval
        rgb_fps = self.rgb_frame_count / elapsed if elapsed > 0 else 0
        pointcloud_fps = self.pointcloud_frame_count / elapsed if elapsed > 0 else 0

        rgb_avg_encode = sum(self.rgb_encode_times) / len(self.rgb_encode_times) if self.rgb_encode_times else 0
        pointcloud_avg_encode = sum(self.pointcloud_encode_times) / len(self.pointcloud_encode_times) if self.pointcloud_encode_times else 0

        print(f"Frame rates: RGB {rgb_fps:.1f}fps, PointCloud {pointcloud_fps:.1f}fps | "
              f"Encode times: RGB {rgb_avg_encode:.1f}ms (JPEG), PointCloud {pointcloud_avg_encode:.1f}ms")

        self.rgb_frame_count = 0
        self.pointcloud_frame_count = 0
        self.rgb_encode_times.clear()
        self.pointcloud_encode_times.clear()
