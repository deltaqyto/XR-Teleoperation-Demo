import threading
import time
from LocalNode.node_client import NodeClient
from LocalNode.udp_remote_connector import UDPRemoteConnector


class UDPVideoCommsNode:
    def __init__(self, node_name, service_port, node_registry_port=10081,
                 config_schema=None, action_schema=None, verbose=False,
                 silent=False, upkeep_interval=1.0, disconnect_on_empty=True,
                 frame_slots=16, chunk_size=1200, jpeg_quality=85, log_interval=5.0,
                 intrinsics_interval=2.0, localhost_port=None):

        self.node_registry_port = node_registry_port
        self.service_port = service_port
        config_schema = config_schema or []
        action_schema = action_schema or []

        self.config_cache = []
        self.upkeep_interval = upkeep_interval
        self.disconnect_on_empty = disconnect_on_empty

        self.current_remote_ip = None
        self.current_remote_port = None
        self._upkeep_running = True

        self.verbose = verbose
        self.silent = silent

        # Initialize node client for discovery and communication
        self.node_client = NodeClient(
            node_name=node_name,
            config_schema=config_schema,
            command_schema=action_schema,
            registry_url=f'http://localhost:{node_registry_port}',
            verbose_actions=verbose,
            silent=silent
        )
        self.node_client.start()

        # Initialize UDP video connector
        self.udp_connector = UDPRemoteConnector(
            chunk_size=chunk_size,
            jpeg_quality=jpeg_quality,
            silent=silent,
            log_interval=log_interval,
            intrinsics_interval=intrinsics_interval,
            localhost_port=localhost_port
        )

        # Start upkeep thread (critical for heartbeat)
        self._upkeep_thread_obj = threading.Thread(target=self._upkeep_thread, daemon=True)
        self._upkeep_thread_obj.start()

    def set_camera_intrinsics(self, rgb_intrinsics, depth_intrinsics, extrinsics):
        """Set camera intrinsics for transmission"""
        self.udp_connector.set_camera_intrinsics(rgb_intrinsics, depth_intrinsics, extrinsics)

    def send_rgb_frame(self, rgb_array):
        """Send RGB frame via UDP stream"""
        self.udp_connector.send_rgb_frame(rgb_array)

    def send_depth_frame(self, depth_array):
        """Send 16-bit depth frame via UDP stream"""
        self.udp_connector.send_depth_frame(depth_array)

    def send_pointcloud_frame(self, pointcloud_data):
        """Send point cloud data via UDP stream"""
        self.udp_connector.send_pointcloud_frame(pointcloud_data)

    def get_actions(self):
        """Pass through to node client"""
        return self.node_client.get_pending_actions()

    def get_latest_config(self):
        """Returns whether config changed, and the latest valid config"""
        new_config = self.node_client.get_config_changes()
        if len(new_config) != len(self.config_cache):
            self.config_cache = new_config
            return True, new_config

        has_diff = any([True for o, n in zip(self.config_cache, new_config) if o != n])

        self.config_cache = new_config
        return has_diff, new_config

    def set_new_schemas(self, config_schema=None, action_schema=None):
        """Pass through to node client"""
        self.node_client.update_schemas(config_schema=config_schema, command_schema=action_schema)

    def is_connected(self):
        """Returns bools (connected to ui, connected to quest)"""
        return self.node_client.is_connected(), self.udp_connector.is_connected()

    def _upkeep_thread(self):
        """Main discovery and connection management loop"""
        while self._upkeep_running:
            try:
                remote_discovery = self.node_client.get_remote_discovery()

                if not remote_discovery:
                    if self.disconnect_on_empty and self.udp_connector.is_connected():
                        self.udp_connector.disconnect()
                        self.current_remote_ip = None
                        self.current_remote_port = None
                else:
                    remote_ip = remote_discovery.get('remote_ip')
                    remote_ports = remote_discovery.get('remote_ports')

                    if remote_ports:
                        quest_port = remote_ports.get(self.service_port)

                        if quest_port and (remote_ip != self.current_remote_ip or quest_port != self.current_remote_port):
                            if self.udp_connector.reconnect(remote_ip, quest_port):
                                self.current_remote_ip = remote_ip
                                self.current_remote_port = quest_port
                                if not self.silent:
                                    print(f"Connected to Quest at {remote_ip}:{quest_port}")
                            else:
                                if not self.silent:
                                    print(f"Failed to connect to Quest at {remote_ip}:{quest_port}")

                time.sleep(self.upkeep_interval)

            except Exception as e:
                if not self.silent:
                    print(f"Upkeep thread error: {e}")
                time.sleep(self.upkeep_interval)
