import threading
import time
from LocalNode.node_client import NodeClient
from LocalNode.remote_connector import JSONRemoteConnector, RTCRemoteConnector


class LocalCommsNode:
    def __init__(self, node_name, service_port: str, node_registry_port=10081, config_schema=None, action_schema=None, verbose=False, silent=False, upkeep_interval=1.0, disconnect_on_empty=True):
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

        self.node_client = NodeClient(node_name=node_name, config_schema=config_schema, command_schema=action_schema, registry_url=f'http://localhost:{node_registry_port}', verbose_actions=verbose, silent=silent)
        self.node_client.start()
        self.remote_client = JSONRemoteConnector()

        self._upkeep_thread_obj = threading.Thread(target=self._upkeep_thread, daemon=True)
        self._upkeep_thread_obj.start()

    def queue_data(self, data):
        self.remote_client.send_data(data)

    def get_data(self) -> list:
        return self.remote_client.get_received_data()

    def get_actions(self):
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
        self.node_client.update_schemas(config_schema=config_schema, command_schema=action_schema)

    def is_connected(self) -> (bool, bool):
        """Returns bools (connected to ui, connected to quest)"""
        return self.node_client.is_connected(), self.remote_client.is_connected()

    def _upkeep_thread(self):
        while self._upkeep_running:
            try:
                remote_discovery = self.node_client.get_remote_discovery()

                if not remote_discovery:
                    if self.disconnect_on_empty and self.remote_client.is_connected():
                        self.remote_client.disconnect()
                        self.current_remote_ip = None
                        self.current_remote_port = None
                else:
                    remote_ip = remote_discovery.get('remote_ip')
                    remote_ports = remote_discovery.get('remote_ports')

                    if remote_ports:
                        echo_port = remote_ports.get(self.service_port)

                        if echo_port and (remote_ip != self.current_remote_ip or echo_port != self.current_remote_port):
                            self.remote_client.reconnect(remote_ip, echo_port)
                            self.current_remote_ip = remote_ip
                            self.current_remote_port = echo_port

                time.sleep(self.upkeep_interval)
            except Exception as e:
                if not self.silent:
                    print(f"Upkeep thread error: {e}")
                time.sleep(self.upkeep_interval)
