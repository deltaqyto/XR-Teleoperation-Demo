import threading
import time
from LocalNode.node_client import NodeClient
from RemoteConnector.remote_connector import JSONRemoteConnector


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


if __name__ == "__main__":
    def perform_echo_ping(comms_node, pings, delay):
        """Perform ping test using the provided LocalCommsNode."""
        for i in range(ping_count):
            ping_data = {
                "ping": i + 1,
                "timestamp": time.time(),
                "message": f"Test ping {i + 1}"
            }

            comms_node.queue_data(ping_data)
            print(f"Sent ping {i + 1}")

            time.sleep(0.1)
            responses = comms_node.get_data()

            if responses:
                print(f"Received echo response: {responses}")
            else:
                print(f"No response for ping {i + 1}")

            if i < pings - 1:
                time.sleep(delay)

    config_schema = [
        ("int", "Ping Count", {"min": 1, "max": 20}, 5),
        ("float", "Ping Delay", {"min": 0.1, "max": 2.0}, 0.5),
    ]

    command_schema = {
        "Start Pings": [{"default_open": True}, "Execute Ping Test"]
    }

    node = LocalCommsNode("Ping Node", "echo", config_schema=config_schema, action_schema=command_schema)

    try:
        while True:
            actions = node.get_actions()
            for action_name, params in actions:
                if action_name == "Start Pings":
                    changed, config = node.get_latest_config()
                    ping_count, ping_delay = config
                    perform_echo_ping(node, ping_count, ping_delay)

            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
