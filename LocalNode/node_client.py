import threading
import time
import requests


class NodeClient:
    def __init__(self, node_name: str, config_schema: list = None, command_schema: dict = None, registry_url: str = "http://localhost:10081",
                 heartbeat_interval: float = 0.3, reconnect_interval: float = 2.0, verbose_actions: bool = False, silent: bool = False):

        self.node_name = node_name
        self.node_id = None
        self.registry_url = registry_url
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_interval = reconnect_interval
        self.verbose_actions = verbose_actions
        self.silent = silent

        # Schemas
        self.config_schema = config_schema or []
        self.command_schema = command_schema or {}

        # Threading
        self.heartbeat_thread = None
        self.running = False

        # Action and config caching
        self.action_list = []
        self.config_changes_list = []
        self.remote_discovery_cache = {}
        self._cache_lock = threading.Lock()

        # Connection state
        self.connection_state = "disconnected"  # "connected", "disconnected", "reconnecting"
        self.last_heartbeat_success = False

    def update_schemas(self, config_schema: list = None, command_schema: dict = None):
        """Update configuration and/or command schemas and send to server."""
        if config_schema is not None:
            self.config_schema = config_schema
        if command_schema is not None:
            self.command_schema = command_schema

        if self.connection_state == "connected":
            self._send_schema_update(
                config_schema=config_schema,
                command_schema=command_schema
            )
            if not self.silent:
                print(f"Updated schemas for '{self.node_name}'")

    def start(self) -> bool:
        """Connect to orchestrator and start heartbeat. Always returns True."""
        if self.running:
            return True

        self.running = True

        # Try initial connection
        success = self._connect(silent=True)
        if success:
            if not self.silent:
                print(f"NodeClient '{self.node_name}' started and connected")
        else:
            if not self.silent:
                print(f"NodeClient '{self.node_name}' started, server unavailable - will reconnect automatically")
            self._enter_reconnection_mode()

        # Always start heartbeat thread regardless of initial connection
        self._start_heartbeat()
        return True

    def stop(self):
        """Cleanly disconnect and stop heartbeat."""
        if not self.running:
            return

        self.running = False

        # Stop heartbeat thread
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=0.2)

        # Disconnect from server
        if self.connection_state == "connected" and self.node_id:
            self._disconnect()

        if not self.silent:
            print(f"NodeClient '{self.node_name}' stopped")

    def get_pending_actions(self):
        """Get all pending actions from the list. Returns list of (action_name, params) tuples."""
        with self._cache_lock:
            actions = self.action_list.copy()
            self.action_list.clear()
        return actions

    def get_config_changes(self):
        """Get any configuration changes. Returns list of config updates."""
        with self._cache_lock:
            changes = self.config_changes_list.copy()
        return changes

    def get_remote_discovery(self):
        """Get cached remote discovery information from server."""
        with self._cache_lock:
            return self.remote_discovery_cache.copy()

    def is_connected(self) -> bool:
        """Check if currently connected to orchestrator."""
        return self.connection_state == "connected" and self.last_heartbeat_success

    def _connect(self, silent: bool = False) -> bool:
        """Internal connection logic."""
        payload = {
            "node_name": self.node_name,
            "config_schema": self.config_schema,
            "command_schema": self.command_schema
        }

        try:
            response = requests.post(f"{self.registry_url}/connect", json=payload, timeout=0.1)
            if response.status_code == 200:
                data = response.json()
                if data.get("message_type") == "success":
                    self.node_id = data["node_id"]
                    self.connection_state = "connected"

                    # Cache remote discovery if present
                    if 'remote_ports' in data:
                        with self._cache_lock:
                            self.remote_discovery_cache.update(data['remote_ports'])

                    return True
        except requests.exceptions.RequestException as e:
            if not silent:
                pass  # Don't print connection errors during initial startup

        return False

    def _disconnect(self) -> bool:
        """Internal disconnection logic."""
        if not self.node_id:
            return False

        try:
            payload = {"node_id": self.node_id}
            response = requests.post(f"{self.registry_url}/disconnect", json=payload, timeout=0.1)
            if response.status_code == 200:
                self.connection_state = "disconnected"
                return True
        except requests.exceptions.RequestException:
            pass

        return False

    def _send_heartbeat(self) -> bool:
        """Send heartbeat and process server response."""
        if not self.node_id:
            return False

        payload = {
            "node_id": self.node_id,
            "node_name": self.node_name,
            "timestamp": time.time()
        }

        try:
            response = requests.post(f"{self.registry_url}/data", json=payload, timeout=0.1)
            if response.status_code == 200:
                data = response.json()

                # Process actions from orchestrator
                if 'actions' in data and data['actions']:
                    with self._cache_lock:
                        for action in data['actions']:
                            if isinstance(action, list) and len(action) >= 1:
                                action_name = action[0]
                                action_params = action[1] if len(action) > 1 else []

                                if self.verbose_actions and not self.silent:
                                    print(f"Received action: {action_name} with params: {action_params}")

                                self.action_list.append((action_name, action_params))

                # Cache remote discovery updates
                if 'remote_ports' in data:
                    with self._cache_lock:
                        self.remote_discovery_cache.update(data['remote_ports'])

                # Handle config updates
                if 'config_update' in data and data['config_update']:
                    with self._cache_lock:
                        self.config_changes_list = []
                        for config_item in data['config_update']:
                            self.config_changes_list.append(config_item)

                self.last_heartbeat_success = True
                return True

        except requests.exceptions.RequestException:
            self.last_heartbeat_success = False

        return False

    def _send_schema_update(self, config_schema=None, command_schema=None):
        """Send schema update to server."""
        if not self.node_id:
            return

        payload = {
            "node_id": self.node_id,
            "node_name": self.node_name,
            "timestamp": time.time()
        }

        if config_schema is not None:
            payload["config_schema"] = config_schema
        if command_schema is not None:
            payload["command_schema"] = command_schema

        try:
            requests.post(f"{self.registry_url}/data", json=payload, timeout=0.1)
        except requests.exceptions.RequestException:
            pass

    def _start_heartbeat(self):
        """Start the heartbeat thread."""
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            return

        self.heartbeat_thread = threading.Thread(target=self._heartbeat_worker)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def _heartbeat_worker(self):
        """Heartbeat worker thread with reconnection handling."""
        consecutive_failures = 0
        max_failures = 3  # Switch to reconnection mode after 3 failed heartbeats

        while self.running:
            if self.connection_state != "reconnecting":
                # Normal heartbeat mode
                success = self._send_heartbeat()

                if success:
                    consecutive_failures = 0
                    if not self.last_heartbeat_success:
                        # Just reconnected
                        print(f"NodeClient '{self.node_name}' reconnected to server")
                else:
                    consecutive_failures += 1

                    if consecutive_failures >= max_failures:
                        # Server appears to be down
                        self._enter_reconnection_mode()

                time.sleep(self.heartbeat_interval)

            else:
                # Reconnection mode - try to reconnect
                if self._attempt_reconnection():
                    # Successfully reconnected
                    self._exit_reconnection_mode()
                    consecutive_failures = 0
                else:
                    # Still can't connect, wait longer
                    time.sleep(self.reconnect_interval)

    def _enter_reconnection_mode(self):
        """Switch to reconnection mode when server is unreachable."""
        if self.connection_state != "reconnecting":
            print(f"NodeClient '{self.node_name}' lost connection to server")

        self.connection_state = "reconnecting"
        self.last_heartbeat_success = False
        self.node_id = None

    def _exit_reconnection_mode(self):
        """Exit reconnection mode when server is reachable."""
        self.connection_state = "connected"
        print(f"NodeClient '{self.node_name}' successfully reconnected")

    def _attempt_reconnection(self) -> bool:
        """Attempt to reconnect to the server."""
        return self._connect(silent=True)
