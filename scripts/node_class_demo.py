"""
Simplified Node Client for Orchestrator System

Provides an easy-to-use wrapper for connecting nodes to the orchestrator.
Handles heartbeats, action queuing, and schema management automatically.

Usage:
    client = NodeClient("my_node", config_schema, command_schema)
    client.start()

    while running:
        actions = client.get_pending_actions()
        config = client.get_config_changes()
        # Process actions and config...
        time.sleep(0.1)

    client.stop()
"""

import requests
import threading
import time
import queue
from typing import Dict, List, Optional, Any, Tuple


class NodeClient:
    def __init__(self, node_name: str, config_schema: List = None, command_schema: Dict = None, registry_url: str = "http://localhost:10081",
                 heartbeat_interval: float = 0.3, reconnect_interval: float = 2.0, verbose_actions: bool = False):
        """
        Initialize a managed node client.

        Args:
            node_name: Unique name for this node
            config_schema: List of config widget definitions
            command_schema: Dict of command definitions
            registry_url: Orchestrator server URL
            heartbeat_interval: Seconds between heartbeats
            reconnect_interval: Seconds between reconnection attempts
            verbose_actions: Print received actions to console
        """
        self.node_name = node_name
        self.node_id: Optional[str] = None
        self.registry_url = registry_url
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_interval = reconnect_interval
        self.verbose_actions = verbose_actions

        # Schemas
        self.config_schema = config_schema or []
        self.command_schema = command_schema or {}

        # Threading
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.running = False

        # Action and config caching
        self.action_queue = queue.Queue()
        self.config_changes = queue.Queue()
        self.remote_discovery_cache = {}
        self._cache_lock = threading.Lock()

        # Connection state
        self.connected = False
        self.last_heartbeat_success = False
        self.connection_lost = False
        self.reconnection_mode = False

    def update_config_schema(self, schema: List):
        """Update the configuration schema and send to server."""
        self.config_schema = schema
        if self.connected:
            self._send_schema_update(config_schema=schema)
            print(f"Updated config schema for '{self.node_name}'")

    def update_command_schema(self, schema: Dict):
        """Update the command schema and send to server."""
        self.command_schema = schema
        if self.connected:
            self._send_schema_update(command_schema=schema)
            print(f"Updated command schema for '{self.node_name}'")

    def update_schemas(self, config_schema: List = None, command_schema: Dict = None):
        """Update both schemas at once."""
        if config_schema is not None:
            self.config_schema = config_schema
        if command_schema is not None:
            self.command_schema = command_schema

        if self.connected:
            self._send_schema_update(
                config_schema=config_schema,
                command_schema=command_schema
            )
            print(f"Updated schemas for '{self.node_name}'")

    def start(self) -> bool:
        """Connect to orchestrator and start heartbeat. Always returns True."""
        if self.running:
            return True

        self.running = True

        # Try initial connection
        success = self._connect(silent=True)
        if success:
            print(f"NodeClient '{self.node_name}' started and connected")
        else:
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
        if self.connected and self.node_id:
            self._disconnect()

        print(f"NodeClient '{self.node_name}' stopped")

    def get_pending_actions(self) -> List[Tuple[str, List]]:
        """Get all pending actions from the queue. Returns list of (action_name, params) tuples."""
        actions = []
        while not self.action_queue.empty():
            try:
                actions.append(self.action_queue.get_nowait())
            except queue.Empty:
                break
        return actions

    def get_config_changes(self) -> List[Any]:
        """Get any configuration changes. Returns list of config updates."""
        changes = []
        while not self.config_changes.empty():
            try:
                changes.append(self.config_changes.get_nowait())
            except queue.Empty:
                break
        return changes

    def get_remote_discovery(self) -> Dict:
        """Get cached remote discovery information from server."""
        with self._cache_lock:
            return self.remote_discovery_cache.copy()

    def send_payload(self, payload: Dict):
        """Send custom payload data with next heartbeat."""
        # This could be enhanced to queue payloads
        pass

    def is_connected(self) -> bool:
        """Check if currently connected to orchestrator."""
        return self.connected and self.last_heartbeat_success

    def _connect(self, silent: bool = False) -> bool:
        """Internal connection logic."""
        payload = {
            "node_name": self.node_name,
            "config_schema": self.config_schema,
            "command_schema": self.command_schema,
            "payload": {"status": "connecting", "timestamp": time.time()}
        }

        try:
            response = requests.post(f"{self.registry_url}/connect", json=payload, timeout=0.1)
            if response.status_code == 200:
                data = response.json()
                if data.get("message_type") == "success":
                    self.node_id = data["node_id"]
                    self.connected = True

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
                self.connected = False
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
            "timestamp": time.time(),
            "payload": {"status": "running"}
        }

        try:
            response = requests.post(f"{self.registry_url}/data", json=payload, timeout=0.1)
            if response.status_code == 200:
                data = response.json()

                # Process actions from orchestrator
                if 'actions' in data and data['actions']:
                    for action in data['actions']:
                        if isinstance(action, list) and len(action) >= 1:
                            action_name = action[0]
                            action_params = action[1] if len(action) > 1 else []

                            if self.verbose_actions:
                                print(f"Received action: {action_name} with params: {action_params}")

                            self.action_queue.put((action_name, action_params))

                # Cache remote discovery updates
                if 'remote_ports' in data:
                    with self._cache_lock:
                        self.remote_discovery_cache.update(data['remote_ports'])

                # Handle config updates (if implemented by server)
                if 'config_update' in data and data['config_update']:
                    for config_item in data['config_update']:
                        self.config_changes.put(config_item)

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
            if not self.reconnection_mode:
                # Normal heartbeat mode
                success = self._send_heartbeat()

                if success:
                    consecutive_failures = 0
                    if not self.last_heartbeat_success:
                        # Just reconnected
                        print(f"NodeClient '{self.node_name}' reconnected to server")
                        self.connection_lost = False
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
        if not self.connection_lost:
            print(f"NodeClient '{self.node_name}' lost connection to server")
            self.connection_lost = True

        self.reconnection_mode = True
        self.connected = False
        self.last_heartbeat_success = False
        self.node_id = None

    def _exit_reconnection_mode(self):
        """Exit reconnection mode when server is reachable."""
        self.reconnection_mode = False
        print(f"NodeClient '{self.node_name}' successfully reconnected")

    def _attempt_reconnection(self) -> bool:
        """Attempt to reconnect to the server."""
        return self._connect(silent=True)


def main():
    """Demo showing how to use NodeClient."""

    # Define your node's configuration interface
    config_schema = [
        ("text", "My Custom Node Configuration", {"color": (100, 255, 100)}, None),
        ("separator", "", {}, None),
        ("bool", "Enable Processing", {}, True),
        ("int", "Processing Rate", {"min": 1, "max": 100}, 10),
        ("float", "Scale Factor", {"min": 0.1, "max": 5.0}, 1.0),
        ("string", "Data Source", {"hint": "Enter file path or URL"}, ""),
        ("dropdown", "Output Format", {"items": ["JSON", "XML", "CSV"]}, "JSON"),
    ]

    # Define available actions/commands
    command_schema = {
        "start_processing": [{"default_open": True}, [
            ("int", "Batch Size", {"min": 1, "max": 1000}, 100),
            ("bool", "Verbose Output", {}, False),
            "Start Processing"
        ]],
        "stop_processing": [{"default_open": False}, "Stop All Processing"],
        "export_data": [{"default_open": False}, [
            ("dropdown", "Format", {"items": ["JSON", "CSV", "Parquet"]}, "JSON"),
            ("string", "Filename", {}, "export_data"),
            "Export Current Data"
        ]],
        "calibrate": [{"default_open": False}, [
            ("int", "Iterations", {"min": 1, "max": 20}, 5),
            ("float", "Tolerance", {"min": 0.001, "max": 1.0}, 0.1),
            "Run Calibration"
        ]]
    }

    # Create and start the node client with verbose action logging for demo
    client = NodeClient("demo_node", config_schema, command_schema, verbose_actions=True)
    client.start()

    print("Node client running. Use the orchestrator GUI to interact with it.")
    print("Available actions: start_processing, stop_processing, export_data, calibrate")
    print("Press Ctrl+C to exit\n")

    try:
        # Main application loop
        loop_count = 0
        schema_updated = False

        while True:
            loop_count += 1

            # Demo: Change the schema after 50 loops (~5 seconds)
            if loop_count == 50 and not schema_updated:
                print("Demo: Adding new configuration option...")
                new_config_schema = config_schema + [
                    ("separator", "Advanced Options", {}, None),
                    ("bool", "Enable Debugging", {}, False),
                    ("int", "Debug Level", {"min": 1, "max": 5}, 1),
                ]

                new_command_schema = command_schema.copy()
                new_command_schema["debug_dump"] = [{"default_open": False}, "Dump Debug Info"]

                client.update_schemas(new_config_schema, new_command_schema)
                schema_updated = True

            # Check for new actions from orchestrator
            actions = client.get_pending_actions()
            for action_name, params in actions:
                # See the config section for how params are sent. Action name is that provided in the schema
                
                # Handle your actions here (verbose logging handled by client)
                if action_name == "start_processing":
                    batch_size = params[0] if params else 100
                    verbose = params[1] if len(params) > 1 else False
                    print(f"  Starting processing with batch_size={batch_size}, verbose={verbose}")

                elif action_name == "stop_processing":
                    print("  Stopping all processing")

                elif action_name == "export_data":
                    format_type = params[0] if params else "JSON"
                    filename = params[1] if len(params) > 1 else "export_data"
                    print(f"  Exporting data as {format_type} to {filename}")

                elif action_name == "calibrate":
                    iterations = params[0] if params else 5
                    tolerance = params[1] if len(params) > 1 else 0.1
                    print(f"  Running calibration: {iterations} iterations, tolerance={tolerance}")

                elif action_name == "debug_dump":
                    print("  Dumping debug information...")

                    # Print remote discovery data
                    remote_info = client.get_remote_discovery()
                    if remote_info:
                        print(f"  Remote Discovery Data: {remote_info}")
                    else:
                        print("  Remote Discovery Data: No remote services discovered")

                    # Print connection status
                    print(f"  Connection Status: {'Connected' if client.is_connected() else 'Disconnected'}")
                    print(f"  Node ID: {client.node_id}")
                    print(f"  Pending Actions in Queue: {client.action_queue.qsize()}")
                    print(f"  Config Changes in Queue: {client.config_changes.qsize()}")

            # Check for configuration changes
            config_changes = client.get_config_changes()
            if config_changes:
                # Example: [True, 10, 1.0, '', 'JSON', False, 1]. Values are the currently set values, ordered by that provided in the config schema. Headers, text, seperators etc are filtered out
                print(f"Configuration updated: {config_changes}")

            # Check connection status
            if not client.is_connected():
                # Client will handle reconnection automatically
                pass

            # Simulate some work
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        client.stop()


if __name__ == "__main__":
    main()
