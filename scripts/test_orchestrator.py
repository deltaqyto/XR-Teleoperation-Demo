"""
Orchestrator GUI Test Script

Creates a control node with actions to test node addition/removal in the orchestrator GUI.

Usage:
    1. Run: python main.py (in separate terminal)
    2. Run: python orchestrator_test.py
    3. Use the GUI actions to trigger different tests
"""

import requests
import threading
import time
import queue
from typing import Dict, Optional


class MockNode:
    def __init__(self, node_name: str, registry_url: str = "http://localhost:10081"):
        self.node_name = node_name
        self.node_id: Optional[str] = None
        self.registry_url = registry_url
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.running = False

        # Default schemas for test nodes
        self.config_schema = [
            ("text", f"Configuration for {node_name}", {"color": (100, 150, 255)}, None),
            ("separator", "", {}, None),
            ("bool", "Enable Feature", {}, True),
            ("int", "Update Rate", {"min": 1, "max": 100}, 10),
            ("float", "Scale Factor", {"min": 0.1, "max": 2.0}, 1.0),
            ("string", "Connection String", {"hint": "Enter connection details"}, ""),
            ("dropdown", "Mode", {"items": ["Auto", "Manual", "Custom"]}, "Auto"),
        ]

        self.command_schema = {
            "restart": [{"default_open": False}, "Restart Node"],
            "calibrate": [{"default_open": True}, [
                ("int", "Iterations", {"min": 1, "max": 10}, 5),
                ("bool", "Save Results", {}, True),
                "Start Calibration"
            ]]
        }

    def connect(self) -> bool:
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
                    print(f"Connected: {self.node_name} -> {self.node_id}")
                    return True
        except requests.exceptions.RequestException as e:
            print(f"Connection failed for {self.node_name}: {e}")

        return False

    def disconnect(self) -> bool:
        if not self.node_id:
            return False

        try:
            payload = {"node_id": self.node_id}
            response = requests.post(f"{self.registry_url}/disconnect", json=payload, timeout=0.1)
            if response.status_code == 200:
                print(f"Disconnected: {self.node_name}")
                self.stop_heartbeat()
                return True
        except requests.exceptions.RequestException as e:
            print(f"Disconnect failed for {self.node_name}: {e}")

        return False

    def send_heartbeat(self):
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
                    self._process_actions(data['actions'])

                return True
        except requests.exceptions.RequestException:
            pass

        return False

    def _process_actions(self, actions):
        """Override in subclasses to handle specific actions"""
        for action in actions:
            print(f"Node {self.node_name} received action: {action}")

    def start_heartbeat(self, interval: float = 0.3):
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            return

        self.running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_worker, args=(interval,))
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def stop_heartbeat(self):
        self.running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=0.1)

    def _heartbeat_worker(self, interval: float):
        while self.running:
            self.send_heartbeat()
            time.sleep(interval)


class TestControllerNode(MockNode):
    def __init__(self, test_controller):
        super().__init__("TEST_CONTROLLER")
        self.test_controller = test_controller
        self.action_queue = queue.Queue()
        self.action_worker_thread = None
        self.action_worker_running = False

        self.config_schema = [
            ("text", "Orchestrator Test Controller", {"color": (255, 100, 100)}, None),
            ("separator", "Node Management", {}, None),
            ("int", "Nodes to Create", {"min": 1, "max": 10}, 3),
            ("string", "Node Name Prefix", {}, "test_node"),
            ("separator", "Test Status", {}, None),
            ("text", "Active Test Nodes: 0", {"color": (100, 255, 100)}, None),
        ]

        self.command_schema = {
            "spawn_nodes": [{"default_open": True}, [
                ("int", "Number of Nodes", {"min": 1, "max": 10}, 3),
                ("string", "Name Prefix", {}, "test_node"),
                "Spawn Test Nodes"
            ]],
            "remove_all": [{"default_open": False}, "Remove All Test Nodes"],
            "stress_test": [{"default_open": False}, [
                ("int", "Rounds", {"min": 1, "max": 20}, 5),
                ("int", "Nodes per Round", {"min": 1, "max": 5}, 3),
                "Start Stress Test"
            ]],
            "timeout_test": [{"default_open": False}, [
                ("dropdown", "Target Node", {"items": ["No nodes"]}, ""),
                "Stop Node Heartbeat"
            ]],
            "schema_update": [{"default_open": False}, [
                ("dropdown", "Target Node", {"items": ["No nodes"]}, ""),
                ("radio", "Schema Type", {"items": ["Config", "Actions", "Both"]}, "Config"),
                "Update Node Schema"
            ]],
            "reconnect_test": [{"default_open": False}, [
                ("string", "Node Name", {}, "reconnect_test"),
                "Test Reconnection"
            ]]
        }

    def start_action_worker(self):
        if self.action_worker_thread and self.action_worker_thread.is_alive():
            return

        self.action_worker_running = True
        self.action_worker_thread = threading.Thread(target=self._action_worker)
        self.action_worker_thread.daemon = True
        self.action_worker_thread.start()

    def stop_action_worker(self):
        self.action_worker_running = False
        if self.action_worker_thread:
            self.action_worker_thread.join(timeout=0.1)

    def _action_worker(self):
        """Worker thread to process actions without blocking heartbeat"""
        while self.action_worker_running:
            try:
                action = self.action_queue.get(timeout=0.1)
                self._execute_action(action)
                self.action_queue.task_done()
            except queue.Empty:
                continue

    def _process_actions(self, actions):
        """Queue actions for processing in separate thread"""
        for action in actions:
            if not isinstance(action, list) or len(action) < 1:
                continue

            print(f"Queueing test: {action[0]}")
            self.action_queue.put(action)

    def _execute_action(self, action):
        """Execute action in worker thread"""
        action_name = action[0]
        action_params = action[1] if len(action) > 1 else []

        print(f"Executing test: {action_name} with params: {action_params}")

        if action_name == "spawn_nodes":
            count = action_params[0] if action_params else 3
            prefix = action_params[1] if len(action_params) > 1 else "test_node"
            self.test_controller.spawn_test_nodes(count, prefix)

        elif action_name == "remove_all":
            self.test_controller.remove_all_test_nodes()

        elif action_name == "stress_test":
            rounds = action_params[0] if action_params else 5
            nodes_per_round = action_params[1] if len(action_params) > 1 else 3
            self.test_controller.run_stress_test(rounds, nodes_per_round)

        elif action_name == "timeout_test":
            target_node = action_params[0] if action_params else ""
            if target_node and target_node != "No nodes":
                self.test_controller.stop_node_heartbeat(target_node)

        elif action_name == "schema_update":
            target_node = action_params[0] if action_params else ""
            schema_type = action_params[1] if len(action_params) > 1 else "Config"
            if target_node and target_node != "No nodes":
                self.test_controller.update_node_schema(target_node, schema_type)

        elif action_name == "reconnect_test":
            node_name = action_params[0] if action_params else "reconnect_test"
            self.test_controller.test_reconnection(node_name)


class TestController:
    def __init__(self):
        self.test_nodes: Dict[str, MockNode] = {}
        self.node_counter = 0
        self.control_node = TestControllerNode(self)
        self.nodes_lock = threading.Lock()

    def start_control_node(self):
        success = self.control_node.connect()
        if success:
            self.control_node.start_action_worker()
            self.control_node.start_heartbeat()
            print("Test controller started - use GUI actions to run tests")
        return success

    def spawn_test_nodes(self, count: int, prefix: str = "test_node"):
        """Spawn nodes in parallel using threads"""
        def create_node(node_name):
            node = MockNode(node_name)
            if node.connect():
                node.start_heartbeat()
                with self.nodes_lock:
                    self.test_nodes[node_name] = node
                print(f"Spawned: {node_name}")

        threads = []
        for i in range(count):
            node_name = f"{prefix}_{self.node_counter + i}"
            thread = threading.Thread(target=create_node, args=(node_name,))
            thread.daemon = True
            threads.append(thread)
            thread.start()

        # Wait for all connections to complete
        for thread in threads:
            thread.join()

        self.node_counter += count
        self.update_control_node_status()

    def remove_all_test_nodes(self):
        """Remove all nodes in parallel"""
        def disconnect_node(node_name, node):
            node.disconnect()

        with self.nodes_lock:
            nodes_to_remove = list(self.test_nodes.items())
            self.test_nodes.clear()

        threads = []
        for node_name, node in nodes_to_remove:
            thread = threading.Thread(target=disconnect_node, args=(node_name, node))
            thread.daemon = True
            threads.append(thread)
            thread.start()

        # Wait for all disconnections to complete
        for thread in threads:
            thread.join()

        print("Removed all test nodes")
        self.update_control_node_status()

    def run_stress_test(self, rounds: int, nodes_per_round: int):
        """Run stress test with parallel operations"""
        print(f"Starting stress test: {rounds} rounds, {nodes_per_round} nodes each")

        for round_num in range(rounds):
            created_nodes = []
            nodes_lock = threading.Lock()

            def create_and_store_node(j):
                node_name = f"stress_{round_num}_{j}"
                node = MockNode(node_name)
                if node.connect():
                    node.start_heartbeat()
                    with nodes_lock:
                        created_nodes.append(node)

            # Create nodes in parallel
            create_threads = []
            for j in range(nodes_per_round):
                thread = threading.Thread(target=create_and_store_node, args=(j,))
                thread.daemon = True
                create_threads.append(thread)
                thread.start()

            # Wait for all creations
            for thread in create_threads:
                thread.join()

            # Disconnect nodes in parallel
            disconnect_threads = []
            for node in created_nodes:
                thread = threading.Thread(target=node.disconnect)
                thread.daemon = True
                disconnect_threads.append(thread)
                thread.start()

            # Wait for all disconnections
            for thread in disconnect_threads:
                thread.join()

            print(f"Completed stress round {round_num + 1}")

    def stop_node_heartbeat(self, node_name: str):
        with self.nodes_lock:
            if node_name in self.test_nodes:
                self.test_nodes[node_name].stop_heartbeat()
                print(f"Stopped heartbeat for {node_name} - will timeout in ~1 second")

    def update_node_schema(self, node_name: str, schema_type: str):
        with self.nodes_lock:
            if node_name not in self.test_nodes:
                return
            node = self.test_nodes[node_name]

        new_config = [
            ("text", f"UPDATED: {node_name}", {"color": (255, 0, 0)}, None),
            ("bool", "New Feature", {}, True),
            ("int", "New Parameter", {"min": 0, "max": 100}, 50)
        ]

        new_commands = {
            "new_action": [{"default_open": True}, "New Action Added"]
        }

        payload = {
            "node_id": node.node_id,
            "node_name": node.node_name,
            "timestamp": time.time()
        }

        if schema_type in ["Config", "Both"]:
            payload["config_schema"] = new_config
        if schema_type in ["Actions", "Both"]:
            payload["command_schema"] = new_commands

        try:
            requests.post(f"{node.registry_url}/data", json=payload, timeout=0.1)
            print(f"Updated {schema_type} schema for {node_name}")
        except requests.exceptions.RequestException as e:
            print(f"Schema update failed: {e}")

    def test_reconnection(self, node_name: str):
        """Test reconnection without blocking"""
        def reconnect_worker():
            node = MockNode(node_name)
            if node.connect():
                node.start_heartbeat()
                print(f"Connected {node_name}")

                # Disconnect after 2 seconds
                time.sleep(2)
                node.disconnect()
                print(f"Disconnected {node_name}")

        thread = threading.Thread(target=reconnect_worker)
        thread.daemon = True
        thread.start()

    def update_control_node_status(self):
        with self.nodes_lock:
            node_count = len(self.test_nodes)
            node_options = list(self.test_nodes.keys()) if self.test_nodes else ["No nodes"]

        status_text = f"Active Test Nodes: {node_count}"

        # Update status in config schema
        self.control_node.config_schema[5] = ("text", status_text, {"color": (100, 255, 100)}, None)

        # Update dropdown options
        self.control_node.command_schema["timeout_test"][1][0] = (
            "dropdown", "Target Node", {"items": node_options}, ""
        )
        self.control_node.command_schema["schema_update"][1][0] = (
            "dropdown", "Target Node", {"items": node_options}, ""
        )

        # Send updated schemas to server
        if self.control_node.node_id:
            payload = {
                "node_id": self.control_node.node_id,
                "node_name": self.control_node.node_name,
                "timestamp": time.time(),
                "config_schema": self.control_node.config_schema,
                "command_schema": self.control_node.command_schema
            }

            try:
                requests.post(f"{self.control_node.registry_url}/data", json=payload, timeout=0.1)
            except requests.exceptions.RequestException:
                pass


def main():
    # Set to True for verbose output during testing
    verbose_testing = False

    print("Starting Test Controller...")

    try:
        controller = TestController()

        if verbose_testing:
            print("Verbose testing enabled")

        # Start control node
        controller.start_control_node()

        print("\n=== Test Controller Ready ===")
        print("Use the GUI 'TEST_CONTROLLER' window to run tests:")
        print("- Spawn Test Nodes: Create multiple test nodes")
        print("- Remove All Test Nodes: Clean up all test nodes")
        print("- Stress Test: Rapid node creation/deletion")
        print("- Timeout Test: Stop heartbeat for a node")
        print("- Schema Update: Update node schemas")
        print("- Reconnect Test: Test node reconnection")
        print("\nPress Ctrl+C to exit")

        # Keep running
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
        controller.control_node.stop_action_worker()
    except Exception as e:
        print(f"Test controller failed: {e}")
        raise


if __name__ == "__main__":
    main()
