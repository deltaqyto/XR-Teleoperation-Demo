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

import time
from typing import Dict
from RemoteConnector.remote_connector import JSONRemoteConnector
from LocalNode.node_client import NodeClient


def perform_echo_ping(remote_discovery: Dict, ping_count: int, delay: float):
    """Perform ping test using JSONRemoteConnector to echo port."""
    # Extract remote IP and ports
    remote_ip = remote_discovery.get('remote_ip', 'localhost')
    remote_ports = remote_discovery.get('remote_ports', {})

    # Find echo port
    echo_port = remote_ports.get('echo')

    if not echo_port:
        print("  No echo port found in remote discovery")
        return

    print(f"  Connecting to echo service at {remote_ip}:{echo_port}")

    connector = JSONRemoteConnector(remote_ip, echo_port)

    if not connector.connect():
        print("  Failed to connect to echo service")
        return

    try:
        # Send pings and check for responses
        for i in range(ping_count):
            ping_data = {
                "ping": i + 1,
                "timestamp": time.time(),
                "message": f"Test ping {i + 1}"
            }

            connector.send_data(ping_data)
            print(f"    Sent ping {i + 1}")

            # Wait for response
            time.sleep(0.1)  # Brief wait for response
            responses = connector.get_received_data()

            if responses:
                print(f"    Received echo response: {responses}")
            else:
                print(f"    No response received for ping {i + 1}")

            if i < ping_count - 1:  # Don't delay after the last ping
                time.sleep(delay)

        print(f"  Completed {ping_count} pings")

    finally:
        connector.disconnect()


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

    # Define available actions/commands - using human-readable names
    command_schema = {
        "Send Pings": [{"default_open": True}, [
            ("int", "Pings", {"min": 1, "max": 1000}, 5),
            ("float", "Delay", {"min": 0.01, "max": 5}, 0.5),
            "Send Pings"
        ]],
        "Stop Processing": [{"default_open": False}, "Stop All Processing"],
        "Export Data": [{"default_open": False}, [
            ("dropdown", "Format", {"items": ["JSON", "CSV", "Parquet"]}, "JSON"),
            ("string", "Filename", {}, "export_data"),
            "Export Current Data"
        ]],
        "Calibrate": [{"default_open": False}, [
            ("int", "Iterations", {"min": 1, "max": 20}, 5),
            ("float", "Tolerance", {"min": 0.001, "max": 1.0}, 0.1),
            "Run Calibration"
        ]]
    }

    # Create and start the node client with verbose action logging for demo
    client = NodeClient("demo_node", config_schema, command_schema, verbose_actions=True)
    client.start()

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
                new_command_schema["Debug Dump"] = [{"default_open": False}, "Dump Debug Info"]

                client.update_schemas(config_schema=new_config_schema, command_schema=new_command_schema)
                schema_updated = True

            # Check for new actions from orchestrator
            actions = client.get_pending_actions()
            for action_name, params in actions:
                # Handle your actions here (verbose logging handled by client)
                if action_name == "Send Pings":
                    ping_count, delay = params
                    print(f"  Starting pings with count={ping_count}, delay={delay}")

                    # Get remote discovery and perform ping
                    remote_info = client.get_remote_discovery()
                    perform_echo_ping(remote_info, ping_count, delay)

                elif action_name == "Stop Processing":
                    print("  Stopping all processing")

                elif action_name == "Export Data":
                    format_type, filename = params
                    print(f"  Exporting data as {format_type} to {filename}")

                elif action_name == "Calibrate":
                    iterations, tolerance = params
                    print(f"  Running calibration: {iterations} iterations, tolerance={tolerance}")

                elif action_name == "Debug Dump":
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
