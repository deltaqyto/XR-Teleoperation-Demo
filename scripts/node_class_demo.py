import time
from LocalNode.local_comms_node import LocalCommsNode


def perform_echo_ping(comms_node, pings, delay):
    """Demonstration utility function. Do not include"""
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
            

if __name__ == "__main__":
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
