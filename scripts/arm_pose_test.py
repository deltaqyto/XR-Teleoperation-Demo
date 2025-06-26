def process_incoming_data(comms_node):
    """Process incoming packets and handle joint targets"""
    data = comms_node.get_data()
    if data:
        for packet in data:
            if isinstance(packet, dict) and packet.get("packet_type") == "arm_joint_target":
                joint_angles_rad = packet.get("joint_angles", [])
                if len(joint_angles_rad) == 6:
                    # Convert to degrees and format with padding
                    joint_angles_deg = [math.degrees(angle) for angle in joint_angles_rad]
                    print(f"Received joint target: J1={joint_angles_deg[0]:6.1f}° J2={joint_angles_deg[1]:6.1f}° J3={joint_angles_deg[2]:6.1f}° J4={joint_angles_deg[3]:6.1f}° J5={joint_angles_deg[4]:6.1f}° J6={joint_angles_deg[5]:6.1f}°")
                else:
                    print(f"Invalid joint target packet: expected 6 angles, got {len(joint_angles_rad)}")


import time
import math
from LocalNode.local_comms_node import LocalCommsNode


def send_arm_state(comms_node, config):
    """Send arm state packet with current configuration"""
    try:
        (pos_x, pos_y, pos_z,
         orient_x, orient_y, orient_z, orient_w,
         joint_1_deg, joint_2_deg, joint_3_deg, joint_4_deg, joint_5_deg, joint_6_deg,
         motion_state) = config

        # Group individual values into arrays
        position = [pos_x, pos_y, pos_z]
        orientation = [orient_x, orient_y, orient_z, orient_w]

        # Convert joint angles from degrees to radians
        joint_angles = [
            math.radians(joint_1_deg),
            math.radians(joint_2_deg),
            math.radians(joint_3_deg),
            math.radians(joint_4_deg),
            math.radians(joint_5_deg),
            math.radians(joint_6_deg)
        ]

        arm_state_packet = {
            "packet_type": "arm_state",
            "position": position,
            "orientation": orientation,
            "joint_angles": joint_angles,
            "motion_state": motion_state
        }

        comms_node.queue_data(arm_state_packet)
        print(f"Sent arm state: pos={position}, orient={orientation}, joints={joint_angles}, state={motion_state}")

    except Exception as e:
        print(f"Error sending arm state: {e}")


if __name__ == "__main__":
    config_schema = [
        ("text", "UR3e Arm Control Interface", {"color": (100, 150, 255), "wrap": 400}, None),
        ("separator", "", {}, None),

        ("header", "Position (metres)", {"collapsible": True, "default_open": True}, None),
        ("float", "Position X", {"min": -1.0, "max": 1.0, "step": 0.001}, 0.0),
        ("float", "Position Y", {"min": -1.0, "max": 1.0, "step": 0.001}, 0.0),
        ("float", "Position Z", {"min": -1.0, "max": 1.0, "step": 0.001}, 0.0),
        ("end", "", {}, None),

        ("header", "Orientation (quaternion)", {"collapsible": True, "default_open": True}, None),
        ("float", "Orientation X", {"min": -1.0, "max": 1.0, "step": 0.001}, 0.0),
        ("float", "Orientation Y", {"min": -1.0, "max": 1.0, "step": 0.001}, 0.0),
        ("float", "Orientation Z", {"min": -1.0, "max": 1.0, "step": 0.001}, 0.0),
        ("float", "Orientation W", {"min": -1.0, "max": 1.0, "step": 0.001}, 1.0),
        ("end", "", {}, None),

        ("header", "Joint Angles (degrees)", {"collapsible": True, "default_open": True}, None),
        ("int", "Joint 1", {"min": -180, "max": 180}, 0),
        ("int", "Joint 2", {"min": -180, "max": 180}, 0),
        ("int", "Joint 3", {"min": -180, "max": 180}, 0),
        ("int", "Joint 4", {"min": -180, "max": 180}, 0),
        ("int", "Joint 5", {"min": -180, "max": 180}, 0),
        ("int", "Joint 6", {"min": -180, "max": 180}, 0),
        ("end", "", {}, None),

        ("radio", "Motion State", {"items": ["idle", "executing"], "horizontal": True}, "idle"),
    ]

    command_schema = {
        "send_arm_state": [{"default_open": True}, "Send Arm State Packet"]
    }

    node = LocalCommsNode("Arm Controls", "arm_controls", config_schema=config_schema, action_schema=command_schema)

    try:
        while True:
            # Process incoming data
            process_incoming_data(node)

            # Process outgoing actions
            actions = node.get_actions()
            for action_name, params in actions:
                if action_name == "send_arm_state":
                    changed, config = node.get_latest_config()
                    if not config:
                        print("Error: No configuration set. Please configure settings before sending.")
                        continue

                    send_arm_state(node, config)

            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
