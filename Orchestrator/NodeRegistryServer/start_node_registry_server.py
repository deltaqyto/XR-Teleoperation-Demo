import threading
import time

from flask import Flask, request, jsonify


class NodeRegistryServer:
    def __init__(self, port=10080, node_expiry_time=1.0, debug=False):
        self.port = port
        self.debug = debug
        self.parameter_lock = threading.Lock()
        self.node_expiry_time = node_expiry_time

        self.node_data_lock = threading.Lock()
        self.node_registry = {}
        self.node_name_counters = {}
        # End node_data_lock variables

        self.server = Flask(__name__)
        self._register_endpoints()

    def _generate_node_id(self, requested_name):
        # Not thread safe. node_name_counters must be locked

        # Check for dead nodes with exact name match
        for node_id, node_data in self.node_registry.items():
            if (node_data.get('node_name') == requested_name and
                    node_data.get('life_status', {}).get('status') == 'dead'):
                return node_id

        # No dead node found, generate new ID
        if requested_name not in self.node_name_counters:
            self.node_name_counters[requested_name] = 0

        self.node_name_counters[requested_name] += 1
        return f"{requested_name}_{self.node_name_counters[requested_name]}"


    def _register_endpoints(self):
        @self.server.route('/connect', methods=["POST"])
        def connect_node():
            # register node into the registry. Generate a unique node id. if possible grant the node name, otherwise add suffixes. If a dead node exists with the same node name, grant it that id instead, it is likely the same node reconnecting.
            data = request.json

            if 'node_name' not in data:
                print("Malformed packet received:")
                print("'node_name' not in connect packet")
                print('Packet:')
                print(data)
                return jsonify({'message_type': 'error', 'message': 'node_name required in connect packet'})

            requested_name = data['node_name']
            message_time = time.time()

            with self.node_data_lock:
                node_id = self._generate_node_id(requested_name)

                self.node_registry[node_id] = {
                    'node_name': requested_name,
                    'payload_queue': [],
                    'config_schema': None,
                    'command_schema': None,
                    'change_flags': {'config_schema': False, 'command_schema': False},
                    'last_message_time': message_time,
                    'life_status': {'status': 'alive', 'reason': None, 'last_seen': message_time}
                }

        @self.server.route('/disconnect', methods=["POST"])
        def disconnect_node():
            # Mark the node as dead
            data = request.json

            if 'node_id' not in data:
                print("Malformed packet received:")
                print("'node_id' not in disconnect packet")
                print('Packet:')
                print(data)
                return jsonify({'message_type': 'error', 'message': 'node_id required in disconnect packet'})

            node_id = data['node_id']
            message_time = time.time()

            with self.node_data_lock:
                if node_id not in self.node_registry:
                    print("Unregistered node id. Did you forget to connect?")
                    print('Packet:')
                    print(data)
                    return jsonify({'message_type': 'error', 'message': 'Unregistered node id. Did you forget to connect?'})

                self.node_registry[node_id]['life_status'] = {'status': 'dead', 'reason': 'disconnected', 'last_seen': message_time}

        @self.server.route('/data', methods=["POST"])
        def handle_heartbeat():
            data = request.json

            errors = []
            if 'node_id' not in data: errors.append(f"'node_id' not in heartbeat packet")
            if 'node_name' not in data: errors.append(f"'node_name' not in heartbeat packet")
            if 'timestamp' not in data: errors.append(f"'timestamp' not in heartbeat packet")
            if errors:
                print("Malformed packet received:")
                for error in errors:
                    print(error)
                print('Packet:')
                print(data)
                return jsonify({'message_type': 'error', 'message': 'Malformed heartbeat packet', 'errors': errors})

            node_id = data['node_id']

            with self.node_data_lock:
                if node_id not in self.node_registry:
                    print("Unregistered node id. Did you forget to connect?")
                    return jsonify({'message_type': 'error', 'message': 'Unregistered node id. Did you forget to connect?'})

                if 'payload' in data:
                    self.node_registry[node_id]['payload_queue'].append(data['payload'])
                if 'config_schema' in data:
                    self.node_registry[node_id]['config_schema'] = data['config_schema']
                    self.node_registry[node_id]['change_flags']['config_schema'] = True
                if 'command_schema' in data:
                    self.node_registry[node_id]['command_schema'] = data['command_schema']
                    self.node_registry[node_id]['change_flags']['command_schema'] = True
                self.node_registry[node_id]['last_message_time'] = time.time()


    def cleanup_task(self):
        expiry_time = 1
        while True:

            with self.parameter_lock:
                expiry_time = self.node_expiry_time

            with self.node_data_lock:
                now = time.time()
                for node_id, node_data in self.node_registry.items():
                    last_message_time = node_data['last_message_time']
                    if now - last_message_time > expiry_time:
                        reason = 'timeout'
                        if node_id not in self.node_registry:
                            print("Unregistered node id. Did you forget to connect?")
                            return
                        new_reason = self.node_registry[node_id]['life_status'].get('reason', reason)
                        if new_reason is not None:
                            reason = new_reason
                        self.node_registry[node_id]['life_status'] = {'status': 'dead', 'reason': reason, 'last_seen': last_message_time}
                    else:
                        if node_id not in self.node_registry:
                            print("Unregistered node id. Did you forget to connect?")
                            return
                        self.node_registry[node_id]['life_status']  = {'status': 'alive', 'reason': None, 'last_seen': last_message_time}
            time.sleep(expiry_time * 1.1)

    def start(self):
        cleanup_thread = threading.Thread(target=self.cleanup_task)
        cleanup_thread.daemon = True
        cleanup_thread.start()

        server_thread = threading.Thread(target=lambda: self.server.run(host='localhost', port=self.port, debug=self.debug))
        server_thread.daemon = True
        server_thread.start()
        
        print(f"Node Registry Server started on localhost:{self.port}")
        return server_thread

    def set_node_expiry_timeout(self, new_time):
        with self.parameter_lock:
            self.node_expiry_time = new_time
