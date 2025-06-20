import threading
import time

from typing import Dict
from copy import deepcopy

from flask import Flask, request, jsonify
from NodeRegistryServer.node_dataclass import Node, LifeStatus, ChangeFlags

class NodeRegistryServer:
    def __init__(self, port=10081, node_expiry_time=1.0, debug=False):
        self.port = port
        self.debug = debug
        self.parameter_lock = threading.Lock()
        self.node_expiry_time = node_expiry_time

        self.node_data_lock = threading.Lock()
        self.node_registry: Dict[str, Node] = {}
        self.node_name_counters: Dict[str, int] = {}
        self.node_outbound_cache: Dict[str, list] = {}
        self.remote_data = None

        self.server = Flask(__name__)
        self._register_endpoints()

        self.start()

    def _generate_node_id(self, requested_name: str) -> str:
        # Not thread safe. node_name_counters must be locked

        # Check for dead nodes with exact name match
        for node_id, node in self.node_registry.items():
            if node.node_name == requested_name and node.life_status.status == 'dead':
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
                self.node_registry[node_id] = Node(requested_name, node_id, message_time)

                node = self.node_registry[node_id]

                if 'payload' in data:
                    node.payload_queue.append(data['payload'])
                if 'config_schema' in data:
                    node.config_schema = data['config_schema']
                    node.change_flags.config_schema = True
                if 'command_schema' in data:
                    node.command_schema = data['command_schema']
                    node.change_flags.command_schema = True

                out = {'message_type': 'success', 'node_id': node_id}
                if self.remote_data is not None:
                    out['remote_ports'] = self.remote_data

                return jsonify(out)

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
                self.node_registry[node_id].change_flags.status_update = True
                self.node_registry[node_id].life_status = LifeStatus(status='dead', reason='disconnected', last_seen=message_time)
            return jsonify({'message_type': 'success'})

        @self.server.route('/data', methods=["POST"])
        def handle_heartbeat():
            data = request.json

            errors = []
            if 'node_id' not in data: errors.append("'node_id' not in heartbeat packet")
            if 'node_name' not in data: errors.append("'node_name' not in heartbeat packet")
            if 'timestamp' not in data: errors.append("'timestamp' not in heartbeat packet")
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

                node = self.node_registry[node_id]

                if 'payload' in data:
                    node.payload_queue.append(data['payload'])
                if 'config_schema' in data:
                    node.config_schema = data['config_schema']
                    node.change_flags.config_schema = True
                if 'command_schema' in data:
                    node.command_schema = data['command_schema']
                    node.change_flags.command_schema = True

                node.last_message_time = time.time()
                out = {'message_type': 'heartbeat_response', 'node_id': node_id}
                if self.remote_data is not None:
                    out['remote_ports'] = self.remote_data

                if node_id in self.node_outbound_cache:
                    out = jsonify({**out, **{'config_update': self.node_outbound_cache[node_id][0], 'actions': self.node_outbound_cache[node_id][1]}})
                    self.node_outbound_cache[node_id] = [self.node_outbound_cache[node_id][0], []]
                    return out
                return jsonify(out)

    def cleanup_task(self):
        expiry_time = 1
        while True:
            with self.parameter_lock:
                expiry_time = self.node_expiry_time

            with self.node_data_lock:
                now = time.time()
                for node_id, node in self.node_registry.items():
                    if now - node.last_message_time > expiry_time:
                        if node.life_status.status != 'dead':
                            node.change_flags.status_update = True
                        reason = 'timeout'
                        current_reason = node.life_status.reason
                        if current_reason is not None:
                            reason = current_reason
                        node.life_status = LifeStatus(status='dead', reason=reason, last_seen=node.last_message_time)
                    else:
                        if node.life_status.status != 'alive':
                            node.change_flags.status_update = True
                        node.life_status = LifeStatus(status='alive', reason=None, last_seen=node.last_message_time)
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

    def get_node_registry(self):
        with self.node_data_lock:
            data = deepcopy(self.node_registry)
            for node in self.node_registry.values():
                node.change_flags.config_schema = False
                node.change_flags.command_schema = False
                node.change_flags.new_node = False
                node.change_flags.status_update = False
                node.payload_queue = []
        return data

    def add_outbound_messages(self, node_id, config=None, actions=None):
        with self.node_data_lock:
            if node_id not in self.node_outbound_cache:
                self.node_outbound_cache[node_id] = [[], []]
            self.node_outbound_cache[node_id][0] = config or self.node_outbound_cache[node_id][0]
            self.node_outbound_cache[node_id][1] += actions or []

    def update_remote_data(self, remote_data):
        with self.node_data_lock:
            self.remote_data = remote_data
