

class LocalCommsNode:
    def __init__(self, node_registry_port=10081, config_schema=None, action_schema=None):
        self.node_registry_port = node_registry_port
        self.set_new_schemas(config_schema, action_schema)

    def queue_data(self, data):
        pass

    def get_data(self) -> list:
        pass

    def set_new_schemas(self, config_schema=None, action_schema=None):
        pass

    def get_connection_status(self):
        pass

    def start_connection(self):
        pass

    def stop_connection(self):
        pass
