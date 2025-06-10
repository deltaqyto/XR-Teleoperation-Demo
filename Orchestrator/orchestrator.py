import time

from NodeRegistryServer.node_registry_server import NodeRegistryServer
from Orchestrator.orchestrator_gui import OrchestratorGui
from RemoteDiscovery.remote_discovery import RemoteDiscovery

class Orchestrator:
    def __init__(self, node_registry_port=10081):
        self.node_registry_server = NodeRegistryServer(port=node_registry_port)
        self.remote_discovery = RemoteDiscovery(service_name="XR Quest")
        self.gui = OrchestratorGui()  # GUI cannot be threaded, must be on main thread

        self.main_loop()

    def main_loop(self):
        while True:
            remote_data = self.remote_discovery.get_remote()
            self.node_registry_server.update_remote_data(remote_data)

            user_inputs = self.gui.get_user_inputs()

            for node_id, (settings, actions) in user_inputs.items():
                self.node_registry_server.add_outbound_messages(node_id, config=settings, actions=actions)

            # check the node server for updates
            node_registry = self.node_registry_server.get_node_registry()
            self.gui.update_from_node_registry(node_registry)

            self.gui.render_frame()
            time.sleep(0.01)


if __name__ == "__main__":
    o = Orchestrator()
