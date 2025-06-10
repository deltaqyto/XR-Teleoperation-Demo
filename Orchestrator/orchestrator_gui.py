import dearpygui.dearpygui as dpg
from time import time, sleep

from Orchestrator.gui_node_settings_window import NodeSettingsWindow
from NodeRegistryServer.node_dataclass import Node, ChangeFlags


class OrchestratorGui:
    def __init__(self, window_size=(800, 600)):
        self.node_setting_windows: dict[str, NodeSettingsWindow] = {}

        dpg.create_context()

        dpg.create_viewport(title='XR Orchestrator', width=window_size[0], height=window_size[1])
        dpg.setup_dearpygui()
        dpg.show_viewport()

        self.start = time()
        self.window_created = False

        self.render_frame()

    def __del__(self):
        dpg.destroy_context()

    def render_frame(self):
        if not dpg.is_dearpygui_running():
            return
        dpg.render_dearpygui_frame()

    def get_user_inputs(self):
        output = {}
        for node_id, window in self.node_setting_windows.items():
            a = window.get_queued_actions()
            s = window.get_current_settings()
            output[node_id] = (s, a)
        return output

    def update_from_node_registry(self, node_registry):
        for node_id, node in node_registry.items():
            if node_id not in self.node_setting_windows:
                self.node_setting_windows[node_id] = NodeSettingsWindow(node.node_name, config_schema=node.config_schema, actions_schema=node.command_schema)
                continue
            if node.change_flags.config_schema:
                self.node_setting_windows[node_id].set_config_schema(node.config_schema)
            if node.change_flags.command_schema:
                self.node_setting_windows[node_id].set_actions_schema(node.command_schema)
            if node.change_flags.status_update:
                self.node_setting_windows[node_id].set_visibility(node.life_status.status == "alive")


if __name__ == "__main__":
    o = OrchestratorGui()
    while True:
        o.render_frame()
