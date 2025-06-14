import time
from re import fullmatch

import dearpygui.dearpygui as dpg
from copy import deepcopy

from dearpygui.dearpygui import render_dearpygui_frame


class NodeSettingsWindow:
    def __init__(self, node_title="NullTitle", config_schema=None, actions_schema=None):
        self.node_title = node_title
        self.config_schema = config_schema or []
        self.actions_schema = actions_schema or {}

        # Caches for outbound commands
        self.action_cache = []
        self.current_config_cache = []

        # Widget tracking for value retrieval
        self.config_widgets = []
        self.action_widget_map = {}

        self.config_validation_errors = {}
        self.action_validation_errors = {}

        # Internal widget mapping
        self.widget_map = {
            # Layout and documentation widgets
            "separator": dpg.add_separator,
            "text": dpg.add_text,
            "header": dpg.add_collapsing_header,
            'end': None,

            # Basic inputs
            "bool": dpg.add_checkbox,
            "int": dpg.add_input_int,
            "float": dpg.add_input_float,
            "double": dpg.add_input_double,
            "string": dpg.add_input_text,
            "radio": dpg.add_radio_button,
            "dropdown": dpg.add_combo,
            "listbox": dpg.add_listbox,
            "knob": dpg.add_knob_float,

            # IP inputs
            "port": dpg.add_input_int,
            "ip_address": dpg.add_input_intx,

            # Complex types
            "colour": dpg.add_color_picker,
        }

        self.config_button = None
        self.action_buttons = {}
        self.window_tag = dpg.add_window(label=self.node_title)

        with dpg.theme() as self.error_button_theme:
            with dpg.theme_component(dpg.mvButton):
                dpg.add_theme_color(dpg.mvThemeCol_Button, (180, 50, 50))
                dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (220, 70, 70))
                dpg.add_theme_color(dpg.mvThemeCol_ButtonActive, (140, 30, 30))

        self.rebuild_window()

    def set_config_schema(self, new_schema):
        self.config_schema = new_schema
        self.rebuild_window()

    def set_actions_schema(self, new_schema):
        self.actions_schema = new_schema
        self.rebuild_window()

    def get_current_settings(self):
        return deepcopy(self.current_config_cache)

    def get_queued_actions(self):
        out_queue = deepcopy(self.action_cache)
        self.action_cache = []
        return out_queue

    def rebuild_window(self):
        self.action_widget_map = {}
        self.config_widgets = []
        self.action_buttons = {}
        dpg.delete_item(self.window_tag, children_only=True)

        indent_level = 0
        parent_tags = [self.window_tag]
        dpg.add_separator(label='Configuration', parent=self.window_tag)
        for action_data in self.config_schema:
            indent_level, widget_tag, parent_tags, is_configurable = self.spawn_widget(*action_data, parent_tags, indent_level)
            if is_configurable:
                self.config_widgets.append(widget_tag)
        self.config_button = dpg.add_button(label="Apply Configuration", callback=self._config_callback, parent=self.window_tag)
        dpg.add_separator(label='Actions', parent=self.window_tag)
        for action_name, action_data in self.actions_schema.items():
            indent_level = 0
            options, widgets = action_data
            action_widgets = []
            with dpg.collapsing_header(label=action_name, parent=self.window_tag, default_open=options.get('default_open', False)) as header:
                parent_tags = [self.window_tag, header]
                if type(widgets) is str:
                    self.action_buttons[action_name] = (dpg.add_button(label=widgets, user_data=action_name, callback=self._action_callback, parent=self.window_tag), widgets)
                    self.action_widget_map[action_name] = []
                    continue
                for widget in widgets:
                    if type(widget) is str:
                        self.action_buttons[action_name] = (dpg.add_button(label=widget, user_data=action_name, callback=self._action_callback, parent=self.window_tag), widget)
                        self.action_widget_map[action_name] = action_widgets
                        break
                    else:
                        indent_level, widget_tag, parent_tags, is_configurable = self.spawn_widget(*widget, parent_tags, indent_level, action_name=action_name)
                        if is_configurable:
                            action_widgets.append(widget_tag)

    def spawn_widget(self, widget_type, label, options, default_value, parents, indent_level, action_name=None):
        widget_kwargs = {'label': label, 'parent': parents[-1], 'indent': indent_level}
        widget_adder = self.widget_map[widget_type]

        validation_data = {
            'widget_type': widget_type,
            'action_name': action_name,
        }

        match widget_type:
            case 'separator':
                pass
            case 'text':
                widget_kwargs['default_value'] = widget_kwargs['label']
                widget_kwargs['label'] = None
                widget_kwargs['wrap'] = options.get('wrap', 0)
                widget_kwargs['color'] = options.get('color', (255, 0, 0))

            case 'header':
                header_id = dpg.add_collapsing_header(label=label, parent=parents[-1], default_open=options.get('default_open', False), leaf=not options.get("collapsible", False))
                parents.append(header_id)
                return indent_level + 1, header_id, parents, False
            
            case 'end':
                parents.pop(-1)
                return indent_level - 1, None, parents, False

            case 'bool':
                widget_kwargs['default_value'] = default_value
            case 'int':
                widget_kwargs['default_value'] = default_value

                if 'min' in options:
                    widget_kwargs['min_value'] = options.get('min')
                if 'max' in options:
                    widget_kwargs['max_value'] = options.get('max')

                if options.get('vertical_slider', False):
                    widget_kwargs['vertical'] = True
                if options.get('horizontal_slider', False) or widget_kwargs.get('vertical', False):
                    count = options.get("count", 1)
                    if count < 1 or count > 4:
                        raise ValueError(f"Invalid count for {label}: {count}. Must be 1 - 4")
                    if count == 1:
                        widget_adder = dpg.add_slider_int
                    else:
                        widget_adder = dpg.add_slider_intx
                        widget_kwargs['size'] = count
                else:
                    if 'step' in options:
                        widget_kwargs['step'] = options.get('step')
                    count = options.get("count", 1)
                    if count < 1 or count > 4:
                        raise ValueError(f"Invalid count for {label}: {count}. Must be 1 - 4")
                    if count == 1:
                        widget_adder = dpg.add_input_int
                    else:
                        widget_adder = dpg.add_input_intx
                        widget_kwargs['size'] = count
                    widget_kwargs['on_enter'] = True
            case 'float':
                widget_kwargs['default_value'] = default_value

                if 'min' in options:
                    widget_kwargs['min_value'] = options.get('min')
                if 'max' in options:
                    widget_kwargs['max_value'] = options.get('max')

                if options.get('vertical_slider', False):
                    widget_kwargs['vertical'] = True
                if options.get('horizontal_slider', False) or widget_kwargs.get('vertical', False):
                    count = options.get("count", 1)
                    if count < 1 or count > 4:
                        raise ValueError(f"Invalid count for {label}: {count}. Must be 1 - 4")
                    if count == 1:
                        widget_adder = dpg.add_slider_float
                    else:
                        widget_adder = dpg.add_slider_floatx
                        widget_kwargs['size'] = count
                else:
                    if 'step' in options:
                        widget_kwargs['step'] = options.get('step')
                    count = options.get("count", 1)
                    if count < 1 or count > 4:
                        raise ValueError(f"Invalid count for {label}: {count}. Must be 1 - 4")
                    if count == 1:
                        widget_adder = dpg.add_input_float
                    else:
                        widget_adder = dpg.add_input_floatx
                        widget_kwargs['size'] = count
                    widget_kwargs['on_enter'] = True
            case 'double':
                widget_kwargs['default_value'] = default_value

                if 'min' in options:
                    widget_kwargs['min_value'] = options.get('min')
                if 'max' in options:
                    widget_kwargs['max_value'] = options.get('max')

                if options.get('vertical_slider', False):
                    widget_kwargs['vertical'] = True
                if options.get('horizontal_slider', False) or widget_kwargs.get('vertical', False):
                    count = options.get("count", 1)
                    if count < 1 or count > 4:
                        raise ValueError(f"Invalid count for {label}: {count}. Must be 1 - 4")
                    if count == 1:
                        widget_adder = dpg.add_slider_double
                    else:
                        widget_adder = dpg.add_slider_doublex
                        widget_kwargs['size'] = count
                else:
                    if 'step' in options:
                        widget_kwargs['step'] = options.get('step')
                    count = options.get("count", 1)
                    if count < 1 or count > 4:
                        raise ValueError(f"Invalid count for {label}: {count}. Must be 1 - 4")
                    if count == 1:
                        widget_adder = dpg.add_input_double
                    else:
                        widget_adder = dpg.add_input_doublex
                        widget_kwargs['size'] = count
                    widget_kwargs['on_enter'] = True
            case 'string':
                widget_kwargs['default_value'] = default_value
                widget_kwargs['multiline'] = options.get('multiline', False)
                widget_kwargs['hint'] = options.get('hint', "")
                widget_kwargs['on_enter'] = True
                widget_kwargs['password'] = options.get('password', False)
                widget_kwargs['uppercase'] = options.get('uppercase', False)
                widget_kwargs['no_spaces'] = options.get('no_spaces', False)
                widget_kwargs['decimal'] = options.get('decimal', False)
                widget_kwargs['hexadecimal'] = options.get('hexadecimal', False)
                widget_kwargs['scientific'] = options.get('scientific', False)

                validation_data['regex'] = options.get('regex')
                validation_data['max_length'] = options.get('max_length')
                widget_kwargs['callback'] = self._validation_callback
                widget_kwargs['user_data'] = validation_data
            case 'knob':
                widget_kwargs['default_value'] = default_value
                widget_kwargs['min_value'] = options.get('min', 0)
                widget_kwargs['max_value'] = options.get('max', 1)

            case 'radio':
                widget_kwargs['items'] = options.get('items', [])
                widget_kwargs['horizontal'] = options.get('horizontal', False)
                widget_kwargs['default_value'] = default_value

            case 'dropdown':
                widget_kwargs['items'] = options.get('items', [])
                widget_kwargs['default_value'] = default_value
            case 'listbox':
                widget_kwargs['items'] = options.get('items', [])
                widget_kwargs['default_value'] = default_value

            case 'port':
                widget_kwargs['default_value'] = default_value
                widget_kwargs['min_value'] = options.get('min', None)
                widget_kwargs['max_value'] = options.get('max', None)

                validation_data['min'] = options.get('min')
                validation_data['max'] = options.get('max')
                validation_data['blacklist'] = options.get('blacklist')
                widget_kwargs['callback'] = self._validation_callback
                widget_kwargs['user_data'] = validation_data
            case 'ip_address':
                widget_kwargs['size'] = 4
                widget_kwargs['default_value'] = default_value

                widget_kwargs['callback'] = self._validation_callback
                widget_kwargs['user_data'] = validation_data
                
            case 'colour':
                widget_kwargs['default_value'] = default_value
                if options.get("format", "RGB").upper() == "RGB":
                    widget_kwargs['display_rgb'] = True
                elif options.get("format", "RGB").upper() == "HSV":
                    widget_kwargs['display_hsv'] = True
                else:
                    widget_kwargs['display_hex'] = True
                widget_kwargs['no_alpha'] = options.get("alpha", False)
                widget_kwargs['alpha_preview'] = options.get("alpha", False)
                widget_kwargs['alpha_bar'] = options.get("alpha", False)

        is_configurable = True
        if widget_type in ['separator', 'text', 'header', 'end']:
            is_configurable = False
        return indent_level, widget_adder(**widget_kwargs), parents, is_configurable

    def _config_callback(self, sender, app_data, user_data):
        if any(len(errors) > 0 for errors in self.config_validation_errors.values()):
            return

        dpg.configure_item(sender, label="Apply Configuration")
        dpg.bind_item_theme(sender, 0)
        output_data = dpg.get_values(self.config_widgets)
        self.current_config_cache = output_data

    def _action_callback(self, sender, app_data, user_data):
        action_errors = self.action_validation_errors.get(user_data, {})
        if any(len(errors) > 0 for errors in action_errors.values()):
            return
        output_data = dpg.get_values(self.action_widget_map[user_data])
        self.action_cache.append((user_data, output_data))

    def _validation_callback(self, sender, app_data, user_data):
        invalid = []
        match user_data['widget_type']:
            case 'string':
                if user_data.get('max_length') is not None and len(app_data) > user_data['max_length']:
                    invalid.append(f"String is longer than allowed length {user_data['max_length']}")
                if not fullmatch(user_data.get('regex', '.*'), app_data):
                    invalid.append(f"String does not meet format regex {user_data.get('regex', '.*')}")
            case 'port':
                if user_data.get('blacklist') is not None and app_data in user_data.get('blacklist', []):
                    invalid.append(f"Port {app_data} is in blacklisted ports")
                if not user_data.get('min', 0) <= app_data <= user_data.get('max', 65535):
                    invalid.append(f"Port {app_data} is out of allowed range ({user_data.get('min', 0)}, {user_data.get('max', 65535)})")
            case 'ip_address':
                for i, value in enumerate(app_data):
                    if not 0 <= value <= 255:
                        invalid.append(f"Field {i+1} of ip address is out of range ({value})")

        if user_data['action_name'] is None:
            self.config_validation_errors[sender] = invalid
            if any(v for v in self.config_validation_errors.values()):
                dpg.configure_item(self.config_button, label="Fix Errors First")
                dpg.bind_item_theme(self.config_button, self.error_button_theme)
            else:
                dpg.configure_item(self.config_button, label="Apply Configuration")
                dpg.bind_item_theme(self.config_button, 0)
        else:
            if user_data['action_name'] not in self.action_validation_errors:
                self.action_validation_errors[user_data['action_name']] = {}

            self.action_validation_errors[user_data['action_name']][sender] = invalid

            if any([v for v in self.action_validation_errors[user_data['action_name']].values()]):
                dpg.configure_item(self.action_buttons[user_data['action_name']][0], label="Fix Errors First")
                dpg.bind_item_theme(self.action_buttons[user_data['action_name']][0], self.error_button_theme)
            else:
                dpg.configure_item(self.action_buttons[user_data['action_name']][0], label=self.action_buttons[user_data['action_name']][1])
                dpg.bind_item_theme(self.action_buttons[user_data['action_name']][0], 0)

    def set_visibility(self, visible):
        if visible:
            dpg.show_item(self.window_tag)
            return
        dpg.hide_item(self.window_tag)

    # TODO Allow maintaining past values in regenerated fields

if __name__ == "__main__":
    config_schema = [
        # Layout and documentation widgets
        ("text", "This demonstrates all widget types and options", {"color": (100, 150, 255), "wrap": 400}, None),
        ("separator", "", {}, None),

        # Basic inputs - comprehensive coverage
        ("bool", "Enable Feature", {}, True),
        ("bool", "Debug Mode", {}, False),

        # Integer widgets with all variations
        ("int", "Simple Int", {}, 42),
        ("int", "Int with Limits", {"min": 0, "max": 100}, 50),
        ("int", "Int with Step", {"min": 0, "max": 100, "step": 5}, 25),
        ("int", "Horizontal Slider", {"min": 0, "max": 100, "horizontal_slider": True}, 30),
        ("int", "Vertical Slider", {"min": 0, "max": 50, "vertical_slider": True}, 25),
        ("int", "Multi Int (2)", {"count": 2}, [10, 20]),
        ("int", "Multi Int Slider (3)", {"count": 3, "horizontal_slider": True, "min": 0, "max": 255}, [128, 64, 192]),

        # Float widgets with all variations
        ("float", "Simple Float", {}, 3.14),
        ("float", "Float with Limits", {"min": 0.0, "max": 10.0}, 5.0),
        ("float", "Float with Step", {"min": 0.0, "max": 1.0, "step": 0.1}, 0.5),
        ("float", "Float H-Slider", {"min": 0.0, "max": 2.0, "horizontal_slider": True}, 1.0),
        ("float", "Float V-Slider", {"min": 0.0, "max": 1.0, "vertical_slider": True}, 0.7),
        ("float", "Multi Float (4)", {"count": 4}, [1.0, 2.0, 3.0, 4.0]),

        # Double precision
        ("double", "Double Precision", {"min": 0.0, "max": 1.0, "step": 0.001}, 0.123456789),
        ("double", "Double Slider", {"min": -1.0, "max": 1.0, "horizontal_slider": True}, 0.0),

        # String inputs with all options
        ("string", "Simple String", {}, "Hello World"),
        ("string", "Password", {"password": True}, "secret123"),
        ("string", "Multiline Text", {"multiline": True}, "Line 1\nLine 2\nLine 3"),
        ("string", "With Hint", {"hint": "Enter your name here"}, ""),
        ("string", "Uppercase Only", {"uppercase": True}, "CAPS"),
        ("string", "No Spaces", {"no_spaces": True}, "NoSpacesAllowed"),
        ("string", "Decimal Numbers", {"decimal": True}, "123.456"),
        ("string", "Hexadecimal", {"hexadecimal": True}, "DEADBEEF"),
        ("string", "Scientific", {"scientific": True}, "1.23e-4"),

        # Selection widgets
        ("radio", "Protocol (Horizontal)", {"items": ["HTTP", "HTTPS", "FTP"], "horizontal": True}, "HTTPS"),
        ("radio", "Mode (Vertical)", {"items": ["Auto", "Manual", "Custom"]}, "Auto"),
        ("dropdown", "Resolution", {"items": ["640x480", "1280x720", "1920x1080", "4K"]}, "1920x1080"),
        ("listbox", "Features", {"items": ["Feature A", "Feature B", "Feature C", "Feature D"]}, "Feature B"),

        # Knob control
        ("knob", "Volume", {"min": 0.0, "max": 100.0}, 75.0),
        ("knob", "Sensitivity", {"min": 0.1, "max": 2.0}, 1.0),

        ("separator", "Network Configuration", {}, None),

        # IP inputs
        ("port", "Server Port", {"min": 1024, "max": 65535}, 8080),
        ("port", "Client Port", {"min": 1024, "max": 49151}, 12345),
        ("ip_address", "Server IP", {}, [192, 168, 1, 100]),
        ("ip_address", "Gateway IP", {}, [192, 168, 1, 1]),

        # Colour inputs with different formats
        ("colour", "RGB Colour", {"format": "RGB"}, [255, 128, 64, 255]),
        ("colour", "HSV Colour", {"format": "HSV"}, [180, 128, 200, 255]),
        ("colour", "Hex Colour", {"format": "HEX"}, [64, 255, 128, 255]),

        # Header demonstration with nesting
        ("header", "Advanced Settings", {"collapsible": True, "default_open": False}, None),
        ("int", "Cache Size", {"min": 1, "max": 1000}, 100),
        ("float", "Timeout", {"min": 1.0, "max": 60.0}, 30.0),

        ("header", "Nested Section", {"collapsible": True, "default_open": True}, None),
        ("bool", "Enable Logging", {}, True),
        ("string", "Log Path", {}, "/var/log/app.log"),
        ("end", "", {}, None),

        ("bool", "Auto-save", {}, False),
        ("end", "", {}, None),
    ]

    actions_schema = {
        "simple_action": [{"default_open": False}, "Execute Simple Action"],

        "restart_service": [{"default_open": True}, "Restart All Services"],

        "complex_calibration": [{"default_open": True}, [
            ("dropdown", "Target Pattern", {"items": ["checkerboard", "circles", "asymmetric_circles", "charuco"]}, "checkerboard"),
            ("int", "Iterations", {"min": 1, "max": 50, "step": 1}, 10),
            ("float", "Precision", {"min": 0.1, "max": 2.0, "step": 0.1}, 1.0),
            ("bool", "High Accuracy Mode", {}, False),
            ("bool", "Save Results", {}, True),
            "Start Calibration Process"
        ]],

        "export_settings": [{"default_open": False}, [
            ("radio", "Format", {"items": ["JSON", "XML", "YAML", "INI", "TOML"], "horizontal": True}, "JSON"),
            ("bool", "Include Defaults", {}, False),
            ("bool", "Include Metadata", {}, True),
            ("bool", "Compress Output", {}, False),
            ("dropdown", "Compression", {"items": ["none", "gzip", "bzip2", "lzma"]}, "none"),
            ("string", "Filename", {"hint": "Enter filename without extension"}, "config_export"),
            "Export Configuration"
        ]],

        "network_diagnostics": [{"default_open": False}, [
            ("radio", "Test Type", {"items": ["ping", "traceroute", "bandwidth", "latency"]}, "ping"),
            ("ip_address", "Target Host", {}, [8,8,8,8]),
            ("int", "Packet Count", {"min": 1, "max": 1000}, 10),
            ("float", "Timeout (s)", {"min": 0.1, "max": 30.0}, 5.0),
            ("bool", "Continuous Mode", {}, False),
            ("bool", "Detailed Output", {}, True),
            "Run Network Test"
        ]],

        "system_info": [{"default_open": False}, [
            ("bool", "CPU Information", {}, True),
            ("bool", "Memory Usage", {}, True),
            ("bool", "Disk Usage", {}, True),
            ("bool", "Network Interfaces", {}, False),
            ("bool", "Process List", {}, False),
            "Gather System Information"
        ]]
    }

    dpg.create_context()
    dpg.create_viewport(title="Widget Demo", width=800, height=1000)

    window = NodeSettingsWindow("Widget Demo", config_schema, actions_schema)

    dpg.setup_dearpygui()
    dpg.show_viewport()
    while dpg.is_dearpygui_running():
        time.sleep(0.01)
        a = window.get_queued_actions()
        print(a) if a else None
        a = window.get_current_settings()
        print(a) if a else None
        render_dearpygui_frame()
    dpg.destroy_context()




"""
# Layout and documentation widgets
'separator': []  # Any text provided in the label will be applied
'text': ['color', 'wrap']  # Wrap is the px from the start of the text to begin wrapping (leave blank or 0 for autowrap). Color is rgba or rgb, 0 - 255 range
'header': ['collapsible', 'default_open']  # Requires closure with end. Both params are bool, default false
'end': []  # Must be used to close headers

# Basic inputs
'bool': []  # A checkbox, simple
'int': ['min', 'max', 'step', 'draggable', 'horizontal_slider', 'vertical_slider', 'count']  # Count is int 1 - 4, causes multiple input fields to spawn on the same widget. If count is not 1, defaults, min, max and outputs will be lists of size 4. Step must be set to cause +- buttons to spawn. Step only works on fields, not sliders. 
'float': ['min', 'max', 'step', 'draggable', 'horizontal_slider', 'vertical_slider', 'count']  # See int
'double': ['min', 'max', 'step', 'draggable', 'horizontal_slider', 'vertical_slider', 'count']  # See int. More detailed float
'string': ["max_length", "regex", "multiline", "password", "hint", 'uppercase', 'no_spaces', 'decimal', 'hexadecimal', 'scientific']  # Hint text shows when box is empty. conflicts with any default value. Max length and regex are only checked when done typing. Other modes will work while typing.
'radio': ["items", "horizontal"]  # Items is a tuple of string options. Horizontal changes the layout. Default value must be a string that matches an option. Checkboxes are not a thing here, make them by stacking bool inputs
'dropdown': ['items']  # Items is a tuple of string options. Default value must be a string that matches an option.
'listbox': ['items', 'max_selections']  # Items is a tuple of string options. Default value must be a string that matches an option. Listboxes allow multi-selection
'knob': ['min', 'max']  # min, max unsupported

# IP inputs
'port': ['min', 'max', 'blacklist']  # int input, but allows checking against a blacklist
'ip_address': []  # Performs some checking to make sure the input ip is formatted right. Output is provided as a string with '.' separators.

# Color types
'colour': ['format', 'alpha']  # Format is 'rgb', 'hsv' or 'hex'. alpha is boolean to enable alpha selection. if alpha is true, will expect RGBA format. Alpha is currently broken. Output is always RGBA, regardless of input format
"""
