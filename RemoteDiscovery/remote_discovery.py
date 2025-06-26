import socket
import json
import time
import threading


class RemoteDiscovery:
    def __init__(self, service_name="XR Quest", discovery_port=9999, autostart=True, debug=False):
        self.service_name = service_name
        self.discovery_port = discovery_port
        self.debug = debug
        self.latest_service = None
        self.discovery_thread = None
        self.running = False
        self.lock = threading.Lock()

        if autostart:
            self.start_discovery()

    def _discovery_worker(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('', self.discovery_port))
            sock.settimeout(1.0)

            if self.debug:
                print(f"Background discovery started for service: {self.service_name}")

            while self.running:
                try:
                    data, addr = sock.recvfrom(1024)
                    announcement = json.loads(data.decode())
                    if announcement.get("service") == self.service_name:
                        print(announcement)
                        service_ip = announcement["ip"]
                        service_ports = announcement["ports"]

                        with self.lock:
                            self.latest_service = {
                                "ip": service_ip,
                                "ports": service_ports,
                                "last_seen": time.time()
                            }

                        if self.debug:
                            print(f"Updated service: {service_ip}:{service_ports}")

                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        print(f"Discovery error: {e}\nData: {data}")

    def start_discovery(self, service_name=None):
        if service_name:
            self.service_name = service_name

        if self.discovery_thread and self.discovery_thread.is_alive():
            self.stop_discovery()

        self.running = True
        self.discovery_thread = threading.Thread(target=self._discovery_worker, daemon=True)
        self.discovery_thread.start()

    def stop_discovery(self):
        self.running = False
        if self.discovery_thread and self.discovery_thread.is_alive():
            self.discovery_thread.join(timeout=2.0)

    def restart_discovery(self, service_name=None):
        self.stop_discovery()
        self.start_discovery(service_name)

    def get_remote(self):
        with self.lock:
            if self.latest_service:
                return {
                    "remote_ip": self.latest_service["ip"],
                    "remote_ports": self.latest_service["ports"]
                }
        return None
