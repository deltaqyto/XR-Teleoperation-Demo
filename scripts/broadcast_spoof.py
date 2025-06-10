import socket
import json
import time


def main():
    # Fake service data
    service_data = {
        "service": "XR Quest",
        "ip": "192.168.1.100",
        "ports": {
            "http": 8080,
            "websocket": 8081,
            "stream": 9001,
            "control": 9002
        }
    }

    # Set up UDP broadcast socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    print("Broadcasting fake XR Quest service...")
    print(f"Data: {service_data}")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            message = json.dumps(service_data).encode('utf-8')
            sock.sendto(message, ('<broadcast>', 9999))
            print("Broadcast sent")
            time.sleep(3)

    except KeyboardInterrupt:
        print("\nStopped broadcasting")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
