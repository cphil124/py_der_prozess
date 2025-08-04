import socket  # noqa: F401
import struct
from threading import Thread
from typing import Tuple

TAG_BUFFER = b'\x00'

def construct_header_line(data: bytes) -> bytes:
    try:
        assert len(data) == 12
    except AssertionError as e:
        print(e)
    message_size, api_key, api_version, correlation_id = struct.unpack('>ihhI', data)
    api_min_version, api_max_version = 0, 4
    error_code = 0 if api_min_version <= api_version <= api_max_version else 35
    message = bytearray(correlation_id.to_bytes(4, byteorder='big'))
    message += error_code.to_bytes(2, byteorder='big')
    message += int(2).to_bytes(1, byteorder='big') # Array length is n + 1
    message += api_key.to_bytes(2, byteorder='big')
    message += api_min_version.to_bytes(2, byteorder='big')
    message += api_max_version.to_bytes(2, byteorder='big')
    message += TAG_BUFFER
    message += int(0).to_bytes(4, byteorder='big') # Throttle Time ms
    message += TAG_BUFFER
    message_leng = bytearray(len(message).to_bytes(4, byteorder='big'))
    return message_leng + message

def handle(sock: socket.socket):
    while True:
        data = sock.recv(1024)
        if not data:
            break
        print(data)
        header = construct_header_line(data[:12])
        sock.sendall(header)



def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092))
    try:
        while True:
            sock, addr = server.accept() #  wait for client
            thread = Thread(target=handle, args=(sock, ))
            thread.start()
    except KeyboardInterrupt:
        print('Interrupted by Keyboard')
        pass
    finally:
        pass


if __name__ == "__main__":
    main()
