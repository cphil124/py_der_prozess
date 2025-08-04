import socket  # noqa: F401
import struct
from typing import Tuple

BYTE = bytes(1)


def construct_header_line(data: bytes):
    """
        Struct format codes:
        i: int              - 4 bytes
        h: short int        - 2 bytes
        b: signed char/int  - 1 byte

    """
    assert len(data) == 12
    message_size, api_key, api_version, correlation_id = struct.unpack('>ihhI', data)
    api_min_version = 0
    api_max_version = 4
    error_code = 0 if api_min_version <= api_version <= api_max_version else 0
    # return struct.pack('>iihb', message_size, correlation_id, error_code, api_key, api_version)
    return struct.pack('>2ihb3h', message_size, correlation_id, error_code, 3, api_key, api_min_version, api_max_version)


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")
    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092))
    sock, addr = server.accept() # wait for client
    data = sock.recv(1024)
    # print('raw data: ', data)
    
    # print(message_size, api_key, api_version, correlation_id)
    
    header = construct_header_line(data[:12])
    sock.sendall(header)


if __name__ == "__main__":
    main()
