import socket  # noqa: F401
import struct
from typing import Tuple

BYTE = bytes(1)


def construct_header_line(message_size:int, corr_id: int):
    # message_size = BYTE * 8
    # correlation_id_bytes = corr_id.to_bytes(8, byteorder='big')
    # return [message_size, correlation_id_bytes]
    return struct.pack('>ii', message_size, corr_id)


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092))
    sock, addr = server.accept() # wait for client
    data = sock.recv(1024)
    message_size, api_key, api_version, correlation_id = struct.unpack('>ihhI', data[:12])
    print(message_size, api_key, api_version, correlation_id)
    # print(type(data))
    # print(data)
    # print("raw_data:",data)
    # message_size = int.from_bytes(data[:4], byteorder='big')
    # api_key = int.from_bytes(data[4:6])
    # api_version = int.from_bytes(data[6:8])
    # correlation_id = int.from_bytes(data[8:12])
    # client_id = data[12:].decode('ascii')
    
    header = construct_header_line(4, correlation_id)
    sock.sendall(header)


if __name__ == "__main__":
    main()
