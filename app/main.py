import socket  # noqa: F401
import struct

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
    
    chunk = sock.recv(2048)
    print(type(chunk))
    decoded = chunk.decode()
    print(type(decoded))
    print(decoded)
    header = construct_header_line(4, 7)
    sock.sendall(header)


if __name__ == "__main__":
    main()
