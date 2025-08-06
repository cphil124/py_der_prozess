import socket  # noqa: F401
import struct
from abc import ABC
from threading import Thread
from typing import Tuple
# from constants import *

TAG_BUFFER = b'\x00'

class KafkaRequest:
    def __init__(self, req_bytes: bytes):
        self.message_size, self.api_key, self.api_version, self.correlation_id = struct.unpack('>ihhI', req_bytes[0:12])
        print('Message Received', self.message_size, self.api_key, self.api_version, self.correlation_id)
        self.error_code = 0

class KafkaResponse(ABC):
    API_KEY = -1
    MIN_VERSION = 0
    MAX_VERSION = 0
    def __init__(self, req: KafkaRequest):
        self.correlation_id = req.correlation_id
        self.api_version = req.api_version
        self.error_code = 0

    @classmethod
    def get_api_versions(cls) -> bytearray:
        ver_arr = bytearray(cls.API_KEY.to_bytes(2, byteorder='big'))
        ver_arr += bytearray(cls.MIN_VERSION.to_bytes(2, byteorder='big'))
        ver_arr += bytearray(cls.MAX_VERSION.to_bytes(2, byteorder='big'))
        print(ver_arr)
        return ver_arr

    def construct_response_message(self) -> bytes:
        return b''

class KafkaAPIVersionsResponse(KafkaResponse):
    API_KEY = 18
    MIN_VERSION = 0
    MAX_VERSION = 4

    def construct_response_message(self) -> bytes:
        message = bytearray(self.correlation_id.to_bytes(4, byteorder='big'))
        message += self.error_code.to_bytes(2, byteorder='big')
        message += int(len(RESPONSE_TYPES)+1).to_bytes(1, byteorder='big') # Array length is n + 1
        for _, typ in RESPONSE_TYPES.items():
            message += typ.get_api_versions()
            message += TAG_BUFFER
        message += int(0).to_bytes(4, byteorder='big') # Throttle Time ms
        message += TAG_BUFFER
        message_leng = bytearray(len(message).to_bytes(4, byteorder='big'))
        ret_message = message_leng + message
        print(ret_message)
        return ret_message

class KafkaDescribeTopicPartitionsResponse(KafkaResponse):
    API_KEY = 75
    MIN_VERSION = 0
    MAX_VERSION = 0


RESPONSE_TYPES = {
    18: KafkaAPIVersionsResponse,
    75: KafkaDescribeTopicPartitionsResponse
}

def create_response(req: KafkaRequest) -> KafkaResponse:
    error_code = 0
    if response_type := RESPONSE_TYPES[req.api_key]:
        response = response_type(req)
        print(f'Api Type: {type(response)}; Request API Version: {req.api_version}; API Min Version: {response.MIN_VERSION}; API Max Version: {response.MAX_VERSION};')
        if not response_type.MIN_VERSION < req.api_version < response_type.MAX_VERSION:
            response.error_code = 35
        print(f'Error Code: {response.error_code}')
        return response
    else:
        return KafkaResponse(req)


def handle(sock: socket.socket):
    while True:
        data = sock.recv(1024)
        if not data:
            break
        print(data)
        request = KafkaRequest(data)
        response = create_response(request)
        if response.API_KEY == -1:
            continue
        response_message = response.construct_response_message()
        sock.sendall(response_message)



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
