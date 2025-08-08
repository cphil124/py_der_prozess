import socket  # noqa: F401
import struct
import codecs
import json
from encodings import aliases
import quopri
from abc import ABC
from threading import Thread
from typing import Tuple, Callable
from collections import namedtuple
# from constants import *

TAG_BUFFER = b'\x00'
NULL_TOPIC_ID = bytes(16)
CLUSTER_METADATA_FILE = '/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log'


def advance_cursors(cur_end:int, incr: int) -> tuple[int, int]:
    return (cur_end, cur_end + incr)

with codecs.open('/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log', 'r', 'quopri_codec') as f:
    data = f.read()

with open('/tmp/kraft-combined-logs/__cluster_metadata-0/partition.metadata', 'r') as f:
    print('partition.metadata file: ', f.read())
    
metadata_records = []
cur_end, batch_length = 0, 999
while data:
    cur_beg, cur_end = 0, 61
    base_offset, batch_length, part_leader_epoch, magic_byte, crc, attrs, last_offset_delta, base_ts, max_ts, producer_id, producer_epoch, base_seq, records_length_count = struct.unpack('>qiibihiqqqhii', data[cur_beg:cur_end]) # type: ignore
    print('base_offset', 'batch_length', 'part_leader_epoch', 'magic_byte', 'crc', 'attrs', 'last_offset_delta', 'base_ts', 'max_ts', 'producer_id', 'producer_epoch', 'base_seq', 'records_length_count')
    print(base_offset, batch_length, part_leader_epoch, magic_byte, crc, attrs, last_offset_delta, base_ts, max_ts, producer_id, producer_epoch, base_seq, records_length_count)
    # print(len(data))
    print(base_offset, batch_length, producer_id, records_length_count)
    # cur_beg = cur_end
    # cur_end += 1
    print(data[0:61])
    print(data[61:75])
    cur_beg, cur_end = advance_cursors(cur_end, 1)
    print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
    length_of_record = struct.unpack('>b', data[cur_beg:cur_end])[0] # type: ignore
    print('length_of_record:', length_of_record)
    cur_beg, cur_end = advance_cursors(cur_end, 4)
    # print(struct.unpack('>bbbb', data[cur_beg:cur_beg+4]))
    print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
    attrs, ts_delta, offset_delta, key_length = struct.unpack('>bbbb', data[cur_beg:cur_end]) # type: ignore
    print('attrs', 'ts_delta', 'offset_delta', 'key_length')
    print(attrs, ts_delta, offset_delta, key_length)
    cur_beg = cur_end
    # print('Num Records: ', records_length_count)
    for record in range(records_length_count):
        # Key Parsing
        if key_length-1 > 0:
            cur_end += key_length
            # key = struct.unpack(f'>{key_length}s', data[cur_beg:cur_end]) # type: ignore
            print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
            key = struct.unpack(f'>{key_length}b', data[cur_beg:cur_end])[0] # type: ignore
            print('key:', key)
        cur_beg, cur_end = advance_cursors(cur_end, 4)
        # Value Parsing
        # value_length = struct.unpack('>b', data[cur_beg:cur_end]) # type: ignore
        print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
        value_length, frame_version, record_type, version = struct.unpack('>4b', data[cur_beg:cur_end]) # type: ignore
        print('value_length', 'frame_version', 'record_type', 'version')
        print(value_length, frame_version, record_type, version)
        if record_type == 12: # Feature Level Record
            cur_beg, cur_end = advance_cursors(cur_end, 1)
            name_length = struct.unpack('>b', data[cur_beg:cur_end])[0] # type: ignore
            print('name_length: ', name_length)
            cur_beg, cur_end = advance_cursors(cur_end, name_length+3)
            print(cur_end - cur_beg)
            print('name_length:',  name_length)
            print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
            name, feature_level, tagged_fields_count, headers_array_count = struct.unpack(f'>{name_length-1}shbb', data[cur_beg:cur_end]) # type: ignore
            print(name, feature_level, tagged_fields_count, headers_array_count)
            metadata_records.append(name.decode())
        elif record_type == 2: # Topic Record
            cur_beg, cur_end = advance_cursors(cur_end, 1)
            name_length = struct.unpack('>b', data[cur_beg:cur_end])[0] # type: ignore
            print('name_length: ', name_length)
            name_length -= 1
            cur_beg, cur_end = advance_cursors(cur_end, name_length+17)
            print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
            topic_name, topic_uuid, tagged_fields_count = struct.unpack(f'>{name_length}s16sb', data[cur_beg:cur_end]) # type: ignore
        elif record_type == 3: # Partition Record
            cur_beg, cur_end = advance_cursors(cur_end, 21)
            partition_id, topic_uuid, replica_array_length = struct.unpack(f'>i16sb', data[cur_beg:cur_end]) # type: ignore
            replica_array_length -= 1 # The actual length is 1 less than the number bc it's a compact array
            cur_beg, cur_end = advance_cursors(cur_end, 4*replica_array_length)
            replicas = struct.unpack(f'>{replica_array_length}i', data[cur_beg:cur_end]) # type: ignore

            cur_beg, cur_end = advance_cursors(cur_end, 1)
            in_sync_replica_array_length = struct.unpack(f'>b', data[cur_beg:cur_end])[0] # type: ignore
            in_sync_replica_array_length -= 1 # The actual length is 1 less than the number bc it's a compact array
            cur_beg, cur_end = advance_cursors(cur_end, 4*in_sync_replica_array_length)
            in_sync_replicas = struct.unpack(f'>{in_sync_replica_array_length}i', data[cur_beg:cur_end]) # type: ignore
            
            cur_beg, cur_end = advance_cursors(cur_end, 1)
            removing_replicas_array_length = struct.unpack(f'>b', data[cur_beg:cur_end])[0] # type: ignore
            removing_replicas_array_length -= 1 # The actual length is 1 less than the number bc it's a compact array
            cur_beg, cur_end = advance_cursors(cur_end, 4*removing_replicas_array_length)
            removing_replicas = struct.unpack(f'>{removing_replicas_array_length}i', data[cur_beg:cur_end]) # type: ignore

            cur_beg, cur_end = advance_cursors(cur_end, 1)
            adding_replica_array_length = struct.unpack(f'>b', data[cur_beg:cur_end])[0] # type: ignore
            adding_replica_array_length -= 1 # The actual length is 1 less than the number bc it's a compact array
            cur_beg, cur_end = advance_cursors(cur_end, 4*adding_replica_array_length)
            adding_replicas = struct.unpack(f'>{adding_replica_array_length}i', data[cur_beg:cur_end]) # type: ignore

            cur_beg, cur_end = advance_cursors(cur_end, 13)
            replica_leader_id, leader_epoch, partition_epoch, directories_array_length = struct.unpack(f'>3ib', data[cur_beg:cur_end]) # type: ignore
            directories_array_length -= 1 # The actual length is 1 less than the number bc it's a compact array
            cur_beg, cur_end = advance_cursors(cur_end, 16*directories_array_length)
            directories = struct.unpack(f'>{"16s"*directories_array_length}', data[cur_beg:cur_end]) # type: ignore
            cur_beg, cur_end = advance_cursors(cur_end, 1)
            tagged_fields_count = struct.unpack(f'>b', data[cur_beg:cur_end])[0] # type: ignore

        data = data[cur_end:]
print(metadata_records)

topics = {'NULL': NULL_TOPIC_ID}

"""
    API Version Request Header:
        - API Key
        - API Version
        - Correlation ID
        - Client ID
        - Tag Buffer
    API Version Request Header:
        - Correlation ID
        - Tag Buffer (Only v1, v0 has no buffer)

    DescribePartition Request Header:
        - API Key
        - API Version
        - Correlation ID
        - Client ID
        - Tag Buffer
    DescribePartition Response Header:
        - Correlation ID
        - Tag Buffer
"""

def response_wrapper(func: Callable, *args, **kwargs) -> Callable:
    def wrapper():
        message = func(args[0])
        message_len = len(message).to_bytes(4)
        return message_len + message + TAG_BUFFER
    return wrapper

class KafkaRequest(ABC):
    def __init__(self, req_bytes: bytes):
        self.message_size, self.api_key, self.api_version, self.correlation_id = struct.unpack('>ihhI', req_bytes[0:12])
        # print('Message Received', self.message_size, self.api_key, self.api_version, self.correlation_id)
        self.client_name = ''
        self.error_code = 0

class KafkaApiVersionRequest(KafkaRequest):
    pass

class KafkaDescribePartitionsRequest(KafkaRequest):
    def __init__(self, req_bytes: bytes):
        cur_beg = 0
        cur_end = 12
        super().__init__(req_bytes=req_bytes[cur_beg:cur_end])
        self.request_topics = []

        cur_beg = cur_end
        cur_end += 2
        content_length = int(struct.unpack('>h', req_bytes[cur_beg:cur_end])[0])
        
        # print('content length', content_length)
        cur_beg += 2
        cur_end = cur_beg + content_length + 1
        self.client_name = struct.unpack(f'>{content_length}sx', req_bytes[cur_beg:cur_end])[0].decode()
        # print(self.client_name)

        cur_beg = cur_end
        cur_end += 1
        # print(cur_beg, cur_end)
        array_length = struct.unpack('>b', req_bytes[cur_beg: cur_end])[0]
        # print(array_length)

        for i in range(array_length-1):
            cur_beg = cur_end
            cur_end += 1
            topic_name_leng = int(struct.unpack('>b', req_bytes[cur_beg:cur_end])[0])
            # print('topic_name_leng', topic_name_leng)
            cur_beg += 1
            cur_end += topic_name_leng
            # print(cur_beg, cur_end)
            topic_name = struct.unpack(f'>{topic_name_leng-1}sx', req_bytes[cur_beg:cur_end])[0].decode()
            # print(topic_name)
            # print('Topic Name:', topic_name)
            self.request_topics.append(topic_name)
        cur_beg = cur_end
        cur_end += 6
        # print(cur_beg, cur_end, len(req_bytes[cur_beg:]), req_bytes[cur_beg:])
        response_limit, page_cursor = struct.unpack('>ibx', req_bytes[cur_beg:cur_end])
        # print(self.request_topics)

class KafkaResponse(ABC):
    API_KEY = -1
    MIN_VERSION = 0
    MAX_VERSION = 0
    def __init__(self, req: KafkaRequest):
        self.correlation_id = req.correlation_id
        self.api_version = req.api_version
        self.client_name = req.client_name
        self.error_code = 0

    @classmethod
    def get_api_versions(cls) -> bytearray:
        ver_arr = bytearray(cls.API_KEY.to_bytes(2, byteorder='big'))
        ver_arr += bytearray(cls.MIN_VERSION.to_bytes(2, byteorder='big'))
        ver_arr += bytearray(cls.MAX_VERSION.to_bytes(2, byteorder='big'))
        return ver_arr
    
    def construct_response_message(self) -> bytes:
        return b''

class KafkaAPIVersionsResponse(KafkaResponse):
    API_KEY = 18
    MIN_VERSION = 0
    MAX_VERSION = 4

    # @response_wrapper
    def construct_response_message(self) -> bytes:
        message = bytearray(self.correlation_id.to_bytes(4, byteorder='big'))
        message += self.error_code.to_bytes(2, byteorder='big')
        message += int(len(API_TYPES)+1).to_bytes(1, byteorder='big') # Array length is n + 1
        for _, typ in API_TYPES.items():
            message += typ.response.get_api_versions()
            message += TAG_BUFFER
        message += int(0).to_bytes(4, byteorder='big') # Throttle Time ms
        message += TAG_BUFFER
        message_leng = bytearray(len(message).to_bytes(4, byteorder='big'))
        ret_message = message_leng + message
        return ret_message

class KafkaDescribeTopicPartitionsResponse(KafkaResponse):
    API_KEY = 75
    MIN_VERSION = 0
    MAX_VERSION = 0
    

    def __init__(self, req: KafkaDescribePartitionsRequest):
        super().__init__(req)
        self.requested_topics = req.request_topics
        self.topic_errors = {topic: (0 if topic in topics else 3)  for topic in req.request_topics}
            


    # @response_wrapper
    def construct_response_message(self) -> bytes:
        response_limit = 100
        message = bytearray(self.correlation_id.to_bytes(4, byteorder='big'))
        message += TAG_BUFFER
        message += bytes(4) # Throttle Time
        message += int(len(self.requested_topics)+1).to_bytes(1, byteorder='big')
        for topic_name, error_code in self.topic_errors.items():
            topic_description = bytearray(error_code.to_bytes(2, byteorder='big'))
            topic_description += int(len(topic_name)+1).to_bytes(1, byteorder='big')
            topic_description += topic_name.encode('utf-8')
            topic_id = topics[topic_name] if topics.get(topic_name) else NULL_TOPIC_ID
            topic_description += topic_id
            topic_description += TAG_BUFFER
            partitions_array_length = 1
            topic_description += partitions_array_length.to_bytes(1, byteorder='big')
            topic_description += bytes(4)
            topic_description += TAG_BUFFER
            message += topic_description
        message += int(255).to_bytes(1, byteorder='big')
        message += TAG_BUFFER
        message_leng = bytearray(len(message).to_bytes(4, byteorder='big'))
        ret_message = message_leng + message 
        # print(ret_message)
        return ret_message



#TODO: Refactor to have a catalog of some sort. Extract the version min/max from the response, and decouple the request from the response.
class ApiType:
    def __init__(self, apikey:int, req: type[KafkaRequest], resp: type[KafkaResponse], min_version: int, max_version: int):
        self.api_key = apikey
        self.request = req
        self.response = resp
        self.min_version = min_version
        self.max_version = max_version


API_TYPES = {
    18: ApiType(18, KafkaApiVersionRequest, KafkaAPIVersionsResponse, 0, 4),
    75: ApiType(75, KafkaDescribePartitionsRequest, KafkaDescribeTopicPartitionsResponse, 0, 0)
}

#TODO: Decouple from KafkaRequest
def create_response(req: KafkaRequest) -> KafkaResponse:
    error_code = 0
    if api_type := API_TYPES[req.api_key]:
        response = api_type.response(req)
        # print(f'Api Type: {type(response)}; Request API Version: {req.api_version}; API Min Version: {response.MIN_VERSION}; API Max Version: {response.MAX_VERSION};')
        if not api_type.response.MIN_VERSION <= req.api_version <= api_type.response.MAX_VERSION:
            response.error_code = 35
        return response
    else:
        return KafkaResponse(req)
    

def request_parser(data:bytes) -> KafkaRequest:
    api_version = int(struct.unpack('>h', data[4:6])[0])
    if api_type := API_TYPES.get(api_version):
        return api_type.request(data)
    else:
        return KafkaRequest(TAG_BUFFER)


def handle(sock: socket.socket):
    while True:
        data = sock.recv(1024)
        if not data:
            break
        request = request_parser(data)
        response = create_response(request)
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
