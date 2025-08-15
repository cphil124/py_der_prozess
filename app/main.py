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
NULL_BYTE = int(255).to_bytes(1)
NULL_TOPIC_ID = bytes(16)
CLUSTER_METADATA_FILE = '/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log'

# with open('/tmp/kraft-combined-logs/foo-0/partition.metadata', 'rb') as f:
#     print(f.read())

# with open('/tmp/kraft-combined-logs/foo-0/00000000000000000000.log', 'rb') as f:
#     print(f.read())

def debug_cursor(data: bytes, cur_beg: int, cur_end: int):
    print('Cursors: ', cur_beg, cur_end, data[cur_beg: cur_end], data[cur_beg-2: cur_end+2])

def advance_cursors(new_beginning:int, interval_length: int) -> tuple[int, int]:
    return (new_beginning, new_beginning + interval_length)

def decode_unsigned_beb128(byte_array: bytearray) -> tuple[int, int]:
    offset = 0
    end = False
    arr = []
    result = 0
    while not end:
        single_byte = byte_array[offset]
        # Check for continuing byte
        if single_byte & 0b10000000 == 0:
                # print(single_byte, 'Not a continuing byte')
                end = True
        # Remove the most significant bit
        single_byte = single_byte & 0b01111111
        arr.insert(0, single_byte)
        # Increment offset
        offset += 1
    result = 0
    for i in range(len(arr)):
        push_multiplier = (len(arr)-(i+1))
        elem = arr[i] << (7*push_multiplier)
        result |= elem
    return result, len(arr)

def decode_signed_beb128(byte_array: bytearray) -> tuple[int, int]:
    number, incr_amt = decode_unsigned_beb128(byte_array)
    if number % 2 == 0:
        return number // 2, incr_amt
    else:
        return -1 *( (number // 2) + 1), incr_amt

with codecs.open(CLUSTER_METADATA_FILE, 'r', 'quopri_codec') as f:
    data = f.read()


def encode_compact_string(string: str) -> bytes:
    str_length = len(string) + 1
    return struct.pack('>b', str_length) + struct.pack(f'>{str_length-1}s', string.encode())

topics = {}
topic_names_by_uuid = {}
topics_metadata = {}
metadata_records = []
cur_end, batch_length = 0, 999
while data:
    cur_beg, cur_end = 0, 61
    # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
    base_offset, batch_length, part_leader_epoch, magic_byte, crc, attrs, last_offset_delta, base_ts, max_ts, producer_id, producer_epoch, base_seq, records_length_count = struct.unpack('>qiibihiqqqhii', data[cur_beg:cur_end]) # type: ignore
    # print(list(zip(['base_offset', 'batch_length', 'part_leader_epoch', 'magic_byte', 'crc', 'attrs', 'last_offset_delta', 'base_ts', 'max_ts', 'producer_id', 'producer_epoch', 'base_seq', 'records_length_count'], struct.unpack('>qiibihiqqqhii', data[cur_beg:cur_end]))) ) # type: ignore
    cur_beg, cur_end = advance_cursors(cur_end, 1)
    # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
    # length_of_record = struct.unpack('>b', data[cur_beg:cur_end])[0] # type: ignore
    for record in range(records_length_count):
        length_of_record, incr_amt = decode_signed_beb128(bytearray(data[cur_beg:])) # type: ignore
        # print('length_of_record:', length_of_record)
        cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 1)
        # print(struct.unpack('>bbbb', data[cur_beg:cur_beg+4]))
        # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
        record_attrs = struct.unpack('>b', data[cur_beg:cur_end]) # type: ignore
        cur_beg, cur_end = advance_cursors(cur_end, 1)
        ts_delta, incr_amt = decode_signed_beb128(bytearray(data[cur_beg:])) # type: ignore
        cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 1)
        offset_delta, incr_amt = decode_signed_beb128(bytearray(data[cur_beg:])) # type: ignore
        cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 1)
        # print(list(zip(['attrs', 'ts_delta', 'offset_delta'], struct.unpack('>bbb', data[cur_beg:cur_end])))) # type: ignore
        # cur_beg, cur_end = advance_cursors(cur_end, 1)
        # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
        key_length, incr_amt = decode_signed_beb128(bytearray(data[cur_beg:]))  # type: ignore
        # print('key length:' , key_length)
        # print('Num Records: ', records_length_count)
        # print('Parsing New Record!')
        # Key Parsing
        # print('key_length ', key_length)
        if key_length > 0:
            cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, key_length)
            # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
            key = struct.unpack(f'>{key_length}s', data[cur_beg:cur_end]) # type: ignore
            # print('key:', key)
        else:
            # print('Key was NULL')
            cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 1)
        # cur_beg, cur_end = advance_cursors(cur_end, 1)
        # Value Parsing
        # print('DECODING THE VALUE!!!')
        value_length, incr_amt = decode_signed_beb128(data[cur_beg:]) # type: ignore
        # print('Value Length: ', value_length)
        # print(f'Advancing {get_increment_amount(value_length)} positions')
        # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_beg + value_length])
        cur_beg, cur_end = advance_cursors(cur_beg + incr_amt, 3)
        # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_beg + value_length])
        frame_version, record_type, version = struct.unpack('>3b', data[cur_beg:cur_end]) # type: ignore
        # print(list(zip(['frame_version', 'record_type', 'version'], struct.unpack('>3b', data[cur_beg:cur_end])))) # type: ignore
        cur_beg, cur_end = advance_cursors(cur_end, 1)
        if record_type == 12: # Feature Level Record
            # print('DECODING THE RECORD!!!')
            # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_beg + 10])
            name_length, incr_amt = decode_unsigned_beb128(bytearray(data[cur_beg:]))  # type: ignore
            # print('name_length: ', name_length)
            name_length -= 1
            cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, name_length+4)
            # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
            name, feature_level, tagged_fields_count, headers_array_count = struct.unpack(f'>{name_length}shbb', data[cur_beg:cur_end]) # type: ignore
            # print(list(zip(['name', 'feature_level', 'tagged_fields_count', 'headers_array_count'], [name, feature_level, tagged_fields_count, headers_array_count])))
            metadata_records.append(name.decode())
        elif record_type == 2: # Topic Record
            # cur_beg, cur_end = advance_cursors(cur_end, 1)
            name_length, incr_amt = decode_unsigned_beb128(bytearray(data[cur_beg:]))  # type: ignore
            # print('name_length: ', name_length)
            name_length -= 1
            cur_beg, cur_end = advance_cursors(cur_end, name_length+18)
            # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
            topic_name, topic_uuid, tagged_fields_count, headers_array_count = struct.unpack(f'>{name_length}s16sbb', data[cur_beg:cur_end]) # type: ignore
            topic_name = topic_name.decode()
            if not topics.get(topic_name):
                if not topics.get(topic_name):
                    topics[topic_name] = {
                        'id':topic_uuid,
                        'partitions': {}
                        } 
                topic_names_by_uuid[topic_uuid] = topic_name
            if not topics_metadata.get(topic_uuid):
                topics_metadata[topic_uuid] = {}
            # print('Topic Name: ', topic_name)
        elif record_type == 3: # Partition Record
            cur_beg, cur_end = advance_cursors(cur_beg, 20)
            partition_id, topic_uuid = struct.unpack(f'>i16s', data[cur_beg:cur_end]) # type: ignore
            partition_dict = {}

            cur_beg, cur_end = advance_cursors(cur_end,1)
            replica_array_length, incr_amt = decode_unsigned_beb128(data[cur_beg:]) # type: ignore
            replica_array_length -= 1 # The actual length is 1 less than the number bc it's a compact array
            cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 4*replica_array_length)
            replicas = struct.unpack(f'>{replica_array_length}i', data[cur_beg:cur_end]) # type: ignore

            cur_beg, cur_end = advance_cursors(cur_end, 1)
            in_sync_replica_array_length, incr_amt = decode_unsigned_beb128(data[cur_beg:]) # type: ignore
            in_sync_replica_array_length -= 1 # The actual length is 1 less than the number bc it's a compact array
            cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 4*in_sync_replica_array_length)
            in_sync_replicas = struct.unpack(f'>{in_sync_replica_array_length}i', data[cur_beg:cur_end]) # type: ignore
            
            cur_beg, cur_end = advance_cursors(cur_end, 1)
            removing_replicas_array_length, incr_amt = decode_unsigned_beb128(data[cur_beg:]) # type: ignore
            removing_replicas_array_length -= 1 # The actual length is 1 less than the number bc it's a compact array
            cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 4*removing_replicas_array_length)
            removing_replicas = struct.unpack(f'>{removing_replicas_array_length}i', data[cur_beg:cur_end]) # type: ignore

            cur_beg, cur_end = advance_cursors(cur_end, 1)
            adding_replica_array_length, incr_amt = decode_unsigned_beb128(data[cur_beg:]) # type: ignore
            adding_replica_array_length -= 1 # The actual length is 1 less than the number bc it's a compact array
            cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 4*adding_replica_array_length)
            adding_replicas = struct.unpack(f'>{adding_replica_array_length}i', data[cur_beg:cur_end]) # type: ignore

            cur_beg, cur_end = advance_cursors(cur_end, 12)
            replica_leader_id, leader_epoch, partition_epoch = struct.unpack(f'>3i', data[cur_beg:cur_end]) # type: ignore

            cur_beg, cur_end = advance_cursors(cur_end, 1)
            directories_array_length, incr_amt = decode_unsigned_beb128(data[cur_beg:]) # type: ignore
            directories_array_length -= 1 # The actual length is 1 less than the number bc it's a compact array
            cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 16*directories_array_length)
            directories = struct.unpack(f'>{"16s"*directories_array_length}', data[cur_beg:cur_end]) # type: ignore
            cur_beg, cur_end = advance_cursors(cur_end, 2)
            tagged_fields_count, headers_array_count = struct.unpack(f'>bb', data[cur_beg:cur_end]) # type: ignore

            partition_dict['replicas'] = replicas
            partition_dict['in_sync_replicas'] = replicas
            partition_dict['removing_replicas'] = replicas
            partition_dict['adding_replicas'] = replicas
            partition_dict['directories'] = replicas
            partition_dict['replica_leader_id'] = replica_leader_id
            partition_dict['leader_epoch'] = leader_epoch
            partition_dict['partition_epoch'] = partition_epoch
            partition_dict['data'] = []
            topic_name = topic_names_by_uuid[topic_uuid]
            topics[topic_name]['partitions'][partition_id] = partition_dict
        else: 
            # print(f'Found unknown record type: {record_type}')
            exit()
        cur_beg, cur_end = advance_cursors(cur_end, 1)
    data = data[cur_beg:]
# print(topics)
# print(topic_names_by_uuid)
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
        print(list(zip(['message_size', 'api_key', 'api_version', 'correlation_id'], [self.message_size, self.api_key, self.api_version, self.correlation_id])))
        self.client_name = ''
        self.error_code = 0

class KafkaFetchRequest(KafkaRequest):
    """
    Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id _tagged_fields 
    max_wait_ms => INT32
    min_bytes => INT32
    max_bytes => INT32
    isolation_level => INT8
    session_id => INT32
    session_epoch => INT32
    topics => topic_id [partitions] _tagged_fields 
        topic_id => UUID
        partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes _tagged_fields 
        partition => INT32
        current_leader_epoch => INT32
        fetch_offset => INT64
        last_fetched_epoch => INT32
        log_start_offset => INT64
        partition_max_bytes => INT32
    forgotten_topics_data => topic_id [partitions] _tagged_fields 
        topic_id => UUID
        partitions => INT32
    rack_id => COMPACT_STRING
    """
    def __init__(self, req_bytes: bytes):
        print(req_bytes)
        cur_beg, cur_end = 0, 12
        super().__init__(req_bytes=req_bytes[cur_beg:cur_end])
        self.reqd_topics = {}
        cur_beg, cur_end = advance_cursors(cur_end, 21)
        max_wait_ms, min_bytes, max_bytes, self.iso_lvl, self.session_id, self.session_epoch = struct.unpack('>3ib2i', req_bytes[cur_beg:cur_end])
        # print(list(zip(['max_wait_ms', 'min_bytes', 'max_bytes', 'iso_lvl', 'session_id', 'session_epoch'], [max_wait_ms, min_bytes, max_bytes, self.iso_lvl, self.session_id, self.session_epoch])))

        cur_beg, cur_end = advance_cursors(cur_end, 1)
        topic_array_length, incr_amt = decode_unsigned_beb128(bytearray(req_bytes[cur_beg:cur_end]))
        # print('topic_array_length: ', topic_array_length, 'incr_amt', incr_amt)
        # print('BEFORE TOPICS ARRAY')
        # debug_cursor(req_bytes, cur_beg, cur_end)
        cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 0)
        # print('BEFORE TOPICS ARRAY INCREMENTED')
        # debug_cursor(req_bytes, cur_beg, cur_end)

        for i in range(topic_array_length):
            cur_beg, cur_end = advance_cursors(cur_end, 16)
            topic_id = struct.unpack('>q', req_bytes[cur_beg:cur_end])
            cur_beg, cur_end = advance_cursors(cur_end, 1)
            partition_list = []
            part_array_length, incr_amt = decode_unsigned_beb128(bytearray(req_bytes[cur_beg:]))
            cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 0)
            for i in range(part_array_length):
                cur_beg, cur_end = advance_cursors(cur_end, 32)
                partition_index, current_leader_epoch, fetch_offset, last_fetched_offset, log_start_offset, partition_max_bytes, tagged_fields_count = struct.unpack('>iiqiqi', req_bytes[cur_beg:cur_end])
                part_dict = {
                    'partition_index': partition_index,
                    'current_leader_epoch': current_leader_epoch,
                    'fetch_offset': fetch_offset,
                    'last_fetched_offset': last_fetched_offset,
                    'log_start_offset':log_start_offset,
                    'partition_max_bytes': partition_max_bytes,
                    'tagged_fields_count': tagged_fields_count
                }
                partition_list.append(part_dict)
            cur_beg, cur_end = advance_cursors(cur_end, 0)
        print('AFTER TOPICS ARRAY')
        # debug_cursor(req_bytes, cur_beg, cur_end)
        forgotten_topics_array_length, incr_amt = decode_signed_beb128(bytearray(req_bytes[cur_beg:]))
        print('forgotten_topics_array_length: ', forgotten_topics_array_length, 'incr_amt', incr_amt)
        cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 0)
        self.forget_topics = {}
        for i in range(forgotten_topics_array_length):
            cur_beg, cur_end = advance_cursors(cur_end, 12)
            topic_id, partition = struct.unpack('>qi', req_bytes[cur_beg:cur_end])
            if self.forget_topics.get(topic_id):
                self.forget_topics[topic_id].append(partition)
            else:
                self.forget_topics[topic_id] = [partition]
            cur_beg, cur_end = advance_cursors(cur_end, 0)
        print('AFTER FORGOTTEN TOPICS ARRAY')
        # debug_cursor(req_bytes, cur_beg, cur_end)
        print(req_bytes[cur_beg:])
        rack_id_length, incr_amt = decode_unsigned_beb128(bytearray(req_bytes[cur_beg:]))
        rack_id_length -= 1
        cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, rack_id_length)
        print('Rack ID Length: ', rack_id_length)
        self.rack_id = struct.unpack(f'>{rack_id_length}s', req_bytes[cur_beg:cur_end])[0].decode()
        print('Rack ID: ', self.rack_id)


class KafkaApiVersionRequest(KafkaRequest):
    pass

class KafkaDescribePartitionsRequest(KafkaRequest):
    def __init__(self, req_bytes: bytes):
        cur_beg, cur_end = 0, 12
        super().__init__(req_bytes=req_bytes[cur_beg:cur_end])
        self.request_topics = []

        cur_beg, cur_end = advance_cursors(cur_end, 2)
        content_length = int(struct.unpack('>h', req_bytes[cur_beg:cur_end])[0])
        # print('content length', content_length)
        
        cur_beg, cur_end = advance_cursors(cur_end, content_length+1)
        self.client_name = struct.unpack(f'>{content_length}sx', req_bytes[cur_beg:cur_end])[0].decode()
        # print(self.client_name)

        cur_beg, cur_end = advance_cursors(cur_end, 1)
        array_length, incr_amt = decode_unsigned_beb128(bytearray(req_bytes[cur_beg:]))
        cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, 1)
        # print(array_length)

        for i in range(array_length-1):
            topic_name_leng, incr_amt = decode_unsigned_beb128(bytearray(req_bytes[cur_beg:]))
            cur_beg, cur_end = advance_cursors(cur_beg+incr_amt, topic_name_leng)
            # print('topic_name_leng', topic_name_leng, ' incr_amount', incr_amt)
            # print('Cursors:', cur_beg, cur_end, data[cur_beg:cur_end])
            topic_name = struct.unpack(f'>{topic_name_leng-1}sx', req_bytes[cur_beg:cur_end])[0]
            # print('Topic Name:', topic_name)
            self.request_topics.append(topic_name.decode())
            cur_beg, cur_end = advance_cursors(cur_end, 0)
        cur_beg, cur_end = advance_cursors(cur_end, 6)
        # print(cur_beg, cur_end, len(req_bytes[cur_beg:]), req_bytes[cur_beg:])
        self.response_limit, page_cursor = struct.unpack('>ibx', req_bytes[cur_beg:cur_end])
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
    

class KafkaFetchResponse(KafkaResponse):
    API_KEY = 1
    MIN_VERSION = 0
    MAX_VERSION = 16
    def __init__(self, req: KafkaFetchRequest):
        super().__init__(req)
        self.session_id = req.session_id
        self.reqd_topics = req.reqd_topics
        self.forget_topics = req.forget_topics

    def read_from_partition(self, topic_id: str, partition_id: int):
        pass
    def construct_response_message(self) -> bytes:
        """
            Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] _tagged_fields 
            throttle_time_ms => INT32
            error_code => INT16
            session_id => INT32
            responses => topic_id [partitions] _tagged_fields 
                topic_id => UUID
                partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records _tagged_fields 
                    partition_index => INT32
                    error_code => INT16
                    high_watermark => INT64
                    last_stable_offset => INT64
                    log_start_offset => INT64
                    aborted_transactions => producer_id first_offset _tagged_fields 
                        producer_id => INT64
                        first_offset => INT64
                    preferred_read_replica => INT32
                    records => COMPACT_RECORDS
        """
        print('Correlation ID:', self.correlation_id.to_bytes(4, byteorder='big'))
        message = bytearray(self.correlation_id.to_bytes(4, byteorder='big'))
        message += int(0).to_bytes(4, byteorder='big')
        message += TAG_BUFFER
        print('Error Code: ', self.error_code, self.error_code.to_bytes(2, byteorder='big'))
        message += self.error_code.to_bytes(2, byteorder='big')
        message += self.session_id.to_bytes(4, byteorder='big')
        print('Reqd Topics: ', len(self.reqd_topics))
        message += int(0).to_bytes(1, byteorder='big')
        for topic_id, partitions in self.reqd_topics.items():
            message += topic_id
            for part_dict in partitions:
                message += part_dict['partition_index'].to_bytes(4, byteorder='big')
                message += int(0).to_bytes(2, byteorder='big') # Error Code
                message += int(0).to_bytes(8, byteorder='big') # High Watermark
                message += int(0).to_bytes(8, byteorder='big') # Last Stable Offset
                message += int(0).to_bytes(8, byteorder='big') # Log Start Offset
                message += int(0).to_bytes(4, byteorder='big') # Aborted Trransactions Array Length
                for topic in []:
                    pass
                message += int(0).to_bytes(4, byteorder='big') # Preferred Read Replica
                message += int(0).to_bytes(1, byteorder='big') # Preferred Read Replica
        message += TAG_BUFFER
        message_len = len(message)
        message_leng = bytearray(message_len.to_bytes(4, byteorder='big'))
        print('message_leng:', message_len)
        ret_message = message_leng + message
        for i in range(0, len(ret_message), 4):
            print(ret_message[i:i+4])
        return ret_message

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
        self.response_limit = req.response_limit
            
    def construct_response_message(self) -> bytes:
        message = bytearray(self.correlation_id.to_bytes(4, byteorder='big'))
        message += TAG_BUFFER
        message += bytes(4) # Throttle Time
        message += int(len(self.requested_topics)+1).to_bytes(1, byteorder='big') # Needs to be encoded as varint
        for topic_name, error_code in self.topic_errors.items():
            topic_description = bytearray(error_code.to_bytes(2, byteorder='big'))
            topic_description += int(len(topic_name)+1).to_bytes(1, byteorder='big') # Needs to be encoded as varint
            topic_description += topic_name.encode('utf-8')
            topic_id = topics[topic_name]['id'] if topics.get(topic_name) else NULL_TOPIC_ID
            # print('topic_id ', topic_id)
            topic_description += topic_id
            topic_description += False.to_bytes(1)
            if error_code == 3:
                topic_description += int(1).to_bytes(1, byteorder='big')
            else:
                partitions_array_length = len(topics[topic_name]['partitions'])+1 
                
                partition_array = bytearray(partitions_array_length.to_bytes(1, byteorder='big')) # Encode as varint
                for part_id, part in topics[topic_name]['partitions'].items():
                    partition_array += int(0).to_bytes(2, byteorder='big')
                    partition_array += part_id.to_bytes(4, byteorder='big')
                    partition_array += part['replica_leader_id'].to_bytes(4, byteorder='big')
                    partition_array += part['leader_epoch'].to_bytes(4, byteorder='big')
                    replicas = part['replicas']
                    partition_array += int(len(replicas)+1).to_bytes(1, byteorder='big') # Encode as varint
                    for replica in replicas:
                        partition_array += replica.to_bytes(4, byteorder='big')

                    replicas = part['in_sync_replicas']
                    partition_array += int(len(replicas)+1).to_bytes(1, byteorder='big') # Encode as varint
                    for replica in replicas:
                        partition_array += replica.to_bytes(4, byteorder='big')

                    replicas = part['adding_replicas']
                    partition_array += int(len(replicas)+1).to_bytes(1, byteorder='big') # Encode as varint
                    for replica in replicas:
                        partition_array += replica.to_bytes(4, byteorder='big')

                    replicas = part['replicas']
                    partition_array += int(len(replicas)+1).to_bytes(1, byteorder='big') # Encode as varint
                    for replica in replicas:
                        partition_array += replica.to_bytes(4, byteorder='big')

                    replicas = part['removing_replicas']
                    partition_array += int(len(replicas)+1).to_bytes(1, byteorder='big') # Encode as varint
                    for replica in replicas:
                        partition_array += replica.to_bytes(4, byteorder='big')
                    partition_array += TAG_BUFFER
                topic_description += partition_array
            topic_description += b'\x00\x00\x0d\xf8'
            topic_description += TAG_BUFFER
            message += topic_description
        message += NULL_BYTE
        message += TAG_BUFFER
        message_leng = bytearray(len(message).to_bytes(4, byteorder='big'))
        ret_message = message_leng + message 
        # print(ret_message.hex())
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
    75: ApiType(75, KafkaDescribePartitionsRequest, KafkaDescribeTopicPartitionsResponse, 0, 0),
    1: ApiType(1, KafkaFetchRequest, KafkaFetchResponse, 0, 16)

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
    

def request_parser(data: bytes) -> KafkaRequest:
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
