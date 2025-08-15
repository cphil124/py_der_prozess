from abc import ABC
import io
import struct
from typing import TypeVar, Generic
byts = b'\x00\x00\x000\x00\x01\x00\x10\x10mlq\x00\x0ckafka-tester\x00\x00\x00\x01\xf4\x00\x00\x00\x01\x03 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x01\x01\x00'

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
    

T = TypeVar('T')

# KAFKA PRIMITIVES
class KafkaPrimitive(ABC): pass
class KafkaFormat(ABC): FORMAT_LIST: list[KafkaPrimitive]
class KafkaCollectionPrimitive[KafkaFormat](KafkaPrimitive):  pass
class Int8(KafkaPrimitive): pass
class Int16(KafkaPrimitive): pass
class UInt16(KafkaPrimitive): pass
class Int32(KafkaPrimitive): pass
class UInt32(KafkaPrimitive): pass
class Int64(KafkaPrimitive): pass
class KBoolean(KafkaPrimitive): pass
class KUUID(KafkaPrimitive): pass
class SVARINT(KafkaPrimitive): pass
class UVARINT(KafkaPrimitive): pass
class STRING(KafkaPrimitive): pass
class COMPACT_STRING(KafkaPrimitive): pass
class ARRAY(KafkaCollectionPrimitive[KafkaPrimitive]): pass
class COMPACT_ARRAY(KafkaCollectionPrimitive[KafkaPrimitive]): pass
class BYTES(KafkaPrimitive): pass
class RECORD(KafkaCollectionPrimitive[KafkaPrimitive]): pass

HEADER_FORMAT = [Int32, Int16, Int16, Int32]
class HEADER_RECORD[HEADER_FORMAT](RECORD): FORMAT_LIST = 











INT8 = 'INT8'
INT16 = 'INT16'
UINT16 = 'UINT16'
INT32 = 'INT32'
UINT32 = 'UINT32'
INT64 = 'INT64'
BOOLEAN = 'BOOLEAN'
UUID = 'UUID'
SVARINT = 'SVARINT'
UVARINT = 'UVARINT'
STRING = 'STRING'
COMPACT_STRING = 'COMPACT_STRING'
ARRAY = 'ARRAY' # Figure out a way to parameterize this generically so that it can take a format as a parameter and parse accordingly
COMPACT_ARRAY = 'COMPACT_ARRAY'
BYTES = 'BYTES'
RECORDS = 'RECORDS'

ARRAY_LIKES = [ARRAY, COMPACT_ARRAY, RECORDS] 

FETCH_REQUEST_FORMAT = [INT32, INT32, INT32, INT8, INT32, INT32, ARRAY[FETCH_REQUEST_TOPIC_FORMAT], ARRAY[FETCH_REQUEST_PARTITION_FORMAT], COMPACT_STRING]
FETCH_REQUEST_TOPIC_FORMAT = [UUID, ARRAY[FETCH_REQUEST_PARTITION_FORMAT]]
FETCH_REQUEST_PARTITION_FORMAT = [INT32, INT32, INT64, INT32, INT64, INT32]

class BytesParser(ABC):
    
    def __init__(self):
        self.cursor = 0
        self.FORMAT_MAP = {
            INT8: self.decode_int8,
            INT16: self.decode_int16,
            UINT16: self.decode_uint16,
            INT32: self.decode_int32,
            UINT32: self.decode_uint32,
            INT64: self.decode_int64,
            BOOLEAN: self.decode_boolean,
            UUID: self.decode_uuid,
            SVARINT: decode_signed_beb128,
            UVARINT: decode_unsigned_beb128,
            STRING: self.decode_string,
            COMPACT_STRING: self.decode_string,
            ARRAY: self.decode_array,
            COMPACT_ARRAY: self.decode_compact_array,
            BYTES: self.decode_bytes
        }

    def advance_cursors(self, new_beginning:int, interval_length: int) -> None:
        self.cur_beg = new_beginning
        self.cur_end = new_beginning + interval_length

    def _decode_header(self, data: bytes) -> tuple[int, int, int, int]:
        # Read message size, correlation_id, tag_buffer
        ret_tuple = self.decode_format(data, HEADER_FORMAT)
        assert len(ret_tuple) == 4
        return ret_tuple # pyright: ignore[reportReturnType]
    
    def decode_format(self, data: bytes, format_list: list[str]) -> list:
        ret_list = []
        for f in format_list:
            if f in format_list:
                ret_list.append(self.FORMAT_MAP[f](data))
        return ret_list
    
    def decode_bytes(self, data:bytes):
        length = self.decode_int32(data)
        return struct.unpack(f'>{length}s', data[self.cursor: self.cursor+length])[0]
    
    def decode_compact_bytes(self, data:bytes):
        length = self.decode_unsigned_varint(data)
        length -= 1
        return struct.unpack(f'>{length}s', data[self.cursor: self.cursor+length])[0]

    def decode_compact_string(self, data:bytes):
        length = self.decode_unsigned_varint(data)
        ret_string: bytes = struct.unpack(f'>{length}s', data[self.cursor:(self.cursor+length-1)])[0]
        self.cursor += (length-1)
        return ret_string.decode()
    
    def decode_string(self, data:bytes):
        length = self.decode_int16(data)
        ret_string: bytes = struct.unpack(f'>{length}s', data[self.cursor:(self.cursor+length-1)])[0]
        self.cursor += length
        return ret_string.decode()
    
    def decode_boolean(self, data: bytes) -> bool:
        ret = struct.unpack('>?', data[self.cursor:self.cursor+1])[0]
        self.cursor += 1
        return ret

    def decode_array(self, data:bytes, t: type):
        length = self.decode_int32(data)
        arr = struct.unpack(f'>{length}s', data[self.cursor : self.cursor+length])[0]
        return arr

    def decode_compact_array(self, data:bytes, t: type):
        length = self.decode_unsigned_varint(data)
        length -= 1
        arr = struct.unpack(f'>{length}s', data[self.cursor : self.cursor+length])[0]
        return arr

    def decode_uuid(self, data:bytes) -> bytes:
        val: bytes = struct.unpack('>16s', data[self.cursor : self.cursor+16])[0]
        self.cursor += 16
        return val

    def decode_int8(self, data:bytes) -> int:
        val: int = struct.unpack('>b', data[self.cursor : self.cursor+1])[0]
        self.cursor += 1
        return val
    
    def decode_int16(self, data:bytes) -> int:
        val: int = struct.unpack('>h', data[self.cursor : self.cursor+2])[0]
        self.cursor += 2
        return val
    
    def decode_uint16(self, data:bytes) -> int:
        val: int = struct.unpack('>H', data[self.cursor : self.cursor+2])[0]
        self.cursor += 2
        return val
    
    def decode_int32(self, data:bytes) -> int:
        val: int = struct.unpack('>i', data[self.cursor : self.cursor+4])[0]
        self.cursor += 4
        return val
    
    def decode_uint32(self, data:bytes) -> int:
        val: int = struct.unpack('>I', data[self.cursor : self.cursor+4])[0]
        self.cursor += 4
        return val
    
    def decode_int64(self, data:bytes) -> int:
        val: int = struct.unpack('>q', data[self.cursor : self.cursor+8])[0]
        self.cursor += 8
        return val
    
    def decode_unsigned_varint(self, data:bytes) -> int:
        num, incr_amt = decode_unsigned_beb128(bytearray(data[self.cursor:]))
        self.cursor += incr_amt
        return num

    def decode_signed_varint(self, data:bytes) -> int:
        num, incr_amt = decode_signed_beb128(bytearray(data[self.cursor:]))
        self.cursor += incr_amt
        return num

parser = BytesParser()
parser._decode_header(byts)