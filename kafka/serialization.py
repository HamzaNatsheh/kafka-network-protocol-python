from typing import Callable, List, TypeVar
from uuid import UUID

from bitstring import BitStream

import util.inspection
import util.numbers

T = TypeVar("T")


# Implementations for serialization of Kafka protocol primitives
# https://kafka.apache.org/protocol.html#protocol_types


def write_uint_8(val: int, stream: BitStream): stream.append(f"uint:8={val}")


def read_uint_8(stream: BitStream) -> int: return stream.read("uint:8")


# Represents a boolean value in a byte. Values 0 and 1 are used to represent false and true respectively. When
# reading a boolean value, any non-zero value is considered true.
def write_boolean(val: bool, stream: BitStream):
    if val:
        write_uint_8(1, stream)
    else:
        write_uint_8(0, stream)


def read_boolean(stream: BitStream) -> bool:
    return read_uint_8(stream) != 0


# Represents an integer between -2^15 and 2^15-1 inclusive. The values are encoded using two bytes in network byte
# order (big-endian).
def write_int_16(val: int, stream: BitStream): stream.append(f"int:16={val}")


def read_int_16(stream: BitStream) -> int: return stream.read("int:16")


# Represents an integer between -2^(31) and 2^(31-1) inclusive. The values are encoded using four bytes in network byte
# order (big-endian).
def write_int_32(val: int, stream: BitStream): stream.append(f"int:32={val}")


def read_int_32(stream: BitStream) -> int: return stream.read("int:32")


# Represents an integer between 0 and 232-1 inclusive. The values are encoded using four bytes in network byte order
# (big-endian).
def write_uint_32(val: int, stream: BitStream): stream.append(f"uint:32={val}")


def read_uint_32(stream: BitStream) -> int: return stream.read("uint:32")


def __write_string_utf8_bytes(val: str): return bytes(val, "UTF-8")


def __read_string_utf8_bytes(length: int, stream: BitStream) -> str: return str(stream.read(f"bytes:{length}"), "UTF-8")


# Represents a sequence of characters or null. For non-null strings, first the length N is given as an INT16. Then N
# bytes follow which are the UTF-8 encoding of the character sequence. A null value is encoded with length of -1 and
# there are no following bytes.
def write_nullable_string(val: None | str, stream: BitStream):
    if val is None:
        write_int_16(-1, stream)
    else:
        string_bytes = __write_string_utf8_bytes(val)
        write_int_16(len(string_bytes), stream)
        stream.append(string_bytes)


def read_nullable_string(stream: BitStream) -> None | str:
    length = read_int_16(stream)
    return None if length == -1 else __read_string_utf8_bytes(length, stream)


# Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT . Then N bytes follow
# which are the UTF-8 encoding of the character sequence.
def write_compact_string(val: str, stream: BitStream):
    string_bytes = __write_string_utf8_bytes(val)
    write_unsigned_varint(len(string_bytes) + 1, stream)
    stream.append(string_bytes)


def read_compact_string(stream: BitStream) -> str:
    return __read_string_utf8_bytes(
        read_unsigned_varint(stream) - 1,
        stream
    )


def write_compact_nullable_string(val: None | str, stream: BitStream):
    if val is None:
        write_unsigned_varint(1, stream)
    else:
        write_compact_string(val, stream)


def read_compact_nullable_string(stream: BitStream) -> None | str:
    length = read_unsigned_varint(stream) - 1
    return None if length == 0 else __read_string_utf8_bytes(length, stream)


# https://github.com/apache/kafka/blob/fe6a827e20d30af5328d7376a831f9666e0c8110/clients/src/main/java/org/apache/kafka/common/utils/ByteUtils.java#L344
def write_unsigned_varint(val: int, stream: BitStream):
    if val & (0xFFFFFFFF << 7) == 0:
        stream.append(f"uint:8={val}")
    else:
        stream.append(f"uint:8={val & 0x7F | 0x80}")
        if (val & (0xFFFFFFFF << 14)) == 0:
            stream.append(f"uint:8={util.numbers.unsigned_right_shift(val, 7)}")
        else:
            stream.append(f"uint:8={util.numbers.unsigned_right_shift(val, 7) & 0x7F | 0x80}")
            if (val & (0xFFFFFFFF << 21)) == 0:
                stream.append(f"uint:8={util.numbers.unsigned_right_shift(val, 14)}")
            else:
                stream.append(f"uint:8={util.numbers.unsigned_right_shift(val, 14) & 0x7F | 0x80}")
                if (val & (0xFFFFFFFF << 28)) == 0:
                    stream.append(f"uint:8={util.numbers.unsigned_right_shift(val, 21)}")
                else:
                    stream.append(f"uint:8={util.numbers.unsigned_right_shift(val, 21) & 0x7F | 0x80}")
                    stream.append(f"uint:8={util.numbers.unsigned_right_shift(val, 28)}")


def read_unsigned_varint(stream: BitStream) -> int:
    tmp = stream.read("uint:8")
    if tmp >= 0:
        return tmp
    else:
        result = tmp & 127
        tmp = stream.read("uint:8")
        if tmp >= 0:
            result |= tmp << 7
        else:
            result |= (tmp & 127) << 7
            tmp = stream.read("uint:8")
            if tmp >= 0:
                result |= tmp << 14
            else:
                result |= (tmp & 127) << 14
                tmp = stream.read("uint:8")
                if tmp >= 0:
                    result |= tmp << 21
                else:
                    result |= (tmp & 127) << 21
                    tmp = stream.read("uint:8")
                    result |= tmp << 28
                    if tmp < 0:
                        raise Exception(f"Unsigned Varint did not terminate after 5 bytes {result}")
        return result


# Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g. STRING) or a
# structure. First, the length N + 1 is given as an UNSIGNED_VARINT. Then N instances of type T follow. A null array
# is represented with a length of 0. In protocol documentation an array of T instances is referred to as [T].
def write_compact_array(arr: List[T],
                        stream: BitStream,
                        item_serializer: Callable[[T, BitStream], None]):
    if arr is None:
        write_unsigned_varint(0, stream)
    else:
        write_unsigned_varint(len(arr) + 1, stream)
        for item in arr:
            item_serializer(item, stream)


def compact_array_reader(
        item_deserializer: Callable[[BitStream], T]
) -> Callable[[BitStream], List[T]]:
    def read_compact_array(stream: BitStream) -> List[T]:
        length = read_unsigned_varint(stream) - 1
        result = []
        if length < 1:
            return result
        for i in range(length):
            result.append(item_deserializer(stream))
        return result

    return read_compact_array


def read_tag_buffer(stream: BitStream) -> bytes:
    stream.read("uint:8")
    return b'\x00'


# Represents a type 4 immutable universally unique identifier (Uuid). The values are encoded using sixteen bytes in
# network byte order (big-endian).
def write_uuid(val: UUID, stream: BitStream):
    stream.append(val.bytes)


def read_uuid(stream: BitStream) -> UUID: return UUID(int=read_uint_32(stream))
