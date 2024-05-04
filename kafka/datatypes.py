import abc
from typing import Generic, List, TypeVar
from dataclasses import dataclass
from uuid import UUID

from bitstring import BitStream

from kafka.serialization import \
    write_boolean, \
    write_int_16, \
    write_int_32, \
    write_nullable_string, \
    write_compact_array, \
    write_compact_string, \
    write_compact_nullable_string, \
    write_uuid

T = TypeVar("T")


# class-based modeling for Kafka network primitives
# intended to be used as building blocks for Kafka requests & responses

class KafkaSerializable(metaclass=abc.ABCMeta):

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'serialize') and
                callable(subclass.serialize))

    @abc.abstractmethod
    def serialize(self, stream: BitStream):
        raise NotImplementedError


@dataclass
class Boolean(KafkaSerializable):
    val: bool

    def serialize(self, stream: BitStream): write_boolean(self.val, stream)


@dataclass
class Int16(KafkaSerializable):
    val: int

    def serialize(self, stream: BitStream): write_int_16(self.val, stream)


@dataclass
class Int32(KafkaSerializable):
    val: int

    def serialize(self, stream: BitStream): write_int_32(self.val, stream)


@dataclass
class NullableString(KafkaSerializable):
    val: None | str

    def serialize(self, stream: BitStream): write_nullable_string(self.val, stream)


@dataclass
class CompactString(KafkaSerializable):
    val: str

    def serialize(self, stream: BitStream): write_compact_string(self.val, stream)


@dataclass
class CompactNullableString(KafkaSerializable):
    val: None | str

    def serialize(self, stream: BitStream): write_compact_nullable_string(self.val, stream)


@dataclass
class CompactArray(Generic[T], KafkaSerializable):
    val: None | List[T]

    def serialize(self, stream: BitStream):
        write_compact_array(self.val, stream, lambda item, _stream: item.serialize(_stream))


@dataclass
class Uuid(KafkaSerializable):
    val: UUID

    def serialize(self, stream: BitStream): write_uuid(self.val, stream)


@dataclass
class TagBuffer(KafkaSerializable):
    val: bytes

    def serialize(self, stream: BitStream): stream.append(self.val)


EMPTY_TAG_BUFFER = TagBuffer(b'\x00')
