import pytest

import bitstring

from dataclasses import dataclass

import kafka.datatypes
import kafka.messages
import kafka.dataclass_binding


@dataclass
class Class1:
    attr1: kafka.datatypes.Int16
    attr2: kafka.datatypes.Int32


@dataclass
class Class2:
    a: kafka.datatypes.CompactArray[kafka.datatypes.Int32]
    b: Class1


def test():
    stream = bitstring.BitStream(b'\x03\x00\x00\x00\x01\x00\x00\x00\x02\x00\x03\x00\x00\x00\x04')

    deserializer = kafka.dataclass_binding.dataclass_deserializer(Class2)

    class2: Class2 = deserializer(stream)

    assert class2.a.val == [kafka.datatypes.Int32(1), kafka.datatypes.Int32(2)]
    assert class2.b.attr1 == kafka.datatypes.Int16(3)
    assert class2.b.attr2 == kafka.datatypes.Int32(4)
