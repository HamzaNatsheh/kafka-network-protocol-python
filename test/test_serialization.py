import pytest

import kafka.serialization
import bitstring


def test_int16_serialization():
    stream = bitstring.BitStream()
    kafka.serialization.write_int_16(1, stream)

    serialized = stream.tobytes()

    assert len(serialized) == 2
    assert serialized == b'\x00\x01'


def test_int32_serialization():
    stream = bitstring.BitStream()
    kafka.serialization.write_int_32(1, stream)

    serialized = stream.tobytes()

    assert len(serialized) == 4
    assert serialized == b'\x00\x00\x00\x01'


def test_nullable_string_serialization_null():
    stream = bitstring.BitStream()

    kafka.serialization.write_nullable_string(None, stream)
    serialized = stream.tobytes()

    assert len(serialized) == 2
    assert serialized == b'\xFF\xFF'


def test_nullable_string_serialization_not_null():
    stream = bitstring.BitStream()
    kafka.serialization.write_nullable_string("Hi", stream)

    serialized = stream.tobytes()

    assert len(serialized) == 4
    assert serialized == b'\x00\x02Hi'


def test_nullable_string_serialization_non_ascii():
    stream = bitstring.BitStream()
    kafka.serialization.write_nullable_string("هلا", stream)

    serialized = stream.tobytes()

    assert len(serialized) == 8
    assert serialized == b'\x00\x06\xD9\x87\xD9\x84\xD8\xA7'


def test_compact_string_serialization():
    stream = bitstring.BitStream()
    kafka.serialization.write_compact_string("Hi", stream)

    serialized = stream.tobytes()

    assert len(serialized) == 3
    assert serialized == b'\x03Hi'
