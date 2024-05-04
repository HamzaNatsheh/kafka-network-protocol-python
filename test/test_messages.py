import pytest

import kafka.datatypes
import kafka.messages
import kafka.dataclass_binding


def test_serialize_data_class_request_header_v2():
    req = kafka.messages.RequestHeaderV2(
        request_api_key=kafka.datatypes.Int16(18),  # 2 bytes
        request_api_version=kafka.datatypes.Int16(3),  # 2 bytes
        correlation_id=kafka.datatypes.Int32(5),  # 4 bytes
        client_id=kafka.datatypes.NullableString("cid"),  # len(2 bytes) + utf8(3 bytes) = 5 bytes
        tag_buffer=kafka.datatypes.EMPTY_TAG_BUFFER  # 1 byte
    )

    serialized = kafka.dataclass_binding.serialize_data_class(req)

    assert len(serialized) == 14
    assert serialized == b'\x00\x12\x00\x03\x00\x00\x00\x05\x00\x03cid\x00'


def test_serialize_data_class_api_versions_v3_request():
    req = kafka.messages.ApiVersionsV3ApiRequest(
        client_software_name=kafka.datatypes.CompactString("unit-tests"),  # 11 bytes
        client_software_version=kafka.datatypes.CompactString("1.0.0"),  # 6 bytes
        tag_buffer=kafka.datatypes.EMPTY_TAG_BUFFER  # 1 byte
    )

    serialized = kafka.dataclass_binding.serialize_data_class(req)

    assert len(serialized) == 18
    assert serialized == b'\x0Bunit-tests\x061.0.0\x00'


def test_response_type():
    req = kafka.messages.ApiVersionsV3ApiRequest(
        client_software_name=kafka.datatypes.CompactString("unit-tests"),  # 11 bytes
        client_software_version=kafka.datatypes.CompactString("1.0.0"),  # 6 bytes
        tag_buffer=kafka.datatypes.EMPTY_TAG_BUFFER  # 1 byte
    )

    assert req.response_type() == kafka.messages.ApiVersionsV3ApiResponse
