from typing import Callable, Type, TypeVar

import bitstring

import kafka.datatypes
from kafka.serialization import read_boolean, read_int_16, read_int_32, read_nullable_string, read_compact_string, \
    read_compact_nullable_string, compact_array_reader, read_uuid, read_tag_buffer
import util.inspection

T = TypeVar("T")


# For serializing and deserializing data classes that use kafka.datatypes module as building blocks

def serialize_data_class(msg) -> bytes:
    stream = bitstring.BitStream()
    for attr in util.inspection.get_data_class_attributes(msg):
        msg.__getattribute__(attr).serialize(stream)
    return stream.tobytes()


def dataclass_deserializer(_type: Type[T]) -> Callable[[bitstring.BitStream], T]:
    def deserialize_data_class(stream: bitstring.BitStream) -> T:
        result = _type.__new__(_type)
        declared_attributes = util.inspection.get_data_class_attributes_types(_type)
        for field_name, field_type in declared_attributes.items():
            deserializer = __determine_deserializer(field_type)
            value = deserializer(stream)
            result.__setattr__(field_name, __wrap_value(value, field_type))
        return result

    return deserialize_data_class


def __determine_deserializer(_type: Type) -> Callable[[bitstring.BitStream], any]:
    if util.inspection.is_generic_type(_type):
        return __determine_generic_container_deserializer(_type)
    else:
        match _type:
            case kafka.datatypes.Boolean:
                return read_boolean
            case kafka.datatypes.Int16:
                return read_int_16
            case kafka.datatypes.Int32:
                return read_int_32
            case kafka.datatypes.NullableString:
                return read_nullable_string
            case kafka.datatypes.CompactString:
                return read_compact_string
            case kafka.datatypes.CompactNullableString:
                return read_compact_nullable_string
            case kafka.datatypes.Uuid:
                return read_uuid
            case kafka.datatypes.TagBuffer:
                return read_tag_buffer
            case _:
                return dataclass_deserializer(_type)


def __determine_generic_container_deserializer(_type: Type):
    items_type = util.inspection.get_generic_type_parameters(_type)[0]
    items_deserializer = __determine_deserializer(items_type)
    match util.inspection.get_generic_class_type(_type):
        case kafka.datatypes.CompactArray:
            return compact_array_reader(items_deserializer)
        case _:
            raise Exception(f"Unknown generic container type {_type}")


def __wrap_value(val: any, _type: Type[T]) -> T:
    if val.__class__ == _type:
        return val
    if util.inspection.is_generic_type(_type):
        return __wrap_generic_container(val, _type)
    result = _type.__new__(_type)
    result.__init__(val)
    return result


def __wrap_generic_container(val: any, _type: Type) -> T:
    container_type = util.inspection.get_generic_class_type(_type)
    items_type = util.inspection.get_generic_type_parameters(_type)[0]
    wrapped_items = list(map(lambda item: __wrap_value(item, items_type), val))
    return __wrap_value(wrapped_items, container_type)
