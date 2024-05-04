import pytest

from typing import ClassVar
from dataclasses import dataclass

from kafka.datatypes import CompactString, EMPTY_TAG_BUFFER
from kafka.messages import ApiVersionsV3ApiRequest
import util.inspection


@dataclass
class MyTestingClass:
    a: int
    b: int
    c: ClassVar[int] = 5


def test_get_data_class_attributes():
    my_class = MyTestingClass(1, 2)

    attributes = util.inspection.get_data_class_attributes(my_class)

    assert len(attributes) == 2  # ClassVars should be ignored here
    assert attributes[0] == 'a'
    assert attributes[1] == 'b'
    assert my_class.__getattribute__(attributes[0]) == 1
    assert my_class.__getattribute__(attributes[1]) == 2


def test_mytest():
    t = ApiVersionsV3ApiRequest

    instance = t.__new__(t)
    instance.__setattr__('client_software_name', CompactString("Hi"))
    instance.__setattr__('client_software_version', CompactString("Hi"))
    instance.__setattr__('tag_buffer', EMPTY_TAG_BUFFER)
    print(instance)


def test_get_data_class_attributes_types():
    result = util.inspection.get_data_class_attributes_types(MyTestingClass)
    assert len(result) == 3
    assert result['a'] == int
    assert result['b'] == int
    assert result['c'] == ClassVar[int]
