import typing


def is_generic_type(t: typing.Type) -> bool: return t.__class__.__name__ == '_GenericAlias'


def get_generic_class_type(t: typing.Type) -> typing.Type: return t.__origin__


def get_generic_type_parameters(t: typing.Type) -> typing.Tuple[typing.Type]: return t.__args__


def get_data_class_attributes(obj) -> tuple[str]:
    return obj.__match_args__


def get_data_class_attributes_types(cls) -> dict[str, typing.Type]:
    members = cls.__dataclass_fields__
    result = {}
    for k in members:
        result[k] = members[k].type
    return result
