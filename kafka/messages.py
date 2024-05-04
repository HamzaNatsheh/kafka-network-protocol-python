import abc
from typing import Generic, Type, TypeVar
from dataclasses import dataclass

from kafka.datatypes import \
    Boolean, \
    CompactArray, \
    CompactString, \
    CompactNullableString, \
    Int16, \
    Int32, \
    NullableString, \
    TagBuffer, \
    Uuid, \
    EMPTY_TAG_BUFFER
import util.inspection

RES_TYPE = TypeVar("RES_TYPE")


class KafkaApiRequest(Generic[RES_TYPE], metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def request_api_key(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def request_api_version(self) -> int:
        raise NotImplementedError

    def response_type(self) -> Type[RES_TYPE]:
        return util.inspection.get_generic_type_parameters(self.__orig_bases__[0])[0]


# Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER
#   request_api_key => INT16
#   request_api_version => INT16
#   correlation_id => INT32
#   client_id => NULLABLE_STRING
@dataclass
class RequestHeaderV2:
    request_api_key: Int16
    request_api_version: Int16
    correlation_id: Int32
    client_id: NullableString
    tag_buffer: TagBuffer


def mk_request_header_v2(request: KafkaApiRequest,
                         correlation_id: int,
                         client_id: None | str) -> RequestHeaderV2:
    return RequestHeaderV2(
        request_api_key=Int16(request.request_api_key()),
        request_api_version=Int16(request.request_api_version()),
        correlation_id=Int32(correlation_id),
        client_id=NullableString(client_id),
        tag_buffer=EMPTY_TAG_BUFFER
    )


# Response Header v0 => correlation_id
#   correlation_id => INT32
@dataclass
class ResponseHeaderV0:
    correlation_id: Int32


# Response Header v1 => correlation_id TAG_BUFFER
#   correlation_id => INT32
@dataclass
class ResponseHeaderV1:
    correlation_id: Int32
    tag_buffer: TagBuffer


# ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms TAG_BUFFER
#   error_code => INT16
#   api_keys => api_key min_version max_version TAG_BUFFER
#     api_key => INT16
#     min_version => INT16
#     max_version => INT16
#   throttle_time_ms => INT32
@dataclass
class ApiVersionsV3ApiResponse:
    @dataclass
    class ApiKey:
        api_key: Int16
        min_version: Int16
        max_version: Int16
        tag_buffer: TagBuffer

    error_code: Int16
    api_keys: CompactArray[ApiKey]
    throttle_time_ms: Int32
    tag_buffer: TagBuffer


# ApiVersions Request (Version: 3) => client_software_name client_software_version TAG_BUFFER
#   client_software_name => COMPACT_STRING
#   client_software_version => COMPACT_STRING
@dataclass
class ApiVersionsV3ApiRequest(KafkaApiRequest[ApiVersionsV3ApiResponse]):
    client_software_name: CompactString
    client_software_version: CompactString
    tag_buffer: TagBuffer

    def request_api_key(self) -> int: return 18

    def request_api_version(self) -> int: return 3


# Metadata Response (Version: 12) => throttle_time_ms [brokers] cluster_id controller_id [topics] TAG_BUFFER
#   throttle_time_ms => INT32
#   brokers => node_id host port rack TAG_BUFFER
#     node_id => INT32
#     host => COMPACT_STRING
#     port => INT32
#     rack => COMPACT_NULLABLE_STRING
#   cluster_id => COMPACT_NULLABLE_STRING
#   controller_id => INT32
#   topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER
#     error_code => INT16
#     name => COMPACT_NULLABLE_STRING
#     topic_id => UUID
#     is_internal => BOOLEAN
#     partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] TAG_BUFFER
#       error_code => INT16
#       partition_index => INT32
#       leader_id => INT32
#       leader_epoch => INT32
#       replica_nodes => INT32
#       isr_nodes => INT32
#       offline_replicas => INT32
#     topic_authorized_operations => INT32
@dataclass
class MetadataV12ApiResponse:
    @dataclass
    class Broker:
        node_id: Int32
        host: CompactString
        port: Int32
        rack: CompactNullableString
        tag_buffer: TagBuffer

    @dataclass
    class Topic:
        @dataclass
        class Partition:
            error_code: Int16
            partition_index: Int32
            leader_id: Int32
            leader_epoch: Int32
            replica_nodes: Int32
            isr_nodes: Int32
            offline_replicas: Int32
            tag_buffer: TagBuffer

        error_code: Int16
        name: CompactNullableString
        topic_id: Uuid
        is_internal: Boolean
        partitions: CompactArray[Partition]
        topic_authorized_operations: Int32
        tag_buffer: TagBuffer

    throttle_time_ms: Int32
    brokers: CompactArray[Broker]
    cluster_id: CompactNullableString
    controller_id: Int32
    topics: CompactArray[Topic]
    tag_buffer: TagBuffer


# Metadata Request (Version: 12) => [topics] allow_auto_topic_creation include_topic_authorized_operations TAG_BUFFER
#   topics => topic_id name TAG_BUFFER
#     topic_id => UUID
#     name => COMPACT_NULLABLE_STRING
#   allow_auto_topic_creation => BOOLEAN
#   include_topic_authorized_operations => BOOLEAN
@dataclass
class MetadataV12ApiRequest(KafkaApiRequest[MetadataV12ApiResponse]):
    @dataclass
    class Topic:
        topic_id: Uuid
        name: CompactNullableString
        tag_buffer: TagBuffer

    topics: CompactArray[Topic]
    allow_auto_topic_creation: Boolean
    include_topic_authorized_operations: Boolean
    tag_buffer: TagBuffer

    def request_api_key(self) -> int: return 3

    def request_api_version(self) -> int: return 12
