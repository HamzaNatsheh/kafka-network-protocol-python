import socket
from typing import TypeVar

import bitstring

import kafka.serialization
import kafka.messages
import kafka.dataclass_binding

T = TypeVar("T")


# Implementation for sending/receiving messages to/from a single Kafka broker synchronously.
class SyncKafkaClient:
    __sock: socket

    def __init__(self, bootstrap_server: str):
        servers = bootstrap_server.split(",")
        assert len(servers) == 1  # A client can connect to multiple bootstrap-server, we're supporting 1 only
        (host, port) = servers[0].split(":")
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((host, int(port)))

    # All requests and responses originate from the following grammar which will be incrementally describe through the
    # rest of this document:
    #
    # RequestOrResponse => Size (RequestMessage | ResponseMessage)
    #   Size => int32
    @staticmethod
    def __mk_msg(header, request) -> bytes:
        serialized_header = kafka.dataclass_binding.serialize_data_class(header)
        serialized_request = kafka.dataclass_binding.serialize_data_class(request)
        size = len(serialized_header) + len(serialized_request)

        stream = bitstring.BitStream()
        kafka.serialization.write_int_32(size, stream)
        stream.append(serialized_header)
        stream.append(serialized_request)
        return stream.tobytes()

    @staticmethod
    def __read_response_header_v0(stream: bitstring.BitStream) -> kafka.messages.ResponseHeaderV0:
        return kafka.dataclass_binding.dataclass_deserializer(kafka.messages.ResponseHeaderV0)(stream)

    def send(self, request: kafka.messages.KafkaApiRequest[T]) -> T:
        msg = self.__mk_msg(
            kafka.messages.mk_request_header_v2(request, 1, 'python-protocol-impl'),
            request
        )

        self.__sock.sendall(msg)

        response_deserializer = kafka.dataclass_binding.dataclass_deserializer(request.response_type())

        response_size = kafka.serialization.read_int_32(bitstring.BitStream(self.__sock.recv(4)))
        print(response_size)
        received = self.__sock.recv(response_size)
        print(len(received))
        response = bitstring.BitStream(received)

        self.__read_response_header_v0(response)
        return response_deserializer(response)

    def close(self): self.__sock.close()
