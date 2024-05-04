from click import command, option

from kafka.client import SyncKafkaClient
from kafka.datatypes import CompactString, EMPTY_TAG_BUFFER
from kafka.messages import ApiVersionsV3ApiRequest, ApiVersionsV3ApiResponse
from kafka.api_keys import get_name


@command
@option('--bootstrap-server', help='REQUIRED: The Kafka server to connect to.')
def api_versions(bootstrap_server):
    client = SyncKafkaClient(bootstrap_server)
    req = ApiVersionsV3ApiRequest(
        client_software_name=CompactString("python-protocol-impl"),
        client_software_version=CompactString("1.0.0"),
        tag_buffer=EMPTY_TAG_BUFFER
    )
    response: ApiVersionsV3ApiResponse = client.send(req)
    client.close()
    print("Api\tMin Ver.\tMax Ver.")
    for api_version in response.api_keys.val:
        print(f"{get_name(api_version.api_key.val)}\t{api_version.min_version.val}\t{api_version.max_version.val}")


if __name__ == "__main__":
    api_versions()
