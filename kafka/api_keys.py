# extracted from https://kafka.apache.org/protocol.html#protocol_api_keys
# Thanks to ChatGPT :)

__codes = {'Produce': 0, 'Fetch': 1, 'ListOffsets': 2, 'Metadata': 3, 'LeaderAndIsr': 4, 'StopReplica': 5,
           'UpdateMetadata': 6, 'ControlledShutdown': 7, 'OffsetCommit': 8, 'OffsetFetch': 9, 'FindCoordinator': 10,
           'JoinGroup': 11, 'Heartbeat': 12, 'LeaveGroup': 13, 'SyncGroup': 14, 'DescribeGroups': 15, 'ListGroups': 16,
           'SaslHandshake': 17, 'ApiVersions': 18, 'CreateTopics': 19, 'DeleteTopics': 20, 'DeleteRecords': 21,
           'InitProducerId': 22, 'OffsetForLeaderEpoch': 23, 'AddPartitionsToTxn': 24, 'AddOffsetsToTxn': 25,
           'EndTxn': 26, 'WriteTxnMarkers': 27, 'TxnOffsetCommit': 28, 'DescribeAcls': 29, 'CreateAcls': 30,
           'DeleteAcls': 31, 'DescribeConfigs': 32, 'AlterConfigs': 33, 'AlterReplicaLogDirs': 34,
           'DescribeLogDirs': 35,
           'SaslAuthenticate': 36, 'CreatePartitions': 37, 'CreateDelegationToken': 38, 'RenewDelegationToken': 39,
           'ExpireDelegationToken': 40, 'DescribeDelegationToken': 41, 'DeleteGroups': 42, 'ElectLeaders': 43,
           'IncrementalAlterConfigs': 44, 'AlterPartitionReassignments': 45, 'ListPartitionReassignments': 46,
           'OffsetDelete': 47, 'DescribeClientQuotas': 48, 'AlterClientQuotas': 49, 'DescribeUserScramCredentials': 50,
           'AlterUserScramCredentials': 51, 'DescribeQuorum': 55, 'AlterPartition': 56, 'UpdateFeatures': 57,
           'Envelope': 58, 'DescribeCluster': 60, 'DescribeProducers': 61, 'UnregisterBroker': 64,
           'DescribeTransactions': 65, 'ListTransactions': 66, 'AllocateProducerIds': 67, 'ConsumerGroupHeartbeat': 68,
           'ConsumerGroupDescribe': 69, 'GetTelemetrySubscriptions': 71, 'PushTelemetry': 72,
           'ListClientMetricsResources': 74}


def get_name(key: int) -> str:
    for name, _key in __codes.items():
        if key == _key:
            return name
    raise Exception(f'Unknown api key {key}')
