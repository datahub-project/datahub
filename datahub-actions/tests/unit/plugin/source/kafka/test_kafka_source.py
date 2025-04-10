# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datahub_actions.plugin.source.kafka.kafka_event_source import KafkaEventSource
from tests.unit.test_helpers import TestMessage


def test_handle_mcl():
    inp = {
        "auditHeader": None,
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
        "entityKeyAspect": None,
        "changeType": "UPSERT",
        "aspectName": "dataPlatformInstance",
        "aspect": (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"platform":"urn:li:dataPlatform:hdfs"}',
                "contentType": "application/json",
            },
        ),
        "systemMetadata": (
            "com.linkedin.pegasus2avro.mxe.SystemMetadata",
            {
                "lastObserved": 1651593943881,
                "runId": "file-2022_05_03-21_35_43",
                "registryName": None,
                "registryVersion": None,
                "properties": None,
            },
        ),
        "previousAspectValue": None,
        "previousSystemMetadata": None,
        "created": (
            "com.linkedin.pegasus2avro.common.AuditStamp",
            {
                "time": 1651593944068,
                "actor": "urn:li:corpuser:UNKNOWN",
                "impersonator": None,
            },
        ),
    }
    msg = TestMessage(inp)
    result = list(KafkaEventSource.handle_mcl(msg))[0]
    assert result is not None
    assert result.event_type == "MetadataChangeLogEvent_v1"


def test_handle_entity_event():
    msg = TestMessage(
        {
            "name": "entityChangeEvent",
            "payload": {
                "contentType": "application/json",
                "value": b'{"entityUrn": "urn:li:dataset:abc","entityType": "dataset","category": "TAG","operation": "ADD","modifier": "urn:li:tag:PII","auditStamp": {"actor": "urn:li:corpuser:jdoe","time": 1649953100653},"version":0}',
            },
        }
    )
    result = list(KafkaEventSource.handle_pe(msg))[0]
    assert result is not None
    assert result.event_type == "EntityChangeEvent_v1"
