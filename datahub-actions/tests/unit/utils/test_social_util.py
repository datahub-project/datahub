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

from datahub.metadata.schema_classes import AuditStampClass, EntityChangeEventClass
from datahub_actions.utils.social_util import get_message_from_entity_change_event


def test_social_util_lifecycle():
    event = EntityChangeEventClass(
        entityType="dataset",
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:kafka,MetadataAuditEvent_v4,PROD)",
        category="LIFECYCLE",
        operation="CREATE",
        auditStamp=AuditStampClass(time=123, actor="urn:li:corpuser:__datahub_system"),
        version=0,
    )
    message = get_message_from_entity_change_event(
        event, "http://localhost:9002", None, "slack"
    )
    assert (
        message
        == ">✏️ *System* has created kafka dataset <http://localhost:9002/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,MetadataAuditEvent_v4,PROD)|MetadataAuditEvent_v4>."
    )


def test_social_util_tech_schema():
    event = EntityChangeEventClass(
        entityType="dataset",
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:kafka,PlatformEvent_v1,PROD)",
        category="TECHNICAL_SCHEMA",
        operation="ADD",
        modifier="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:kafka,PlatformEvent_v1,PROD),payload)",
        auditStamp=AuditStampClass(time=123, actor="urn:li:corpuser:__datahub_system"),
        version=0,
    )
    message = get_message_from_entity_change_event(
        event, "http://localhost:9002", None, "slack"
    )
    assert (
        message
        == ">✏️ *System* has added field *payload* in schema for kafka dataset <http://localhost:9002/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,PlatformEvent_v1,PROD)|PlatformEvent_v1>."
    )
