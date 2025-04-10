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

import json
import pprint

import deepdiff

from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    GenericAspectClass,
    StatusClass,
)
from datahub_actions.event.event_registry import (
    EntityChangeEvent,
    MetadataChangeLogEvent,
)


def test_entity_change_event_from_json():
    event = EntityChangeEvent(
        entityType="dataset",
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
        category="TAG",
        operation="ADD",
        modifier="urn:li:tag:PII",
        auditStamp=AuditStampClass(
            time=0,
            actor="urn:li:corpuser:unknown",
        ),
        version=1,
        parameters=None,
    )

    event._inner_dict["__parameters_json"] = {
        "tagUrn": "urn:li:tag:PII",
    }

    event_json_str = event.as_json()
    event_from_json = EntityChangeEvent.from_json(event_json_str)

    diff = deepdiff.DeepDiff(event, event_from_json, ignore_order=True)

    assert not diff, (
        f"EntityChangeEvents differ\n{pprint.pformat(diff)} \n output was: {event_from_json.as_json()}"
    )


def test_metadata_change_log_event_from_json():
    event = MetadataChangeLogEvent(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
        aspectName="status",
        aspect=GenericAspectClass(
            value=json.dumps(StatusClass(removed=False).to_obj()).encode(),
            contentType="application/json",
        ),
        created=AuditStampClass(
            time=0,
            actor="urn:li:corpuser:unknown",
        ),
    )

    event_json_str = event.as_json()
    event_from_json = MetadataChangeLogEvent.from_json(event_json_str)

    diff = deepdiff.DeepDiff(event, event_from_json, ignore_order=True)

    assert not diff, (
        f"MetadataChangeLogEvents differ\n{pprint.pformat(diff)} \n output was: {event_from_json.as_json()}"
    )
