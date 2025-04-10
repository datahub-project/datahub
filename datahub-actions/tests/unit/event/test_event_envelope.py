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
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
    MetadataChangeLogEvent,
)


def test_event_envelope_from_json():
    event_type = METADATA_CHANGE_LOG_EVENT_V1_TYPE
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

    event_envelope = EventEnvelope(event_type=event_type, event=event, meta={})

    event_envelope_json_str = event_envelope.as_json()
    event_envelope_from_json = EventEnvelope.from_json(event_envelope_json_str)

    diff = deepdiff.DeepDiff(
        event_envelope, event_envelope_from_json, ignore_order=True
    )

    assert not diff, (
        f"EventEnvelopes differ\n{pprint.pformat(diff)} \n output was: {event_envelope_from_json.as_json()}"
    )
