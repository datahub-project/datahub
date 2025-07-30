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
from typing import Type, TypeVar

from datahub.emitter.serialization_helper import post_json_transform
from datahub.metadata.schema_classes import (
    DictWrapper,
    GenericAspectClass,
    GenericPayloadClass,
)

TC = TypeVar("TC", bound=DictWrapper)


def parse_generic_aspect(cls: Type[TC], aspect: GenericAspectClass) -> TC:
    if aspect.contentType != "application/json":
        raise Exception(
            f"Failed to parse aspect. Unsupported content-type {aspect.contentType} provided!"
        )
    return cls.from_obj(post_json_transform(json.loads(aspect.get("value"))))


def parse_generic_payload(cls: Type[TC], payload: GenericPayloadClass) -> TC:
    if payload.contentType != "application/json":
        raise Exception(
            f"Failed to parse payload. Unsupported content-type {payload.contentType} provided!"
        )
    return cls.from_obj(post_json_transform(json.loads(payload.get("value"))))
