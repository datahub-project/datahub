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

import logging
import re

DATAHUB_SYSTEM_ACTOR_URN = "urn:li:corpuser:__datahub_system"
ENTITY_TYPE_TO_URL_PATH_MAP = {
    "glossaryTerm": "glossary",
    "dataset": "dataset",
    "tag": "tag",
    "corpuser": "user",
}
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def strip_urn(prefix: str, urn: str) -> str:
    return urn.lstrip(prefix)


def pretty_anything_urn(urn: str) -> str:
    return urn.replace("urn:li:", "")


def sanitize_urn(urn: str) -> str:
    return urn.replace(":", r"\:").replace("(", r"\(").replace(")", r"\)")


def sanitize_user_urn_for_search(urn: str) -> str:
    return sanitize_urn(pretty_user_urn(urn)).replace(".", r"\.")


def pretty_dataset_urn(urn: str) -> str:
    strip_dataset = strip_urn("urn:li:dataset:", urn)
    dataset_parts = strip_dataset[1:-1].split(",")
    platform = strip_urn("urn:li:dataPlatform:", dataset_parts[0])
    name = dataset_parts[1]
    return f"{platform}:{name}"


def pretty_user_urn(urn: str) -> str:
    return strip_urn("urn:li:corpuser:", urn)


def entity_type_from_urn(urn: str) -> str:
    entity_match = re.search(r"urn:li:([a-zA-Z]*):", urn)
    assert entity_match
    entity_type = entity_match.group(1)
    return entity_type


def make_datahub_url(urn: str, base_url: str) -> str:
    entity_type = entity_type_from_urn(urn)
    urn = urn.replace("/", "%2F")
    return f"{base_url}/{ENTITY_TYPE_TO_URL_PATH_MAP[entity_type]}/{urn}/"
