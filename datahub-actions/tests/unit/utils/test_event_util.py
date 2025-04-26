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

import pytest

from datahub.metadata.schema_classes import (
    DomainsClass,
    GenericAspectClass,
    GenericPayloadClass,
)
from datahub_actions.utils.event_util import parse_generic_aspect, parse_generic_payload


def test_parse_generic_aspect():
    # Success case
    test_domain_urn = "urn:li:domain:test"
    domains_obj = {"domains": [test_domain_urn]}
    generic_aspect = GenericAspectClass(
        value=json.dumps(domains_obj).encode(), contentType="application/json"
    )
    assert parse_generic_aspect(DomainsClass, generic_aspect) == DomainsClass(
        [test_domain_urn]
    )

    bad_aspect = GenericAspectClass(
        value=json.dumps(domains_obj).encode(), contentType="avrojson"
    )
    with pytest.raises(Exception, match="Unsupported content-type"):
        parse_generic_aspect(DomainsClass, bad_aspect)


def test_parse_generic_payload():
    # Success case
    test_domain_urn = "urn:li:domain:test"
    domains_obj = {"domains": [test_domain_urn]}
    generic_payload = GenericPayloadClass(
        value=json.dumps(domains_obj).encode(), contentType="application/json"
    )
    assert parse_generic_payload(DomainsClass, generic_payload) == DomainsClass(
        [test_domain_urn]
    )

    bad_payload = GenericPayloadClass(
        value=json.dumps(domains_obj).encode(), contentType="avrojson"
    )
    with pytest.raises(Exception, match="Unsupported content-type"):
        parse_generic_payload(DomainsClass, bad_payload)
