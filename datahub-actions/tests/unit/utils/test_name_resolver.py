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

from unittest.mock import Mock

from datahub.metadata.schema_classes import DomainPropertiesClass
from datahub.utilities.urns.urn import Urn
from datahub_actions.utils.name_resolver import (
    DomainNameResolver,
    _name_resolver_registry,
    get_entity_name_from_urn,
    get_entity_qualifier_from_urn,
)


def test_domain_name_resolver_without_graph():
    # Test DomainNameResolver fallback behavior when DataHubGraph is None
    resolver = DomainNameResolver()
    domain_urn = Urn.from_string("urn:li:domain:marketing-domain")
    entity_name = resolver.get_entity_name(domain_urn, None)
    assert entity_name == "marketing-domain"


def test_domain_name_resolver_with_graph():
    resolver = DomainNameResolver()
    domain_urn = Urn.from_string("urn:li:domain:marketing-domain")
    mock_graph = Mock()

    # Test DomainNameResolver when DataHubGraph is available but no properties found
    mock_graph.get_aspect.return_value = None
    entity_name = resolver.get_entity_name(domain_urn, mock_graph)
    mock_graph.get_aspect.assert_called_once_with(
        str(domain_urn), DomainPropertiesClass
    )
    assert entity_name == "marketing-domain"

    # Test DomainNameResolver when DataHubGraph returns properties with name
    mock_properties = DomainPropertiesClass(name="Marketing Domain")
    mock_graph.get_aspect.return_value = mock_properties
    entity_name = resolver.get_entity_name(domain_urn, mock_graph)
    mock_graph.get_aspect.assert_called_once_with(
        str(domain_urn), DomainPropertiesClass
    )
    assert entity_name == "Marketing Domain"


