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
    """Test DomainNameResolver fallback behavior when DataHubGraph is None"""
    resolver = DomainNameResolver()
    domain_urn = Urn.from_string("urn:li:domain:marketing-domain")

    entity_name = resolver.get_entity_name(domain_urn, None)

    # Should fallback to the domain ID from the URN
    assert entity_name == "marketing-domain"


def test_domain_name_resolver_with_graph_no_properties():
    """Test DomainNameResolver when DataHubGraph is available but no properties found"""
    resolver = DomainNameResolver()
    domain_urn = Urn.from_string("urn:li:domain:marketing-domain")

    # Mock DataHubGraph that returns None for domain properties
    mock_graph = Mock()
    mock_graph.get_aspect.return_value = None

    entity_name = resolver.get_entity_name(domain_urn, mock_graph)

    # Should call get_aspect with correct parameters
    mock_graph.get_aspect.assert_called_once_with(
        str(domain_urn), DomainPropertiesClass
    )

    # Should fallback to the domain ID from the URN
    assert entity_name == "marketing-domain"

def test_domain_name_resolver_with_graph_and_properties():
    """Test DomainNameResolver when DataHubGraph returns properties with name"""
    resolver = DomainNameResolver()
    domain_urn = Urn.from_string("urn:li:domain:marketing-domain")

    # Mock DataHubGraph that returns domain properties with a friendly name
    mock_graph = Mock()
    mock_properties = DomainPropertiesClass(name="Marketing Domain")
    mock_graph.get_aspect.return_value = mock_properties

    entity_name = resolver.get_entity_name(domain_urn, mock_graph)

    # Should call get_aspect with correct parameters
    mock_graph.get_aspect.assert_called_once_with(
        str(domain_urn), DomainPropertiesClass
    )

    # Should return the friendly name from properties
    assert entity_name == "Marketing Domain"


def test_domain_name_resolver_with_graph_and_empty_string_name():
    """Test DomainNameResolver when domain properties has empty string name"""
    resolver = DomainNameResolver()
    domain_urn = Urn.from_string("urn:li:domain:marketing-domain")

    # Mock DataHubGraph that returns domain properties with empty string name
    mock_graph = Mock()
    mock_properties = DomainPropertiesClass(
        name=""
    )  # Empty string should be treated as falsy
    mock_graph.get_aspect.return_value = mock_properties

    entity_name = resolver.get_entity_name(domain_urn, mock_graph)

    # Should fallback to the domain ID from the URN
    assert entity_name == "marketing-domain"


