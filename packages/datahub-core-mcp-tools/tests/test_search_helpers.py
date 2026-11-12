"""Helpers the search tools share: response cleaning, configuration exclusion,
and caller-injected search flags."""

from unittest.mock import MagicMock

from pydantic import JsonValue

from datahub_core_mcp_tools.graphql_helpers import (
    clean_gql_response,
    with_datahub_client,
)
from datahub_core_mcp_tools.result_policy import (
    DEFAULT_POLICY_DOCUMENT_URN,
    ResultPolicyBinding,
    exclude_configuration,
)
from datahub_core_mcp_tools.search_filter_parser import parse_filter_string
from datahub_core_mcp_tools.search_flags import SearchFlagsOverride
from datahub_core_mcp_tools.tool_context import ToolContext


def test_clean_gql_response_drops_noise_and_inline_images():
    cleaned = clean_gql_response(
        {
            "__typename": "Dataset",
            "urn": "urn:li:dataset:x",
            "tags": [],
            "owner": None,
            "properties": {"__typename": "DatasetProperties", "customProperties": []},
            "fields": [{"description": "see data:image/png;base64,iVBORw0KGgo= here"}],
        }
    )
    assert cleaned == {
        "urn": "urn:li:dataset:x",
        "fields": [{"description": "see [image removed] here"}],
    }


def test_exclude_configuration_keeps_the_callers_filter_whole():
    combined = exclude_configuration("platform = a OR platform = b")
    assert combined.startswith("(platform = a OR platform = b) AND ")
    assert f'urn != "{DEFAULT_POLICY_DOCUMENT_URN}"' in combined
    parse_filter_string(combined)


def test_exclude_configuration_excludes_the_hosts_reserved_documents():
    binding = ResultPolicyBinding(
        policy=MagicMock(), reserved_urns=("urn:li:document:host-policy",)
    )
    with with_datahub_client(MagicMock(), tool_context=ToolContext([binding])):
        combined = exclude_configuration(None)
    assert 'urn != "urn:li:document:host-policy"' in combined
    assert not combined.startswith("(")


def test_search_flags_override_merges_over_defaults_without_sharing_state():
    flags: dict[str, JsonValue] = {"skipCache": True, "nested": {"a": 1}}
    override = SearchFlagsOverride(flags)
    flags["skipCache"] = False

    merged = override.apply({"skipCache": False, "maxAggValues": 5})
    assert merged == {"skipCache": True, "maxAggValues": 5, "nested": {"a": 1}}
    nested = merged["nested"]
    assert isinstance(nested, dict)
    nested["a"] = 2
    assert override.apply({})["nested"] == {"a": 1}
