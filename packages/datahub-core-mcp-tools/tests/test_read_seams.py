from typing import Mapping, Sequence
from unittest.mock import MagicMock

from fastmcp import Client, FastMCP
from pydantic import JsonValue

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import ToolType, register_tool
from datahub_core_mcp_tools.openapi_client import RawVersionedBatch, VersionedAspectPair
from datahub_core_mcp_tools.read_backend import (
    GraphQLReadBackend,
    ReadBackendBinding,
    get_read_backend,
)
from datahub_core_mcp_tools.result_policy import (
    ANNOTATION_KEY,
    DEFAULT_POLICY_DOCUMENT_URN,
    HEADER_KEY,
    TRUST_LABELED_TAG,
    ResultPolicyBinding,
    configuration_response,
    label_entity_results,
    label_search_response,
    trust_labeled,
)
from datahub_core_mcp_tools.tool_context import ToolContext

TRUSTED = "urn:li:dataset:(urn:li:dataPlatform:hive,trusted,PROD)"
OTHER = "urn:li:dataset:(urn:li:dataPlatform:hive,other,PROD)"
HIDDEN = "urn:li:document:hidden-config"


class _Decisions:
    def annotation(
        self, urn: str, *, explain: bool = False
    ) -> Mapping[str, JsonValue] | None:
        return {"grade": "certified"} if urn == TRUSTED else None

    def header(self, urns: Sequence[str]) -> Mapping[str, JsonValue] | None:
        return {"policy": "default", "count": len(urns)}

    def is_configuration(self, urn: str) -> bool:
        return urn == HIDDEN


class _Policy:
    def __init__(self) -> None:
        self.asked: list[Sequence[str]] = []

    def decide(self, urns: Sequence[str]) -> _Decisions:
        self.asked.append(urns)
        return _Decisions()


def _results(*urns: str) -> list[dict]:
    return [{"entity": {"urn": urn}} for urn in urns]


def test_without_a_policy_results_are_untouched() -> None:
    response = {"searchResults": _results(TRUSTED, HIDDEN)}

    with with_datahub_client(MagicMock()):
        assert label_search_response(dict(response)) == response
        assert label_entity_results(_results(TRUSTED)) is None


def test_paged_results_withhold_the_policy_document_without_a_policy() -> None:
    items = _results(TRUSTED, DEFAULT_POLICY_DOCUMENT_URN)

    with with_datahub_client(MagicMock()):
        assert label_entity_results(items) is None

    assert items[0] == {"entity": {"urn": TRUSTED}}
    assert items[1]["entity"] == configuration_response(DEFAULT_POLICY_DOCUMENT_URN)


def test_search_results_are_labeled_in_order_without_configuration() -> None:
    policy = _Policy()
    response = {
        "searchResults": _results(OTHER, HIDDEN, TRUSTED, DEFAULT_POLICY_DOCUMENT_URN)
    }

    with with_datahub_client(MagicMock(), ToolContext([ResultPolicyBinding(policy)])):
        labeled = label_search_response(response)

    assert [r["entity"]["urn"] for r in labeled["searchResults"]] == [OTHER, TRUSTED]
    assert ANNOTATION_KEY not in labeled["searchResults"][0]
    assert labeled["searchResults"][1][ANNOTATION_KEY] == {"grade": "certified"}
    assert labeled[HEADER_KEY] == {"policy": "default", "count": 2}
    # One decision for the whole page, asked once per distinct URN.
    assert len(policy.asked) == 1


def test_entity_results_keep_positions_for_paging() -> None:
    items = _results(TRUSTED, HIDDEN, OTHER)

    with with_datahub_client(
        MagicMock(), ToolContext([ResultPolicyBinding(_Policy())])
    ):
        label_entity_results(items)

    assert items[0][ANNOTATION_KEY] == {"grade": "certified"}
    assert items[1]["entity"] == configuration_response(HIDDEN)
    assert items[2] == {"entity": {"urn": OTHER}}


async def test_trust_labeled_tools_are_tagged() -> None:
    @trust_labeled
    def labeled() -> str:
        """Returns labeled entities."""
        return "ok"

    def plain() -> str:
        """Returns nothing labeled."""
        return "ok"

    mcp = FastMCP("t")
    register_tool(mcp, "labeled", labeled, tool_type=ToolType.SEARCH)
    register_tool(mcp, "plain", plain, tool_type=ToolType.SEARCH)

    async with Client(mcp) as client:
        tags = {
            t.name: set(t.meta["fastmcp"]["tags"]) for t in await client.list_tools()
        }

    assert tags == {"labeled": {"search", TRUST_LABELED_TAG}, "plain": {"search"}}


def test_reads_go_to_the_callers_datahub_by_default() -> None:
    client = MagicMock()
    client._graph.exists.return_value = True
    aspect = MagicMock()
    aspect.to_obj.return_value = {"fields": []}
    client._graph.get_entity_semityped.return_value = {"schemaMetadata": aspect}

    with with_datahub_client(client):
        backend = get_read_backend()
        assert isinstance(backend, GraphQLReadBackend)
        assert backend.exists(TRUSTED)
        assert backend.aspects(TRUSTED) == {"schemaMetadata": {"fields": []}}


def test_graphql_backend_reports_a_missing_entity_as_missing() -> None:
    client = MagicMock()
    client._graph.exists.return_value = False

    with with_datahub_client(client):
        assert get_read_backend().aspects(TRUSTED) is None

    client._graph.get_entity_semityped.assert_not_called()


def test_a_bound_backend_replaces_graphql_reads() -> None:
    class Workspace:
        def exists(self, urn: str) -> bool:
            return urn == OTHER

        def aspects(self, urn: str) -> dict | None:
            return None

        def entity(self, urn: str, *, include_contract: bool = False) -> None:
            return None

        def related_documents(self, urn: str, start: int, count: int) -> dict:
            return {}

        def search_entities(self, variables: dict) -> dict:
            return {"total": 0}

        def search_documents(self, variables: dict, *, semantic: bool) -> dict:
            return {"total": 0}

        def document_contents(self, urns: list[str]) -> list:
            return []

        def dataset_usage(self, urn: str, time_range: str) -> None:
            return None

        def aspect_versions(
            self, pairs: list[VersionedAspectPair]
        ) -> RawVersionedBatch:
            return RawVersionedBatch(aspects={}, errors={}, http_calls=0)

        def glossary_term_version(self, urn: str) -> dict | None:
            return None

        def version_set_history(self, urn: str, start: int, count: int) -> dict | None:
            return None

        def glossary_terms(self, urns: list[str]) -> list:
            return []

        def tags_named(self, name: str, count: int) -> list:
            return []

        def structured_property(self, urn: str) -> dict | None:
            return None

        def ingestion_source(self, urn: str) -> dict | None:
            return None

        def ingestion_sources(self, variables: dict) -> list:
            return []

        def ingestion_executions(self, urn: str, start: int, count: int) -> dict | None:
            return None

        def execution_request(self, urn: str, *, report: bool = True) -> dict | None:
            return None

        def ingestion_run_rows(self, run_id: str, start: int, count: int) -> list:
            return []

        def description_sources(self, urn: str) -> dict | None:
            return None

        def ownership_types_named(self, name: str, count: int) -> list:
            return []

        def domains_named(
            self, name: str, parent_domain: str | None, count: int
        ) -> list:
            return []

        def list_queries(self, variables: dict) -> dict:
            return {}

        def lineage_paths(self, variables: dict) -> dict:
            return {}

        def search_lineage(self, variables: dict) -> dict:
            return {}

    workspace = Workspace()
    client = MagicMock()

    with with_datahub_client(client, ToolContext([ReadBackendBinding(workspace)])):
        assert get_read_backend() is workspace
        assert get_read_backend().exists(OTHER)

    client._graph.exists.assert_not_called()
