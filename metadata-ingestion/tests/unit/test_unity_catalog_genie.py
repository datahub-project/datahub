import json
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Set, Type, TypeVar, Union
from unittest.mock import Mock, patch

import pytest
from databricks.sdk.core import ApiClient
from databricks.sdk.errors import DatabricksError, PermissionDenied

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import INGEST_MAX_PAYLOAD_BYTES
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.genie import GenieSpaceExtractor
from datahub.ingestion.source.unity.genie_client import (
    GenieSpace,
    GenieSpaceClient,
    InvalidGenieListing,
)
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.metadata.schema_classes import (
    AIAgentInfoClass,
    DataPlatformInstanceClass,
    MetadataChangeProposalClass,
    UpstreamLineageClass,
    _Aspect,
)

_AspectT = TypeVar("_AspectT", bound=_Aspect)

AGENT_URN = "urn:li:aiAgent:databricks-genie-c46c678e-debf-5861-b6ee-498f4d43ea7c"
CAN_EDIT = "Genie agent export requires CAN EDIT"
INVALID_EXPORT = "Invalid Genie agent export"
FETCH_FAILED = "Failed to fetch Genie agent"

AGENT_ASPECTS = {
    "aiAgentInfo",
    "status",
    "dataPlatformInstance",
    "upstreamLineage",
}


@pytest.fixture
def source() -> Iterable[UnityCatalogSource]:
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        instance = UnityCatalogSource(
            ctx=PipelineContext(run_id="test-run", pipeline_name="test-pipeline"),
            config=UnityCatalogSourceConfig(
                workspace_url="https://workspace.example.com",
                token="test-token",
                include_hive_metastore=False,
                include_tags=False,
                include_genie_spaces=True,
                platform_instance="workspace",
                env="DEV",
            ),
        )
        yield instance
        instance.close()


def space_with_sources(
    identifiers: List[str], *, space_id: str = "space-1", metric_views: bool = False
) -> GenieSpace:
    return GenieSpace(
        space_id=space_id,
        title="Sales Assistant",
        description="Answers questions about sales.",
        serialized_space=json.dumps(
            {
                "version": 2,
                "data_sources": {
                    "metric_views" if metric_views else "tables": [
                        {"identifier": identifier} for identifier in identifiers
                    ]
                },
                "instructions": {
                    "text_instructions": [{"content": ["Use ", "net revenue."]}]
                },
            }
        ),
    )


def aspect_of(
    workunits: List[MetadataWorkUnit], aspect_type: Type[_AspectT]
) -> _AspectT:
    found = [
        aspect
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(aspect_type)) is not None
    ]
    assert len(found) == 1
    return found[0]


def aspect_names(workunits: List[MetadataWorkUnit]) -> Set[str]:
    names: Set[str] = set()
    for wu in workunits:
        metadata = wu.metadata
        if isinstance(
            metadata, (MetadataChangeProposalWrapper, MetadataChangeProposalClass)
        ):
            assert metadata.aspectName
            names.add(metadata.aspectName)
    return names


def with_export(serialized: Optional[str]) -> GenieSpace:
    space = space_with_sources([])
    space.serialized_space = serialized
    return space


def oversized_space() -> GenieSpace:
    space = space_with_sources([])
    space.description = "x" * INGEST_MAX_PAYLOAD_BYTES
    return space


def extract(
    source: UnityCatalogSource,
    space: Optional[GenieSpace] = None,
    *,
    metastore: Optional[str] = None,
    client: Optional[GenieSpaceClient] = None,
    preserve_urn: Optional[Callable[[str], None]] = None,
    preserve_all_urns: Optional[Callable[[], None]] = None,
) -> List[MetadataWorkUnit]:
    if client is None:
        assert space is not None
        client = Mock(spec=GenieSpaceClient)
        client.list_spaces.return_value = [space]
        client.get_space.return_value = space
    return list(
        GenieSpaceExtractor(
            config=source.config,
            client=client,
            report=source.report,
            table_urn_builder=source.gen_dataset_urn,
            platform_instance=source.platform_instance_name,
            metastore=metastore,
            preserve_urn=preserve_urn or (lambda urn: None),
            preserve_all_urns=preserve_all_urns or (lambda: None),
        ).get_workunits()
    )


@pytest.mark.parametrize("has_content", [False, True])
def test_empty_instruction_entries_preserve_space_and_lineage(
    source: UnityCatalogSource, has_content: bool
) -> None:
    space = space_with_sources(["catalog.schema.orders"])
    assert space.serialized_space is not None
    definition = json.loads(space.serialized_space)
    entries: List[Dict[str, object]] = [{"id": "empty-instruction"}]
    if has_content:
        entries.append({"content": ["Use net revenue."]})
    definition["instructions"]["text_instructions"] = entries
    space.serialized_space = json.dumps(definition)

    workunits = extract(source, space)
    info = aspect_of(workunits, AIAgentInfoClass)
    assert info.instructions == ("Use net revenue." if has_content else None)
    lineage = aspect_of(workunits, UpstreamLineageClass)
    assert len(lineage.upstreams) == 1


@pytest.mark.parametrize("metric_views", [False, True])
@pytest.mark.parametrize("metastore", [None, "metastore"])
def test_declared_lineage_and_agent_metadata(
    source: UnityCatalogSource, metric_views: bool, metastore: Optional[str]
) -> None:
    space = space_with_sources(
        ["catalog.`schema.with.dots`.orders"] * 2, metric_views=metric_views
    )
    workunits = extract(source, space, metastore=metastore)
    assert aspect_names(workunits) == AGENT_ASPECTS
    info = aspect_of(workunits, AIAgentInfoClass)
    assert info.source and info.source.type == "EXTERNAL"
    assert info.name == space.title
    assert info.created.time == 0
    assert info.lastModified.time == 0
    assert info.instructions == "Use net revenue."
    platform = aspect_of(workunits, DataPlatformInstanceClass)
    assert platform.platform == "urn:li:dataPlatform:databricks"
    assert (
        platform.instance
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:databricks,workspace)"
    )
    lineage = aspect_of(workunits, UpstreamLineageClass)
    assert len(lineage.upstreams) == 1
    upstream = lineage.upstreams[0]
    assert upstream.auditStamp and upstream.auditStamp.time == 0
    prefix = "metastore." if metastore else ""
    assert (
        upstream.dataset
        == f"urn:li:dataset:(urn:li:dataPlatform:databricks,workspace.{prefix}catalog.schema.with.dots.orders,DEV)"
    )


def test_empty_definition_clears_lineage_and_lineage_can_be_disabled(
    source: UnityCatalogSource,
) -> None:
    space = space_with_sources([])
    workunits = extract(source, space)
    lineage = next(
        wu.get_aspect_of_type(UpstreamLineageClass)
        for wu in workunits
        if wu.get_aspect_of_type(UpstreamLineageClass)
    )
    assert lineage is not None and lineage.upstreams == []
    source.config.include_table_lineage = False
    assert all(
        wu.get_aspect_of_type(UpstreamLineageClass) is None
        for wu in extract(source, space)
    )


def test_identity_is_stable_and_workspace_scoped(source: UnityCatalogSource) -> None:
    space = space_with_sources([])
    assert extract(source, space)[0].get_urn() == AGENT_URN
    space.title = "Renamed Assistant"
    source.ctx.run_id = "another-run"
    assert extract(source, space)[0].get_urn() == AGENT_URN
    source.config.workspace_url = "https://WORKSPACE.example.com/"
    assert extract(source, space)[0].get_urn() == AGENT_URN
    source.config.workspace_url = "https://other.example.com"
    assert extract(source, space)[0].get_urn() != AGENT_URN


def test_pagination_and_export_request() -> None:
    api = Mock(spec=ApiClient)
    api.do.side_effect = [
        {"spaces": [{"space_id": "a", "title": "A"}], "next_page_token": "next"},
        {"spaces": [{"space_id": "b", "title": "B"}]},
        {"space_id": "b", "title": "B", "serialized_space": "{}"},
    ]
    client = GenieSpaceClient(api)
    assert [space.space_id for space in client.list_spaces()] == ["a", "b"]
    assert api.do.call_args_list[1].kwargs["query"] == {"page_token": "next"}
    client.get_space("b")
    assert api.do.call_args.kwargs["query"] == {"include_serialized_space": True}


def test_permission_failure_continues_and_filtered_spaces_are_not_fetched(
    source: UnityCatalogSource,
) -> None:
    api = Mock(spec=ApiClient)
    source.unity_catalog_api_proxy.genie = GenieSpaceClient(api)
    source.config.genie_space_pattern = AllowDenyPattern(deny=["skip"])
    space = space_with_sources([], space_id="ok")
    api.do.side_effect = [
        {
            "spaces": [
                {"space_id": name, "title": name} for name in ["skip", "denied", "ok"]
            ]
        },
        PermissionDenied("No access"),
        space.model_dump(),
    ]
    with patch.object(source, "process_catalogs", return_value=iter([])):
        workunits = list(source.process_metastores())
    assert aspect_names(workunits) == AGENT_ASPECTS
    assert not source.report.failures
    assert [warning.message for warning in source.report.warnings] == [CAN_EDIT]
    assert api.do.call_count == 3


@pytest.mark.parametrize("incremental", [False, True])
def test_lineage_survives_workunit_processors(
    source: UnityCatalogSource, incremental: bool
) -> None:
    source.config.incremental_lineage = incremental
    workunits = extract(source, space_with_sources(["catalog.schema.orders"]))
    with patch.object(source, "get_workunits_internal", return_value=iter(workunits)):
        processed = list(source.get_workunits())
    lineage_workunits = [
        wu
        for wu in processed
        if isinstance(
            wu.metadata, (MetadataChangeProposalClass, MetadataChangeProposalWrapper)
        )
        and wu.metadata.aspectName == "upstreamLineage"
    ]
    assert len(lineage_workunits) == 1
    if incremental:
        proposal = lineage_workunits[0].metadata
        assert isinstance(proposal, MetadataChangeProposalClass)
        assert proposal.changeType == "PATCH"
        assert proposal.entityType == "aiAgent"
        assert proposal.aspect is not None
        operations = json.loads(proposal.aspect.value)
        assert operations[0]["value"]["dataset"].endswith("catalog.schema.orders,DEV)")
    else:
        lineage = lineage_workunits[0].get_aspect_of_type(UpstreamLineageClass)
        assert lineage and len(lineage.upstreams) == 1
        assert lineage.upstreams[0].dataset.endswith("catalog.schema.orders,DEV)")


def test_repeated_page_token_fails_instead_of_looping() -> None:
    api = Mock(spec=ApiClient)
    api.do.return_value = {"spaces": [], "next_page_token": "same-token"}
    with pytest.raises(InvalidGenieListing):
        list(GenieSpaceClient(api).list_spaces())


def test_genie_disabled_skips_listing(source: UnityCatalogSource) -> None:
    source.config.include_genie_spaces = False
    source.unity_catalog_api_proxy.genie = Mock(spec=GenieSpaceClient)
    with patch.object(source, "process_catalogs", return_value=iter([])):
        assert list(source.process_metastores()) == []
    source.unity_catalog_api_proxy.genie.list_spaces.assert_not_called()


def test_large_paginated_listing_is_consumed_lazily() -> None:
    spaces_per_page = 500
    page_count = 6
    api = Mock(spec=ApiClient)
    pages = [
        {
            "spaces": [
                {
                    "space_id": f"space-{page * spaces_per_page + i}",
                    "title": "Assistant",
                }
                for i in range(spaces_per_page)
            ],
            **({"next_page_token": str(page + 1)} if page + 1 < page_count else {}),
        }
        for page in range(page_count)
    ]
    api.do.side_effect = pages
    client = GenieSpaceClient(api)
    summaries = iter(client.list_spaces())
    assert len(list(islice(summaries, 10))) == 10
    assert api.do.call_count == 1
    remaining = list(summaries)
    assert len(remaining) == page_count * spaces_per_page - 10
    assert remaining[-1].space_id == "space-2999"
    assert api.do.call_count == page_count


def test_large_export_ignores_unused_content_and_preserves_instructions(
    source: UnityCatalogSource,
) -> None:
    instructions = "Use net revenue. " * 65536
    definition = {
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": f"catalog.schema.table_{i}"} for i in range(25)]
        },
        "instructions": {"text_instructions": [{"content": [instructions]}]},
        "benchmarks": {"questions": ["ignored benchmark data" * 1024] * 100},
    }
    space = space_with_sources([])
    space.serialized_space = json.dumps(definition)
    assert len(space.serialized_space) > 3_000_000
    workunits = extract(source, space)
    info = aspect_of(workunits, AIAgentInfoClass)
    assert info.instructions == instructions
    lineage = next(
        aspect
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(UpstreamLineageClass)) is not None
    )
    assert lineage and len(lineage.upstreams) == 25
    assert "ignored benchmark data" not in json.dumps(info.to_obj())


def test_payload_limit_accounts_for_json_escaping(source: UnityCatalogSource) -> None:
    space = space_with_sources([])
    space.description = "\x00" * 1000
    with patch("datahub.ingestion.source.unity.genie.GENIE_MAX_MCP_BYTES", 4096):
        assert extract(source, space) == []
    assert not source.report.failures
    assert source.report.warnings[0].message == (
        "Genie agent metadata exceeds the ingestion payload limit"
    )


def test_legacy_export_version_is_supported(source: UnityCatalogSource) -> None:
    space = space_with_sources(["catalog.schema.orders"])
    assert space.serialized_space is not None
    definition = json.loads(space.serialized_space)
    definition["version"] = 1
    space.serialized_space = json.dumps(definition)
    workunits = extract(source, space)
    assert aspect_names(workunits) == AGENT_ASPECTS
    assert not source.report.failures


def test_source_timestamps_are_preserved(
    source: UnityCatalogSource,
) -> None:
    space = space_with_sources(["catalog.schema.orders"])
    space.create_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
    space.update_time = datetime(2021, 1, 1, tzinfo=timezone.utc)
    workunits = extract(source, space)
    info = aspect_of(workunits, AIAgentInfoClass)
    assert info.created.time == 1577836800000
    assert info.lastModified.time == 1609459200000
    lineage = aspect_of(workunits, UpstreamLineageClass)
    assert lineage.upstreams[0].auditStamp
    assert lineage.upstreams[0].auditStamp.time == 1609459200000


def test_partial_listing_failure_retains_completed_space_and_warns(
    source: UnityCatalogSource,
) -> None:
    api = Mock(spec=ApiClient)
    space = space_with_sources(["catalog.schema.orders"])
    api.do.side_effect = [
        {
            "spaces": [{"space_id": space.space_id, "title": space.title}],
            "next_page_token": "next",
        },
        space.model_dump(),
        PermissionDenied("Listing failed"),
    ]
    source.unity_catalog_api_proxy.genie = GenieSpaceClient(api)
    with patch.object(source, "process_catalogs", return_value=iter([])):
        workunits = list(source.process_metastores())
    assert aspect_names(workunits) == AGENT_ASPECTS
    assert not source.report.failures
    assert [w.message for w in source.report.warnings] == [
        "Failed to list Genie agents"
    ]


@pytest.mark.parametrize(
    ("fetched", "message", "context_part"),
    [
        pytest.param(with_export(None), CAN_EDIT, "space_id=space-1", id="missing"),
        pytest.param(
            PermissionDenied("No access"), CAN_EDIT, "space_id=space-1", id="permission"
        ),
        pytest.param(
            DatabricksError("Internal"), FETCH_FAILED, "space_id=space-1", id="api"
        ),
        pytest.param(
            TimeoutError("Timed out"), FETCH_FAILED, "space_id=space-1", id="timeout"
        ),
        pytest.param(with_export("{"), INVALID_EXPORT, "space_id=space-1", id="json"),
        pytest.param(
            with_export("[]"), INVALID_EXPORT, "space_id=space-1", id="not_object"
        ),
        pytest.param(
            with_export("{}"), INVALID_EXPORT, "space_id=space-1", id="no_version"
        ),
        pytest.param(
            with_export('{"version": null}'),
            INVALID_EXPORT,
            "space_id=space-1",
            id="null_version",
        ),
        pytest.param(
            with_export('{"version": "1"}'),
            INVALID_EXPORT,
            "space_id=space-1",
            id="string_version",
        ),
        pytest.param(
            with_export('{"version": true}'),
            INVALID_EXPORT,
            "space_id=space-1",
            id="bool_version",
        ),
        pytest.param(
            with_export('{"version": 2}'),
            INVALID_EXPORT,
            "space_id=space-1",
            id="no_data_sources",
        ),
        pytest.param(
            space_with_sources(["unqualified"]),
            INVALID_EXPORT,
            "identifier='unqualified'",
            id="bad_identifier",
        ),
        pytest.param(
            space_with_sources([], space_id="other"),
            INVALID_EXPORT,
            "space_id=space-1",
            id="mismatch",
        ),
        pytest.param(
            with_export('{"version": 999, "data_sources": {"new_source_type": []}}'),
            "Unsupported Genie export version",
            "space_id=space-1",
            id="unsupported",
        ),
        pytest.param(
            oversized_space(),
            "Genie agent metadata exceeds the ingestion payload limit",
            "aspect=aiAgentInfo size=",
            id="oversized",
        ),
    ],
)
def test_per_space_problem_warns_and_preserves_urn(
    source: UnityCatalogSource,
    fetched: Union[GenieSpace, Exception],
    message: str,
    context_part: str,
) -> None:
    client = Mock(spec=GenieSpaceClient)
    client.list_spaces.return_value = [space_with_sources([])]
    if isinstance(fetched, Exception):
        client.get_space.side_effect = fetched
    else:
        client.get_space.return_value = fetched
    preserve_urn = Mock()
    assert extract(source, client=client, preserve_urn=preserve_urn) == []
    assert not source.report.failures
    [warning] = source.report.warnings
    assert warning.message == message
    [context] = warning.context
    assert context_part in context
    assert "_GenieSpaceWarning" not in context
    preserve_urn.assert_called_once_with(AGENT_URN)


def two_space_client(first: Exception) -> GenieSpaceClient:
    client = Mock(spec=GenieSpaceClient)
    client.list_spaces.return_value = [
        space_with_sources([], space_id=space_id) for space_id in ["a", "b"]
    ]
    client.get_space.side_effect = [first, space_with_sources([], space_id="b")]
    return client


def test_unexpected_space_error_fails_and_later_spaces_continue(
    source: UnityCatalogSource,
) -> None:
    preserve_urn = Mock()
    workunits = extract(
        source,
        client=two_space_client(RuntimeError("boom")),
        preserve_urn=preserve_urn,
    )
    assert aspect_names(workunits) == AGENT_ASPECTS
    assert not source.report.warnings
    assert [f.message for f in source.report.failures] == [
        "Unexpected error ingesting Genie agent"
    ]
    preserve_urn.assert_not_called()


def test_preserve_error_fails_and_later_spaces_continue(
    source: UnityCatalogSource,
) -> None:
    workunits = extract(
        source,
        client=two_space_client(PermissionDenied("No access")),
        preserve_urn=Mock(side_effect=ConnectionError("checkpoint unavailable")),
    )
    assert aspect_names(workunits) == AGENT_ASPECTS
    assert [w.message for w in source.report.warnings] == [CAN_EDIT]
    assert [f.message for f in source.report.failures] == [
        "Failed to preserve Genie agent state"
    ]


@pytest.mark.parametrize(
    ("error", "expected_warning"),
    [
        pytest.param(PermissionDenied("No access"), True, id="api"),
        pytest.param(InvalidGenieListing("Repeated token"), True, id="pagination"),
        pytest.param(RuntimeError("boom"), False, id="unexpected"),
    ],
)
def test_listing_failure_preserves_all_agents_only_when_expected(
    source: UnityCatalogSource, error: Exception, expected_warning: bool
) -> None:
    client = Mock(spec=GenieSpaceClient)
    client.list_spaces.side_effect = error
    preserve_urn = Mock()
    preserve_all_urns = Mock()
    assert (
        extract(
            source,
            client=client,
            preserve_urn=preserve_urn,
            preserve_all_urns=preserve_all_urns,
        )
        == []
    )
    preserve_urn.assert_not_called()
    if expected_warning:
        assert not source.report.failures
        assert [w.message for w in source.report.warnings] == [
            "Failed to list Genie agents"
        ]
        preserve_all_urns.assert_called_once_with()
    else:
        assert not source.report.warnings
        assert [f.message for f in source.report.failures] == [
            "Unexpected error listing Genie agents"
        ]
        preserve_all_urns.assert_not_called()


@pytest.mark.parametrize("preserve_all", [False, True])
def test_preserving_agents_skips_checkpoint_read_when_stale_removal_is_off(
    tmp_path: Path, preserve_all: bool
) -> None:
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        source = UnityCatalogSource(
            ctx=PipelineContext(run_id="test-run", pipeline_name="test-pipeline"),
            config=UnityCatalogSourceConfig.model_validate(
                {
                    "workspace_url": "https://workspace.example.com",
                    "token": "test-token",
                    "include_genie_spaces": True,
                    "stateful_ingestion": {
                        "enabled": True,
                        "remove_stale_metadata": False,
                        "state_provider": {
                            "type": "file",
                            "config": {"filename": str(tmp_path / "checkpoint.json")},
                        },
                    },
                }
            ),
        )
    source.get_workunit_processors()
    with patch.object(source.state_provider, "get_last_checkpoint") as last:
        if preserve_all:
            source._preserve_all_genie_agent_urns()
        else:
            source._preserve_genie_agent_urn(AGENT_URN)
    last.assert_not_called()
    source.close()
