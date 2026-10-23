import json
from datetime import datetime, timezone
from itertools import islice
from typing import Dict, Iterable, List, Optional
from unittest.mock import Mock, patch

import pytest
from databricks.sdk.core import ApiClient
from databricks.sdk.errors import PermissionDenied

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import INGEST_MAX_PAYLOAD_BYTES
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.genie import GenieSpaceExtractor
from datahub.ingestion.source.unity.genie_client import GenieSpace, GenieSpaceClient
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.metadata.schema_classes import (
    AIAgentInfoClass,
    DataPlatformInstanceClass,
    MetadataChangeProposalClass,
    SubTypesClass,
    UpstreamLineageClass,
)


@pytest.fixture
def source() -> Iterable[UnityCatalogSource]:
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        instance = UnityCatalogSource(
            ctx=PipelineContext(run_id="test-run", pipeline_name="test-pipeline"),
            config=UnityCatalogSourceConfig(
                workspace_url="https://workspace.example.com",
                token="test-token",
                include_hive_metastore=False,
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


def extract(
    source: UnityCatalogSource, space: GenieSpace, metastore: Optional[str] = None
) -> List[MetadataWorkUnit]:
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
    info = workunits[0].get_aspect_of_type(AIAgentInfoClass)
    assert info is not None
    assert info.instructions == ("Use net revenue." if has_content else None)
    lineage = workunits[3].get_aspect_of_type(UpstreamLineageClass)
    assert lineage is not None and len(lineage.upstreams) == 1


@pytest.mark.parametrize("metric_views", [False, True])
@pytest.mark.parametrize("metastore", [None, "metastore"])
def test_declared_lineage_and_agent_metadata(
    source: UnityCatalogSource, metric_views: bool, metastore: Optional[str]
) -> None:
    space = space_with_sources(
        ["catalog.`schema.with.dots`.orders"] * 2, metric_views=metric_views
    )
    workunits = extract(source, space, metastore)
    info = workunits[0].get_aspect_of_type(AIAgentInfoClass)
    assert info is not None
    assert info.source and info.source.type == "EXTERNAL"
    assert info.name == space.title
    subtype = workunits[-1].get_aspect_of_type(SubTypesClass)
    assert subtype is not None and subtype.typeNames == ["Genie Agent"]
    assert info.instructions == "Use net revenue."
    platform = workunits[2].get_aspect_of_type(DataPlatformInstanceClass)
    assert platform is not None
    assert platform.platform == "urn:li:dataPlatform:databricks"
    assert (
        platform.instance
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:databricks,workspace)"
    )
    lineage = workunits[3].get_aspect_of_type(UpstreamLineageClass)
    assert lineage is not None
    assert len(lineage.upstreams) == 1
    upstream = lineage.upstreams[0]
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


@pytest.mark.parametrize(
    "serialized",
    [
        None,
        "{",
        "{}",
        '{"version":2,"data_sources":{"tables":[{"identifier":"unqualified"}]}}',
    ],
)
def test_invalid_export_does_not_erase_previous_lineage(
    source: UnityCatalogSource, serialized: Optional[str]
) -> None:
    space = space_with_sources([])
    space.serialized_space = serialized
    assert extract(source, space) == []
    assert source.report.failures


def test_identity_is_stable_and_workspace_scoped(source: UnityCatalogSource) -> None:
    space = space_with_sources([])
    first = extract(source, space)[0].get_urn()
    space.title = "Renamed Assistant"
    source.ctx.run_id = "another-run"
    assert extract(source, space)[0].get_urn() == first
    source.config.workspace_url = "https://other.example.com"
    assert extract(source, space)[0].get_urn() != first


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
    assert len(workunits) == 5
    assert source.report.failures
    assert api.do.call_count == 3


def test_listing_failure_is_reported(source: UnityCatalogSource) -> None:
    source.unity_catalog_api_proxy.genie = Mock(spec=GenieSpaceClient)
    source.unity_catalog_api_proxy.genie.list_spaces.side_effect = PermissionDenied(
        "No access"
    )
    with patch.object(source, "process_catalogs", return_value=iter([])):
        assert list(source.process_metastores()) == []
    assert source.report.failures


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
    with pytest.raises(ValueError):
        list(GenieSpaceClient(api).list_spaces())


def test_genie_disabled_by_default(source: UnityCatalogSource) -> None:
    assert (
        UnityCatalogSourceConfig.model_fields["include_genie_spaces"].default is False
    )
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
    info = workunits[0].get_aspect_of_type(AIAgentInfoClass)
    assert info and info.instructions == instructions
    lineage = next(
        aspect
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(UpstreamLineageClass)) is not None
    )
    assert lineage and len(lineage.upstreams) == 25
    assert "ignored benchmark data" not in json.dumps(info.to_obj())


def test_oversized_agent_is_reported_before_emitting_partial_metadata(
    source: UnityCatalogSource,
) -> None:
    space = space_with_sources([])
    space.description = "x" * INGEST_MAX_PAYLOAD_BYTES
    assert extract(source, space) == []
    assert source.report.failures


def test_payload_limit_accounts_for_json_escaping(source: UnityCatalogSource) -> None:
    space = space_with_sources([])
    space.description = "\x00" * 1000
    with patch("datahub.ingestion.source.unity.genie.GENIE_MAX_MCP_BYTES", 4096):
        assert extract(source, space) == []
    assert source.report.failures


def test_legacy_export_version_is_supported(source: UnityCatalogSource) -> None:
    space = space_with_sources(["catalog.schema.orders"])
    assert space.serialized_space is not None
    definition = json.loads(space.serialized_space)
    definition["version"] = 1
    space.serialized_space = json.dumps(definition)
    workunits = extract(source, space)
    assert len(workunits) == 5
    assert not source.report.failures


def test_unknown_export_version_does_not_clear_lineage(
    source: UnityCatalogSource,
) -> None:
    space = space_with_sources([])
    space.serialized_space = '{"version": 999, "data_sources": {"new_source_type": []}}'
    assert extract(source, space) == []
    assert source.report.failures


def test_source_timestamps_are_preserved(
    source: UnityCatalogSource,
) -> None:
    space = space_with_sources(["catalog.schema.orders"])
    space.create_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
    space.update_time = datetime(2021, 1, 1, tzinfo=timezone.utc)
    workunits = extract(source, space)
    info = workunits[0].get_aspect_of_type(AIAgentInfoClass)
    assert info and info.created.time == 1577836800000
    assert info.lastModified.time == 1609459200000


def test_partial_listing_failure_retains_completed_space_and_reports_failure(
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
    assert len(workunits) == 5
    assert source.report.failures


def test_mismatched_space_id_is_not_ingested(source: UnityCatalogSource) -> None:
    client = Mock(spec=GenieSpaceClient)
    client.list_spaces.return_value = [space_with_sources([], space_id="expected")]
    client.get_space.return_value = space_with_sources([], space_id="other")
    source.unity_catalog_api_proxy.genie = client
    with patch.object(source, "process_catalogs", return_value=iter([])):
        assert list(source.process_metastores()) == []
    assert source.report.failures
