from typing import Any, Dict, List, Type
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dagster.config import DagsterSourceConfig
from datahub.ingestion.source.dagster.dagster import DagsterSource
from datahub.ingestion.source.dagster.data_classes import (
    DagsterAsset,
    DagsterAssetMetadata,
    DagsterColumn,
    DagsterJob,
    DagsterLink,
    DagsterOwner,
    DagsterRepository,
    DagsterTag,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
)


def _build_repository() -> DagsterRepository:
    upstream = DagsterAsset(
        key=["my_db", "my_schema", "raw_events"],
        group_name="analytics",
        op_names=["raw_events"],
        job_names=["__ASSET_JOB"],
        description="Raw events landing asset.",
        compute_kind="python",
        kinds=["python"],
        owners=[DagsterOwner(email="data_eng@example.com")],
        tags=[DagsterTag(key="tier", value="gold")],
        upstream_keys=[],
        downstream_keys=[["my_db", "my_schema", "events"]],
    )
    downstream = DagsterAsset(
        key=["my_db", "my_schema", "events"],
        group_name="analytics",
        op_names=["events"],
        job_names=["__ASSET_JOB"],
        description="Cleaned events.",
        compute_kind="python",
        kinds=["python"],
        owners=[
            DagsterOwner(email="data_eng@example.com"),
            DagsterOwner(team="platform"),
        ],
        tags=[DagsterTag(key="tier", value="gold"), DagsterTag(key="pii")],
        upstream_keys=[["my_db", "my_schema", "raw_events"]],
        downstream_keys=[],
        metadata=DagsterAssetMetadata(
            custom_properties={"docs": "# Events\nMarkdown docs."},
            links=[
                DagsterLink(url="https://example.com/runbook", description="runbook")
            ],
            columns=[
                DagsterColumn(name="id", native_type="int", nullable=False),
                DagsterColumn(name="ts", native_type="timestamp"),
            ],
        ),
    )
    job = DagsterJob(
        name="__ASSET_JOB",
        location_name="my_location",
        description="Auto-materialization job.",
        owners=[DagsterOwner(email="data_eng@example.com")],
        tags=[DagsterTag(key="schedule", value="daily")],
    )
    return DagsterRepository(
        name="my_repo",
        location_name="my_location",
        jobs=[job],
        assets=[upstream, downstream],
    )


def _make_source(**overrides: Any) -> DagsterSource:
    config = DagsterSourceConfig.parse_obj(
        {"host": "http://localhost:3000", **overrides}
    )
    source = DagsterSource(config, PipelineContext(run_id="dagster-test"))
    source.client = MagicMock()
    source.client.get_repositories.return_value = [_build_repository()]
    return source


def _aspects_by_urn(source: DagsterSource) -> Dict[str, List[object]]:
    result: Dict[str, List[object]] = {}
    for wu in source.get_workunits_internal():
        mcp = wu.metadata
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        assert mcp.entityUrn is not None
        result.setdefault(mcp.entityUrn, []).append(mcp.aspect)
    return result


def _find(aspects: List[object], cls: Type[Any]) -> List[Any]:
    return [a for a in aspects if isinstance(a, cls)]


def test_asset_dataset_urn_matches_plugin_convention() -> None:
    source = _make_source()
    urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    # Mirrors datahub_dagster_plugin's dataset_urn_from_asset: platform=dagster,
    # name = dotted asset key.
    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:dagster,my_db.my_schema.events,PROD)"
    )


def test_dataflow_and_datajob_urns() -> None:
    source = _make_source()
    flow_urn = str(source._dataflow_urn("__ASSET_JOB", "my_location"))
    job_urn = str(source._datajob_urn("__ASSET_JOB", "events", "my_location"))
    assert flow_urn == "urn:li:dataFlow:(dagster,my_location/__ASSET_JOB,PROD)"
    assert "my_location/events" in job_urn
    assert flow_urn in job_urn


def test_cloud_id_prefix_includes_deployment() -> None:
    source = _make_source(is_cloud=True, deployment="prod", token="secret")
    flow_urn = str(source._dataflow_urn("__ASSET_JOB", "my_location"))
    assert flow_urn == "urn:li:dataFlow:(dagster,prod/my_location/__ASSET_JOB,PROD)"


def test_emits_core_orchestration_aspects() -> None:
    source = _make_source()
    by_urn = _aspects_by_urn(source)

    flow_urn = str(source._dataflow_urn("__ASSET_JOB", "my_location"))
    assert _find(by_urn[flow_urn], DataFlowInfoClass)

    job_urn = str(source._datajob_urn("__ASSET_JOB", "events", "my_location"))
    assert _find(by_urn[job_urn], DataJobInfoClass)
    io = _find(by_urn[job_urn], DataJobInputOutputClass)[0]
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:dagster,my_db.my_schema.events,PROD)"
        in io.outputDatasets
    )
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:dagster,my_db.my_schema.raw_events,PROD)"
        in io.inputDatasets
    )


def test_emits_descriptions_ownership_tags_docs_schema() -> None:
    source = _make_source()
    by_urn = _aspects_by_urn(source)
    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    aspects = by_urn[events_urn]

    props = _find(aspects, DatasetPropertiesClass)[0]
    assert props.description == "Cleaned events."

    ownership = _find(aspects, OwnershipClass)[0]
    owner_urns = {o.owner for o in ownership.owners}
    assert "urn:li:corpuser:data_eng@example.com" in owner_urns
    assert "urn:li:corpGroup:platform" in owner_urns
    assert all(o.type == OwnershipTypeClass.TECHNICAL_OWNER for o in ownership.owners)

    tags = _find(aspects, GlobalTagsClass)[0]
    tag_urns = {t.tag for t in tags.tags}
    assert "urn:li:tag:tier:gold" in tag_urns
    assert "urn:li:tag:pii" in tag_urns
    assert "urn:li:tag:asset_group:analytics" in tag_urns

    docs = _find(aspects, InstitutionalMemoryClass)[0]
    assert docs.elements[0].url == "https://example.com/runbook"

    schema = _find(aspects, SchemaMetadataClass)[0]
    assert {f.fieldPath for f in schema.fields} == {"id", "ts"}


def test_asset_upstream_lineage() -> None:
    source = _make_source()
    by_urn = _aspects_by_urn(source)
    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    lineage = _find(by_urn[events_urn], UpstreamLineageClass)[0]
    assert lineage.upstreams[0].dataset == source._asset_dataset_urn(
        ["my_db", "my_schema", "raw_events"]
    )


def test_feature_flags_disable_extraction() -> None:
    source = _make_source(
        include_ownership=False, include_tags=False, include_metadata=False
    )
    by_urn = _aspects_by_urn(source)
    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    aspects = by_urn[events_urn]
    assert not _find(aspects, OwnershipClass)
    assert not _find(aspects, GlobalTagsClass)
    assert not _find(aspects, InstitutionalMemoryClass)
    assert not _find(aspects, SchemaMetadataClass)


def test_repository_pattern_filters() -> None:
    source = _make_source(repository_pattern={"deny": ["my_repo"]})
    by_urn = _aspects_by_urn(source)
    assert by_urn == {}
    assert source.report.repositories_filtered == 1


def test_cloud_requires_token() -> None:
    with pytest.raises(ValidationError):
        DagsterSourceConfig.parse_obj(
            {"host": "https://org.dagster.cloud", "is_cloud": True}
        )
