from typing import Any, Dict, List, Optional, Type
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dagster.config import DagsterSourceConfig
from datahub.ingestion.source.dagster.dagster import DagsterSource
from datahub.ingestion.source.dagster.dagster_api import DagsterGraphQLClient
from datahub.ingestion.source.dagster.data_classes import (
    DagsterAsset,
    DagsterAssetMetadata,
    DagsterColumn,
    DagsterColumnDep,
    DagsterColumnLineage,
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
    NumberTypeClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaMetadataClass,
    TimeTypeClass,
    UpstreamLineageClass,
)
from datahub.sdk._attribution import KnownAttribution, change_default_attribution


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
            column_lineage=[
                DagsterColumnLineage(
                    downstream_column="id",
                    upstreams=[
                        DagsterColumnDep(
                            asset_key=["my_db", "my_schema", "raw_events"],
                            column="id",
                        )
                    ],
                )
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


def _make_source(
    repos: Optional[List[DagsterRepository]] = None, **overrides: Any
) -> DagsterSource:
    config = DagsterSourceConfig.parse_obj(
        {"host": "http://localhost:3000", **overrides}
    )
    source = DagsterSource(config, PipelineContext(run_id="dagster-test"))
    client = MagicMock()
    client.get_repositories.return_value = (
        repos if repos is not None else [_build_repository()]
    )
    source.client = client
    return source


def _client_mock(source: DagsterSource) -> MagicMock:
    """Typed accessor for the mocked client (mypy treats source.client as the real type)."""
    assert isinstance(source.client, MagicMock)
    return source.client


def _aspects_by_urn(source: DagsterSource) -> Dict[str, List[object]]:
    result: Dict[str, List[object]] = {}
    # Mirror the real ingestion pipeline, which runs sources under INGESTION
    # attribution — this routes source-owned descriptions to datasetProperties.
    with change_default_attribution(KnownAttribution.INGESTION):
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


def test_jobs_disabled_by_default() -> None:
    # include_jobs defaults to False: no DataFlow/DataJob entities, only assets.
    source = _make_source()
    by_urn = _aspects_by_urn(source)
    assert source.report.jobs_scanned == 0
    assert not any("dataFlow" in urn or "dataJob" in urn for urn in by_urn)


def test_emits_core_orchestration_aspects() -> None:
    source = _make_source(include_jobs=True)
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
    assert props.customProperties["group_name"] == "analytics"
    assert "__ASSET_JOB" in props.customProperties["job_names"]

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
    fields = {f.fieldPath: f for f in schema.fields}
    assert set(fields) == {"id", "ts"}
    # type mapping + nullability are preserved
    assert isinstance(fields["id"].type.type, NumberTypeClass)
    assert fields["id"].nullable is False
    assert isinstance(fields["ts"].type.type, TimeTypeClass)
    assert fields["ts"].nullable is True


def test_asset_upstream_lineage() -> None:
    source = _make_source()
    by_urn = _aspects_by_urn(source)
    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    raw_urn = source._asset_dataset_urn(["my_db", "my_schema", "raw_events"])
    lineage = _find(by_urn[events_urn], UpstreamLineageClass)[0]
    assert lineage.upstreams[0].dataset == raw_urn


def test_emits_column_lineage() -> None:
    source = _make_source()
    by_urn = _aspects_by_urn(source)
    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    raw_urn = source._asset_dataset_urn(["my_db", "my_schema", "raw_events"])
    lineage = _find(by_urn[events_urn], UpstreamLineageClass)[0]
    assert lineage.fineGrainedLineages
    fgl = lineage.fineGrainedLineages[0]
    assert fgl.downstreams == [f"urn:li:schemaField:({events_urn},id)"]
    assert fgl.upstreams == [f"urn:li:schemaField:({raw_urn},id)"]


def test_column_lineage_disabled() -> None:
    source = _make_source(include_column_lineage=False)
    by_urn = _aspects_by_urn(source)
    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    lineage = _find(by_urn[events_urn], UpstreamLineageClass)[0]
    assert not lineage.fineGrainedLineages


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


def test_asset_lineage_disabled_keeps_column_lineage() -> None:
    # Coarse edges suppressed, but fine-grained edges still carry their backing
    # coarse upstream so they render.
    source = _make_source(include_asset_lineage=False, include_column_lineage=True)
    by_urn = _aspects_by_urn(source)
    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    raw_urn = source._asset_dataset_urn(["my_db", "my_schema", "raw_events"])
    lineage = _find(by_urn[events_urn], UpstreamLineageClass)[0]
    assert lineage.fineGrainedLineages
    # raw_events is referenced by column lineage, so it remains a coarse upstream
    assert [u.dataset for u in lineage.upstreams] == [raw_urn]


def test_all_lineage_disabled_emits_no_upstream() -> None:
    source = _make_source(include_asset_lineage=False, include_column_lineage=False)
    by_urn = _aspects_by_urn(source)
    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    assert not _find(by_urn[events_urn], UpstreamLineageClass)


def test_team_prefix_stripped_for_group_owner() -> None:
    repo = _build_repository()
    repo.assets[1].owners = [DagsterOwner(team="team:engineering")]
    source = _make_source(repos=[repo])
    by_urn = _aspects_by_urn(source)
    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    ownership = _find(by_urn[events_urn], OwnershipClass)[0]
    assert "urn:li:corpGroup:engineering" in {o.owner for o in ownership.owners}


def test_multi_asset_op_groups_io() -> None:
    # Two assets produced by the same op in the same job -> one DataJob with
    # both assets as outputs.
    repo = _build_repository()
    for asset in repo.assets:
        asset.op_names = ["shared_op"]
        asset.job_names = ["shared_job"]
    source = _make_source(repos=[repo], include_jobs=True)
    by_urn = _aspects_by_urn(source)
    job_urn = str(source._datajob_urn("shared_job", "shared_op", "my_location"))
    io = _find(by_urn[job_urn], DataJobInputOutputClass)[0]
    assert set(io.outputDatasets) == {
        source._asset_dataset_urn(["my_db", "my_schema", "raw_events"]),
        source._asset_dataset_urn(["my_db", "my_schema", "events"]),
    }


def test_asset_pattern_filters() -> None:
    source = _make_source(asset_pattern={"deny": [".*raw_events"]})
    by_urn = _aspects_by_urn(source)
    raw_urn = source._asset_dataset_urn(["my_db", "my_schema", "raw_events"])
    assert raw_urn not in by_urn
    assert source.report.assets_filtered == 1
    assert source.report.assets_scanned == 1


def test_job_pattern_filters() -> None:
    source = _make_source(include_jobs=True, job_pattern={"deny": ["__ASSET_JOB"]})
    by_urn = _aspects_by_urn(source)
    assert source.report.jobs_filtered == 1
    # A filtered job suppresses both the DataFlow and its DataJobs.
    assert not any("dataFlow" in urn for urn in by_urn)
    assert not any("dataJob" in urn for urn in by_urn)


def test_get_repositories_failure_is_reported() -> None:
    source = _make_source()
    _client_mock(source).get_repositories.side_effect = RuntimeError("boom")
    workunits = list(source.get_workunits_internal())
    assert workunits == []
    assert source.report.failures


def test_job_url_cloud_includes_deployment() -> None:
    source = _make_source(
        host="https://org.dagster.cloud", is_cloud=True, deployment="prod", token="t"
    )
    assert source._job_url("my_job") == "https://org.dagster.cloud/prod/jobs/my_job"


def test_per_asset_failure_is_isolated() -> None:
    # One bad asset must not drop the rest of the repository.
    source = _make_source()
    original = source._emit_asset

    def flaky(asset: DagsterAsset) -> Any:
        if asset.key[-1] == "raw_events":
            raise RuntimeError("bad asset")
        yield from original(asset)

    with patch.object(source, "_emit_asset", side_effect=flaky):
        by_urn = _aspects_by_urn(source)

    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    raw_urn = source._asset_dataset_urn(["my_db", "my_schema", "raw_events"])
    assert events_urn in by_urn  # the healthy asset still emitted
    assert raw_urn not in by_urn  # the failing one was skipped
    assert source.report.warnings


def test_per_repository_failure_is_isolated() -> None:
    source = _make_source()
    with patch.object(
        source, "_process_repository", side_effect=RuntimeError("bad repo")
    ):
        workunits = list(source.get_workunits_internal())
    assert workunits == []
    assert source.report.warnings
    assert source.report.repositories_scanned == 1


def test_op_name_falls_back_to_asset_key() -> None:
    repo = _build_repository()
    repo.assets[1].op_names = []  # no op name -> fall back to asset key's last part
    source = _make_source(repos=[repo], include_jobs=True)
    by_urn = _aspects_by_urn(source)
    job_urn = str(source._datajob_urn("__ASSET_JOB", "events", "my_location"))
    assert job_urn in by_urn


def test_include_assets_false_emits_only_jobs() -> None:
    source = _make_source(include_assets=False, include_jobs=True)
    by_urn = _aspects_by_urn(source)
    assert not any(urn.startswith("urn:li:dataset") for urn in by_urn)
    assert any("dataFlow" in urn for urn in by_urn)


def test_custom_ownership_type() -> None:
    source = _make_source(default_ownership_type="BUSINESS_OWNER")
    by_urn = _aspects_by_urn(source)
    events_urn = source._asset_dataset_urn(["my_db", "my_schema", "events"])
    ownership = _find(by_urn[events_urn], OwnershipClass)[0]
    assert all(o.type == OwnershipTypeClass.BUSINESS_OWNER for o in ownership.owners)


def test_source_test_connection_success_and_failure() -> None:
    config_dict = {"host": "http://localhost:3000"}
    with patch.object(DagsterGraphQLClient, "test_connection", return_value=None):
        ok = DagsterSource.test_connection(config_dict)
    assert ok.basic_connectivity is not None
    assert ok.basic_connectivity.capable is True

    with patch.object(
        DagsterGraphQLClient, "test_connection", side_effect=Exception("nope")
    ):
        bad = DagsterSource.test_connection(config_dict)
    assert bad.basic_connectivity is not None
    assert bad.basic_connectivity.capable is False
