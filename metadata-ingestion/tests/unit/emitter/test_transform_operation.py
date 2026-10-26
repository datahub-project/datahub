import json
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.configuration.env_vars import get_max_transform_operation_length
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.transform_operation import truncate_transform_operation
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_upstream_lineage_to_patch,
)
from datahub.ingestion.source.metadata.lineage import (
    EntityConfig,
    EntityNodeConfig,
    FineGrainedLineageConfig,
    _get_lineage_mcp,
)
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import TableauSiteSource
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    MetadataChangeProposalClass,
    UpstreamLineageClass,
)
from datahub.specific.aspect_helpers.fine_grained_lineage import (
    HasFineGrainedLineagePatch,
)
from datahub.specific.datajob import DataJobPatchBuilder
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.datajob import to_datajob_input_output
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

ENV_VAR = "DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH"
LOGIC = "SQL: SUM(col_a) AS total"
UPSTREAM = make_dataset_urn("postgres", "db.public.source_table")
DOWNSTREAM = make_dataset_urn("postgres", "db.public.target_table")
QUERY = (
    "INSERT INTO target_table (total) "
    "SELECT CASE WHEN col_a > 0 THEN col_a + col_b ELSE col_b END FROM source_table"
)


@pytest.mark.parametrize(
    "value, max_length, expected",
    [
        (LOGIC, 100, LOGIC),
        (LOGIC, 8, "SQL: SUM(col_..."),
        (LOGIC, 16, LOGIC),
        (LOGIC, 15, "SQL: SUM(col_a) AS t..."),
        ("COPY: col_a AS col_a", 0, "COPY"),
        ("col_a + col_b", 5, "col_a..."),
        ("col_a + col_b", 0, None),
        ("NOTE: col_a", 2, "NO..."),
    ],
)
def test_truncate_transform_operation(
    value: str, max_length: int, expected: Optional[str]
) -> None:
    assert truncate_transform_operation(value, max_length) == expected


@pytest.mark.parametrize("max_length", [0, 8])
@pytest.mark.parametrize("value", [LOGIC, "COPY: col_a", "col_a + col_b + col_c"])
def test_truncate_transform_operation_is_idempotent(
    value: str, max_length: int
) -> None:
    once = truncate_transform_operation(value, max_length)
    twice = truncate_transform_operation(once, max_length) if once else None
    assert twice == once


def test_max_length_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_VAR, raising=False)
    assert get_max_transform_operation_length() is None
    monkeypatch.setenv(ENV_VAR, "8")
    assert get_max_transform_operation_length() == 8
    monkeypatch.setenv(ENV_VAR, "-1")
    with pytest.raises(ValueError):
        get_max_transform_operation_length()


def _aggregator_lineage() -> List[MetadataChangeProposalWrapper]:
    aggregator = SqlParsingAggregator(
        platform="postgres",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )
    aggregator.add_observed_query(
        ObservedQuery(query=QUERY, default_db="db", default_schema="public")
    )
    return [
        mcp
        for mcp in aggregator.gen_metadata()
        if isinstance(mcp, MetadataChangeProposalWrapper)
        and isinstance(mcp.aspect, UpstreamLineageClass)
    ]


def _transform_operations(mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
    return [
        fgl.transformOperation or ""
        for mcp in mcps
        if isinstance(mcp.aspect, UpstreamLineageClass)
        for fgl in mcp.aspect.fineGrainedLineages or []
    ]


def test_aggregator_keeps_full_logic_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENV_VAR, raising=False)
    [op] = _transform_operations(_aggregator_lineage())
    assert op.startswith("SQL: ")
    assert not op.endswith("...")


def test_aggregator_upsert_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_VAR, "10")
    [op] = _transform_operations(_aggregator_lineage())
    assert op.startswith("SQL: ")
    assert len(op) == len("SQL: ") + 10 + len("...")


def _patch_paths(mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
    assert isinstance(mcps[0].aspect, UpstreamLineageClass)
    patch = convert_upstream_lineage_to_patch(
        str(mcps[0].entityUrn), mcps[0].aspect, None
    )
    assert isinstance(patch.metadata, MetadataChangeProposalClass)
    assert patch.metadata.aspect is not None
    return [
        op["path"]
        for op in json.loads(patch.metadata.aspect.value)
        if op["path"].startswith("/fineGrainedLineages/")
    ]


def test_incremental_patch_path_is_capped_and_stable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENV_VAR, "10")
    first = _patch_paths(_aggregator_lineage())
    second = _patch_paths(_aggregator_lineage())
    assert first and first == second
    assert all(
        path.startswith("/fineGrainedLineages/SQL: CASE WHEN .../") for path in first
    )


def test_incremental_patch_path_keeps_label_at_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENV_VAR, "0")
    paths = _patch_paths(_aggregator_lineage())
    assert paths
    assert all(path.startswith("/fineGrainedLineages/SQL/") for path in paths)


def test_datajob_input_output_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_VAR, "10")
    datajob_io = to_datajob_input_output(mcps=_aggregator_lineage())
    assert datajob_io is not None
    [fgl] = datajob_io.fineGrainedLineages or []
    assert fgl.transformOperation is not None
    assert fgl.transformOperation.endswith("...")


def _long_fine_grained() -> FineGrainedLineageClass:
    return FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[make_schema_field_urn(UPSTREAM, "col_a")],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(DOWNSTREAM, "total")],
        transformOperation=LOGIC,
    )


def test_sdk_patch_builders_cap_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_VAR, "8")
    builders: List[HasFineGrainedLineagePatch] = [
        DatasetPatchBuilder(DOWNSTREAM),
        DataJobPatchBuilder("urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),task)"),
    ]
    for builder in builders:
        [mcp] = builder.add_fine_grained_lineage(_long_fine_grained()).build()
        assert mcp.aspect is not None
        value = mcp.aspect.value.decode()
        assert "SQL: SUM(col_..." in value
        assert "SUM(col_a) AS total" not in value


def test_sdk_set_fine_grained_lineages_is_capped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENV_VAR, "8")
    fine_grained = _long_fine_grained()
    [mcp] = (
        DatasetPatchBuilder(DOWNSTREAM)
        .set_fine_grained_lineages([fine_grained])
        .build()
    )
    assert mcp.aspect is not None
    [op] = json.loads(mcp.aspect.value)
    assert op["value"][0]["transformOperation"] == "SQL: SUM(col_..."
    assert fine_grained.transformOperation == LOGIC


def test_lineage_file_source_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_VAR, "8")
    node = EntityNodeConfig(
        entity=EntityConfig(
            name="db.public.target_table", type="dataset", platform="postgres"
        ),
        upstream=[
            EntityNodeConfig(
                entity=EntityConfig(
                    name="db.public.source_table", type="dataset", platform="postgres"
                )
            )
        ],
        fineGrainedLineages=[
            FineGrainedLineageConfig(
                upstreams=[make_schema_field_urn(UPSTREAM, "col_a")],
                downstreams=[make_schema_field_urn(DOWNSTREAM, "total")],
                transformOperation=LOGIC,
            )
        ],
    )
    mcp = _get_lineage_mcp(node, preserve_upstream=False)
    assert mcp is not None
    assert _transform_operations([mcp]) == ["SQL: SUM(col_..."]


@pytest.mark.parametrize(
    "max_length, expected",
    [("5", "CalculatedFieldformula: SUM([..."), ("0", "CalculatedField")],
)
def test_tableau_calculated_field_formula_is_capped(
    monkeypatch: pytest.MonkeyPatch, max_length: str, expected: str
) -> None:
    monkeypatch.setenv(ENV_VAR, max_length)
    calculated = {c.TYPE_NAME: c.CALCULATED_FIELD, c.FORMULA: "SUM([col_a])"}
    column = {c.TYPE_NAME: c.COLUMN_FIELD}
    assert (
        TableauSiteSource.get_transform_operation(MagicMock(), calculated) == expected
    )
    assert TableauSiteSource.get_transform_operation(MagicMock(), column) == c.IDENTITY
