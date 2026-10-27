import importlib
import json
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from datahub.configuration.env_vars import get_max_transform_operation_length
from datahub.emitter import transform_operation
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.transform_operation import truncate_transform_operation
from datahub.ingestion.source.metadata.lineage import (
    EntityConfig,
    EntityNodeConfig,
    FineGrainedLineageConfig,
    _get_lineage_mcp,
)
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import TableauSiteSource
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.specific.aspect_helpers.fine_grained_lineage import (
    HasFineGrainedLineagePatch,
)
from datahub.specific.datajob import DataJobPatchBuilder
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

ENV_VAR = "DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH"
LOGIC = "SQL: SUM(col_a) AS total"
CUT_AT_8 = "SQL: SUM..."
UPSTREAM = make_dataset_urn("postgres", "db.public.source_table")
DOWNSTREAM = make_dataset_urn("postgres", "db.public.target_table")
DATAJOB = "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),task)"


@pytest.mark.parametrize(
    "value, char_limit, expected",
    [
        (LOGIC, 100, LOGIC),
        (LOGIC, 21, LOGIC),
        (LOGIC, 20, "SQL: SUM(col_a) AS t..."),
        (LOGIC, 8, CUT_AT_8),
        ("CalculatedFieldformula: SUM([col_a])", 1, "C..."),
    ],
)
def test_truncate_transform_operation(
    value: str, char_limit: int, expected: str
) -> None:
    assert truncate_transform_operation(value, char_limit) == expected


@pytest.mark.parametrize("char_limit", [1, 8])
def test_truncate_transform_operation_is_idempotent(char_limit: int) -> None:
    once = truncate_transform_operation(LOGIC, char_limit)
    assert truncate_transform_operation(once, char_limit) == once


def test_max_length_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_VAR, raising=False)
    assert get_max_transform_operation_length() is None
    monkeypatch.setenv(ENV_VAR, "  ")
    assert get_max_transform_operation_length() is None
    monkeypatch.setenv(ENV_VAR, "8")
    assert get_max_transform_operation_length() == 8
    for invalid in ["0", "-1", "abc"]:
        monkeypatch.setenv(ENV_VAR, invalid)
        with pytest.raises(ValueError):
            get_max_transform_operation_length()


def test_char_limit_is_read_from_env_at_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENV_VAR, "8")
    try:
        module = importlib.reload(transform_operation)
        assert module.TRANSFORM_OPERATION_CHAR_LIMIT == 8
    finally:
        monkeypatch.delenv(ENV_VAR)
        importlib.reload(transform_operation)


def _set_char_limit(monkeypatch: pytest.MonkeyPatch, char_limit: Optional[int]) -> None:
    monkeypatch.setattr(
        transform_operation, "TRANSFORM_OPERATION_CHAR_LIMIT", char_limit
    )


def _fine_grained(
    transform_operation: Optional[str] = LOGIC,
) -> FineGrainedLineageClass:
    return FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[make_schema_field_urn(UPSTREAM, "col_a")],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(DOWNSTREAM, "total")],
        transformOperation=transform_operation,
    )


def _lineage() -> UpstreamLineageClass:
    return UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=UPSTREAM, type=DatasetLineageTypeClass.TRANSFORMED)
        ],
        fineGrainedLineages=[_fine_grained(), _fine_grained(None)],
    )


def _transform_operations(
    aspect: Optional[UpstreamLineageClass | DataJobInputOutputClass],
) -> List[Optional[str]]:
    assert aspect is not None
    return [f.transformOperation for f in aspect.fineGrainedLineages or []]


def test_aggregator_cuts_lineage_it_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_char_limit(monkeypatch, 5)
    aggregator = SqlParsingAggregator(
        platform="postgres",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="INSERT INTO target_table (total) SELECT col_a + col_b FROM source_table",
            default_db="db",
            default_schema="public",
        )
    )
    [lineage] = [
        mcp.aspect
        for mcp in aggregator.gen_metadata()
        if isinstance(mcp, MetadataChangeProposalWrapper)
        and isinstance(mcp.aspect, UpstreamLineageClass)
    ]
    assert _transform_operations(lineage) == ["SQL: ..."]


def _serialized_operations(mcpw: MetadataChangeProposalWrapper) -> List[str]:
    mcp = mcpw.make_mcp()
    assert mcp.aspect is not None
    return [
        f.get("transformOperation")
        for f in json.loads(mcp.aspect.value)["fineGrainedLineages"]
    ]


def test_make_mcp_cuts_serialized_copy_only(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_char_limit(monkeypatch, 8)
    lineage = _lineage()
    mcpw = MetadataChangeProposalWrapper(entityUrn=DOWNSTREAM, aspect=lineage)

    assert _serialized_operations(mcpw) == [CUT_AT_8, None]
    assert _transform_operations(lineage) == [LOGIC, None]


def test_make_mcp_cuts_datajob_input_output(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_char_limit(monkeypatch, 8)
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=DATAJOB,
        aspect=DataJobInputOutputClass(
            inputDatasets=[UPSTREAM],
            outputDatasets=[DOWNSTREAM],
            fineGrainedLineages=[_fine_grained()],
        ),
    )
    assert _serialized_operations(mcpw) == [CUT_AT_8]


def test_make_mcp_unchanged_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_char_limit(monkeypatch, None)
    mcpw = MetadataChangeProposalWrapper(entityUrn=DOWNSTREAM, aspect=_lineage())
    assert _serialized_operations(mcpw) == [LOGIC, None]


def test_patch_builders_cut_key(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_char_limit(monkeypatch, 8)
    builders: List[HasFineGrainedLineagePatch] = [
        DatasetPatchBuilder(DOWNSTREAM),
        DataJobPatchBuilder(DATAJOB),
    ]
    for builder in builders:
        builder.add_fine_grained_lineage(_fine_grained())
        builder.add_fine_grained_lineage(_fine_grained(None))
        [mcp] = builder.build()
        assert mcp.aspect is not None
        keys = [op["path"].split("/")[2] for op in json.loads(mcp.aspect.value)]
        assert keys == [CUT_AT_8, "NONE"]


def test_set_fine_grained_lineages_cuts_copy_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_char_limit(monkeypatch, 8)
    fine_grained = _fine_grained()
    [mcp] = (
        DatasetPatchBuilder(DOWNSTREAM)
        .set_fine_grained_lineages([fine_grained, _fine_grained(None)])
        .build()
    )
    assert mcp.aspect is not None
    [op] = json.loads(mcp.aspect.value)
    assert [f.get("transformOperation") for f in op["value"]] == [CUT_AT_8, None]
    assert fine_grained.transformOperation == LOGIC


def test_tableau_transform_operation_is_cut(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_char_limit(monkeypatch, 5)
    calculated = {c.TYPE_NAME: c.CALCULATED_FIELD, c.FORMULA: "SUM([col_a])"}
    column = {c.TYPE_NAME: c.COLUMN_FIELD}
    assert TableauSiteSource.get_transform_operation(MagicMock(), calculated) == (
        "Calcu..."
    )
    assert TableauSiteSource.get_transform_operation(MagicMock(), column) == c.IDENTITY


def test_lineage_file_source_is_cut(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_char_limit(monkeypatch, 8)
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
    assert mcp is not None and isinstance(mcp.aspect, UpstreamLineageClass)
    assert _transform_operations(mcp.aspect) == [CUT_AT_8]
