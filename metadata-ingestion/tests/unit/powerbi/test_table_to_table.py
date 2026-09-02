"""Unit tests for lineage between tables of one dataset.

A query with "Enable load" switched off has no entity, so its chain is followed
inline to the data source it holds. A loaded table does have an entity, so the
reference becomes an edge to it instead -- following it inline would attribute
the data source to the referring table and drop the intermediate tables that
actually exist.

The M here is parsed by the real bridge rather than hand-built into a NodeIdMap,
so a change in what the parser emits shows up as a failure instead of passing
against a fabricated tree.
"""

from typing import Dict, List, Optional
from unittest.mock import MagicMock

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query import parser
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Column,
    PowerBIDataset,
    Table,
    Workspace,
)
from datahub.metadata.schema_classes import StringTypeClass

# A three-step MSSQL connector: the only shape here that reaches a data source.
CONNECTED = (
    'let\n    database = Sql.Database("a-server", "a-db"),\n'
    '    tbl = database{[Schema="a_schema",Item="a_table"]}[Data]\nin\n    tbl'
)
EXPECTED_SOURCE = "mssql,a-db.a_schema.a_table,PROD"


def _config(**kwargs: object) -> PowerBiDashboardSourceConfig:
    return PowerBiDashboardSourceConfig.parse_obj(
        {
            "tenant_id": "a-tenant",
            "client_id": "a-client",
            "client_secret": "a-secret",
            "extract_lineage": True,
            "native_query_parsing": True,
            **kwargs,
        }
    )


def _dataset(
    tables: Dict[str, str], expressions: Optional[Dict[str, str]] = None
) -> PowerBIDataset:
    dataset = PowerBIDataset(
        id="a-dataset",
        name="A Dataset",
        description="",
        webUrl=None,
        workspace_id="a-workspace",
        workspace_name="A Workspace",
        parameters={},
        tables=[],
        tags=[],
        expressions=expressions or {},
    )
    dataset.tables = [
        Table(name=name, full_name=f"a_dataset.{name}", expression=m, dataset=dataset)
        for name, m in tables.items()
    ]
    return dataset


def _resolve(
    dataset: PowerBIDataset,
    table_name: str,
    config: Optional[PowerBiDashboardSourceConfig] = None,
) -> List[str]:
    """Upstream names the resolver produces for one table of the dataset."""
    config = config or _config()
    table = next(t for t in dataset.tables if t.name == table_name)
    lineages = parser.get_upstream_tables(
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=create_dataplatform_instance_resolver(config),
        ctx=PipelineContext(run_id="t"),
        config=config,
        expressions=dataset.expressions,
        tables={t.name: t.full_name for t in dataset.tables if t.name != table_name},
    )
    return sorted(
        [u.urn for lineage in lineages for u in lineage.upstreams]
        + [name for lineage in lineages for name in lineage.powerbi_table_upstreams]
    )


def test_a_table_built_on_a_sibling_names_the_sibling() -> None:
    dataset = _dataset({"base": CONNECTED, "derived": "let s = base in s"})

    assert _resolve(dataset, "derived") == ["a_dataset.base"]


def test_the_sibling_keeps_its_own_lineage_to_the_data_source() -> None:
    """The chain is base -> source and derived -> base, not derived -> source."""
    dataset = _dataset({"base": CONNECTED, "derived": "let s = base in s"})

    assert EXPECTED_SOURCE in _resolve(dataset, "base")[0]


def test_a_table_combining_two_siblings_names_both() -> None:
    dataset = _dataset(
        {
            "base": CONNECTED,
            "kept": "let s = base in s",
            "dropped": "let s = base in s",
            "merged": "let s = Table.Combine({kept, dropped}) in s",
        }
    )

    assert _resolve(dataset, "merged") == ["a_dataset.dropped", "a_dataset.kept"]


def test_a_reference_is_matched_case_insensitively_and_unquoted() -> None:
    """M identifiers are case-insensitive, and `#"..."` is just a quoting form."""
    dataset = _dataset({"Base Rows": CONNECTED, "derived": 'let s = #"base rows" in s'})

    assert _resolve(dataset, "derived") == ["a_dataset.Base Rows"]


def test_a_step_name_is_not_mistaken_for_a_sibling() -> None:
    """`Source` is the default Power Query step name and a plausible table name.

    The step binds it, so scope resolution wins and no edge is invented.
    """
    dataset = _dataset(
        {
            "Source": CONNECTED,
            "derived": 'let Source = Sql.Database("s", "d") in Source',
        }
    )

    assert _resolve(dataset, "derived") == []


def test_a_name_that_is_neither_a_table_nor_a_query_yields_nothing() -> None:
    """An M parameter reaches here routinely; it must not become an edge."""
    dataset = _dataset({"derived": "let s = SomeParameter in s"})

    assert _resolve(dataset, "derived") == []


def test_a_table_does_not_reference_itself() -> None:
    """A table's own name is withheld, so a self-reference cannot become an edge."""
    dataset = _dataset({"derived": "let s = derived in s"})

    assert _resolve(dataset, "derived") == []


def test_a_hidden_query_still_wins_over_nothing() -> None:
    """With the sibling absent, the same name resolves as a hidden query and is
    followed inline to the source it holds."""
    dataset = _dataset(
        {"derived": "let s = base in s"}, expressions={"base": CONNECTED}
    )

    resolved = _resolve(dataset, "derived")
    assert len(resolved) == 1 and EXPECTED_SOURCE in resolved[0]


def test_switching_the_feature_off_stops_collecting_references() -> None:
    dataset = _dataset({"base": CONNECTED, "derived": "let s = base in s"})
    config = _config(extract_table_to_table_lineage=False)
    table = next(t for t in dataset.tables if t.name == "derived")

    lineages = parser.get_upstream_tables(
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=create_dataplatform_instance_resolver(config),
        ctx=PipelineContext(run_id="t"),
        config=config,
        expressions=dataset.expressions,
        tables={},  # what the mapper passes when the flag is off
    )

    assert [n for lin in lineages for n in lin.powerbi_table_upstreams] == []


def test_an_unaccounted_name_is_counted_for_the_operator() -> None:
    """The denominator: a model that should have edges and has none needs
    something in the report to look at."""
    dataset = _dataset({"derived": "let s = SomeParameter in s"})
    config = _config()
    reporter = PowerBiDashboardSourceReport()
    table = next(t for t in dataset.tables if t.name == "derived")

    parser.get_upstream_tables(
        table=table,
        reporter=reporter,
        platform_instance_resolver=create_dataplatform_instance_resolver(config),
        ctx=PipelineContext(run_id="t"),
        config=config,
        expressions={},
        tables={},
    )

    assert reporter.m_query_unresolved_references == 1


def _workspace() -> Workspace:
    return Workspace(
        id="a-workspace",
        name="A Workspace",
        type="Workspace",
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )


def _mapper(config: PowerBiDashboardSourceConfig) -> Mapper:
    return Mapper(
        ctx=MagicMock(),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
    )


def test_the_emitted_edge_points_at_the_siblings_own_urn() -> None:
    """The URN has to be built the way the sibling's own entity is built, or the
    edge points at a dataset that does not exist."""
    config = _config()
    dataset = _dataset({"base": CONNECTED, "derived": "let s = base in s"})
    mapper = _mapper(config)
    derived = next(t for t in dataset.tables if t.name == "derived")

    mcps = mapper.extract_lineage(
        derived,
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,a_dataset.derived,PROD)",
        _workspace(),
    )
    edges = [
        u.dataset for mcp in mcps for u in getattr(mcp.aspect, "upstreams", []) or []
    ]

    assert edges == ["urn:li:dataset:(urn:li:dataPlatform:powerbi,a_dataset.base,PROD)"]


# --- column edges between siblings -------------------------------------------


def _with_columns(dataset: PowerBIDataset, columns: Dict[str, List[str]]) -> None:
    """Give each named table the listed columns."""
    for table in dataset.tables:
        names = columns.get(table.name)
        if names is None:
            continue
        table.columns = [
            Column(
                name=name,
                dataType="string",
                isHidden=False,
                datahubDataType=StringTypeClass(),
            )
            for name in names
        ]


def _column_edges(
    dataset: PowerBIDataset,
    table_name: str,
    config: Optional[PowerBiDashboardSourceConfig] = None,
) -> List[tuple]:
    config = config or _config()
    mapper = _mapper(config)
    table = next(t for t in dataset.tables if t.name == table_name)
    ds_urn = builder.make_dataset_urn_with_platform_instance(
        platform="powerbi", name=table.full_name, platform_instance=None, env="PROD"
    )
    mcps = mapper.extract_lineage(table, ds_urn, _workspace())
    return sorted(
        (
            fl.downstreams[0].split(",")[-1],
            tuple(sorted(u.split(",")[-1] for u in fl.upstreams)),
        )
        for mcp in mcps
        for fl in (getattr(mcp.aspect, "fineGrainedLineages", None) or [])
    )


def test_a_column_present_on_both_sides_gets_an_edge() -> None:
    dataset = _dataset({"base": CONNECTED, "derived": "let s = base in s"})
    _with_columns(dataset, {"base": ["amount"], "derived": ["amount"]})

    assert _column_edges(dataset, "derived") == [("amount)", ("amount)",))]


def test_a_column_only_the_downstream_has_is_left_alone() -> None:
    """A renaming or aggregating step produces this; guessing would invent an edge."""
    dataset = _dataset({"base": CONNECTED, "derived": "let s = base in s"})
    _with_columns(dataset, {"base": ["amount"], "derived": ["amount", "computed"]})

    assert _column_edges(dataset, "derived") == [("amount)", ("amount)",))]


def test_one_column_reached_through_two_siblings_is_one_edge() -> None:
    """Table.Combine unions two tables, so the column has two upstreams -- but
    only one downstream entry, or the aspect names the same column twice."""
    dataset = _dataset(
        {
            "base": CONNECTED,
            "kept": "let s = base in s",
            "dropped": "let s = base in s",
            "merged": "let s = Table.Combine({kept, dropped}) in s",
        }
    )
    _with_columns(
        dataset,
        {"kept": ["amount"], "dropped": ["amount"], "merged": ["amount"]},
    )

    edges = _column_edges(dataset, "merged")
    assert len(edges) == 1
    downstream, upstreams = edges[0]
    assert downstream == "amount)" and len(upstreams) == 2


def test_column_edges_follow_the_asset_casing_flag_not_the_lineage_one() -> None:
    """A sibling is a Power BI asset, so its URN has to agree with how its own
    entity is built. Using convert_lineage_urns_to_lowercase would point the
    edge at a dataset that does not exist."""
    dataset = _dataset({"Base": CONNECTED, "derived": "let s = Base in s"})
    _with_columns(dataset, {"Base": ["amount"], "derived": ["amount"]})
    config = _config(
        convert_urns_to_lowercase=False, convert_lineage_urns_to_lowercase=True
    )
    mapper = _mapper(config)
    derived = next(t for t in dataset.tables if t.name == "derived")
    ds_urn = builder.make_dataset_urn_with_platform_instance(
        platform="powerbi", name=derived.full_name, platform_instance=None, env="PROD"
    )

    mcps = mapper.extract_lineage(derived, ds_urn, _workspace())
    upstreams = [
        u
        for mcp in mcps
        for fl in (getattr(mcp.aspect, "fineGrainedLineages", None) or [])
        for u in fl.upstreams
    ]

    assert upstreams == [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:powerbi,"
        "a_dataset.Base,PROD),amount)"
    ]


def test_switching_column_lineage_off_drops_the_edges_not_the_table_edge() -> None:
    dataset = _dataset({"base": CONNECTED, "derived": "let s = base in s"})
    _with_columns(dataset, {"base": ["amount"], "derived": ["amount"]})
    config = _config(extract_column_level_lineage=False)

    assert _column_edges(dataset, "derived", config) == []
    assert _resolve(dataset, "derived", config) == ["a_dataset.base"]
