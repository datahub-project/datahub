"""Regression tests for the Fivetran task-level lineage fan-out: each
connector emits one DataJob per source->destination pair (one inlet, one
outlet) instead of a single node connecting every source to every destination.
"""

from functools import partial
from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fivetran.config import (
    Constant,
    FivetranLogConfig,
    FivetranSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.fivetran.data_classes import (
    ColumnLineage,
    Connector,
    TableLineage,
)
from datahub.ingestion.source.fivetran.fivetran import FivetranSource
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    auto_stale_entity_removal,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.metadata.urns import DataFlowUrn

CONNECTOR_ID = "my_connector_id"
DESTINATION_ID = "test_destination"


@pytest.fixture
def source():
    config = FivetranSourceConfig(
        fivetran_log_config=FivetranLogConfig(
            destination_platform="snowflake",
            snowflake_destination_config={
                "host_port": "test.snowflakecomputing.com",
                "username": "u",
                "password": "p",
                "database": "dwh",
                "warehouse": "wh",
                "role": "r",
                "log_schema": "fivetran_log",
            },
        ),
        sources_to_platform_instance={
            CONNECTOR_ID: PlatformDetail(platform="mssql", database="src_db")
        },
        destination_to_platform_instance={
            DESTINATION_ID: PlatformDetail(platform="snowflake", database="dwh")
        },
    )
    ctx = PipelineContext(run_id="test_fanout")
    with patch(
        "datahub.ingestion.source.fivetran.fivetran_log_db_reader.create_engine"
    ):
        src = FivetranSource(config, ctx)
        src.api_client = None
        src.log_reader.get_user_email = lambda *a, **k: None  # type: ignore[method-assign]
    return src


def _connector(num_tables: int) -> Connector:
    lineage = [
        TableLineage(
            source_table=f"dbo.table_{i:02d}",
            destination_table=f"dbo.table_{i:02d}",
            column_lineage=[ColumnLineage(source_column="id", destination_column="id")],
        )
        for i in range(num_tables)
    ]
    return Connector(
        connector_id=CONNECTOR_ID,
        connector_name=CONNECTOR_ID,
        connector_type="sql_server",
        paused=False,
        sync_frequency=1440,
        destination_id=DESTINATION_ID,
        user_id="",
        lineage=lineage,
        jobs=[],
    )


def _flow_urn(source: FivetranSource, connector: Connector) -> DataFlowUrn:
    return DataFlowUrn.create_from_ids(
        orchestrator=Constant.ORCHESTRATOR,
        flow_id=connector.connector_id,
        env=source.config.env,
        platform_instance=source.config.platform_instance,
    )


def _table_datajobs(source: FivetranSource, connector: Connector) -> list:
    source_details = source._resolve_source_details(connector)
    destination_details = source.resolve_destination_details(connector.destination_id)
    lineage_properties = source._compose_lineage_properties(
        source_details, destination_details
    )
    return list(
        source._generate_table_datajobs(
            connector,
            source_details,
            destination_details,
            lineage_properties,
            _flow_urn(source, connector),
            None,
        )
    )


def test_each_destination_has_exactly_one_upstream_source(source):
    """The core fix: every output table maps to exactly its own source, not
    to all sources in the connector."""
    num_tables = 5
    connector = _connector(num_tables)

    datajobs = _table_datajobs(source, connector)

    assert len(datajobs) == num_tables
    for datajob in datajobs:
        assert len(datajob.inlets) == 1
        assert len(datajob.outlets) == 1
        # table_03 must trace back only to table_03, not to every source.
        inlet_table = str(datajob.inlets[0]).split(",")[1]
        outlet_table = str(datajob.outlets[0]).split(",")[1]
        assert inlet_table.split(".")[-1] == outlet_table.split(".")[-1]


def test_no_single_datajob_fans_out_all_sources(source):
    """No DataJob (per-table or connector-level) should carry more than one
    inlet — that many-inlets-one-node shape is exactly what caused the
    fan-out."""
    connector = _connector(5)

    connector_job = source._generate_datajob_from_connector(
        connector, {}, _flow_urn(source, connector), None
    )
    # Connector-level job anchors run history only — no dataset I/O.
    assert connector_job.inlets == []
    assert connector_job.outlets == []

    for datajob in _table_datajobs(source, connector):
        assert len(datajob.inlets) <= 1
        assert len(datajob.outlets) == 1


def test_per_pair_column_lineage_is_scoped_to_its_pair(source):
    """Fine-grained lineage on each per-table job must connect only that
    pair's columns (source table X col -> destination table X col)."""
    connector = _connector(3)

    for datajob in _table_datajobs(source, connector):
        outlet_table = str(datajob.outlets[0]).split(",")[1].split(".")[-1]
        for fgl in datajob.fine_grained_lineages:
            for up in fgl.upstreams or []:
                assert outlet_table in up
            for down in fgl.downstreams or []:
                assert outlet_table in down


def test_datajob_names_are_unique_and_deterministic(source):
    """Per-pair DataJob URNs must be unique per pair and stable across runs."""
    connector = _connector(4)

    names_run_1 = [dj.name for dj in _table_datajobs(source, connector)]
    names_run_2 = [dj.name for dj in _table_datajobs(source, connector)]

    assert len(set(names_run_1)) == len(names_run_1)
    assert names_run_1 == names_run_2


def test_long_table_names_use_stable_hashed_datajob_id(source):
    """When the readable job id would exceed the URN length limit, the id
    falls back to a deterministic content hash that respects the cap."""
    long_table = "dbo." + "x" * 300
    lineage = TableLineage(
        source_table=long_table,
        destination_table=long_table,
        column_lineage=[],
    )
    connector = _connector(0)

    name_1 = source._table_datajob_name(connector, lineage)
    name_2 = source._table_datajob_name(connector, lineage)

    assert name_1 == name_2
    assert len(name_1) <= 200
    assert name_1.startswith(f"{CONNECTOR_ID}.")


def test_short_table_names_keep_readable_datajob_id(source):
    """Under the length cap, the readable name is used verbatim (not hashed)."""
    lineage = TableLineage(
        source_table="dbo.orders",
        destination_table="public.orders",
        column_lineage=[],
    )
    connector = _connector(0)

    name = source._table_datajob_name(connector, lineage)

    assert name == f"{CONNECTOR_ID}.dbo.orders_to_public.orders"


def test_duplicate_table_pairs_merge_into_one_datajob(source):
    """Repeated (source, destination) pairs collapse to one DataJob, and their
    column lineage is merged rather than dropped."""
    connector = _connector(0)
    connector.lineage = [
        TableLineage(
            source_table="dbo.orders",
            destination_table="dbo.orders",
            column_lineage=[ColumnLineage("id", "id")],
        ),
        TableLineage(
            source_table="dbo.orders",
            destination_table="dbo.orders",
            column_lineage=[ColumnLineage("amount", "amount")],
        ),
    ]

    datajobs = _table_datajobs(source, connector)

    assert len(datajobs) == 1
    assert source.report.num_duplicate_table_pairs_merged == 1
    # Both rows' columns survive on the single merged job.
    assert len(datajobs[0].fine_grained_lineages) == 2


def test_column_lineage_disabled_produces_no_fine_grained(source):
    """With column lineage disabled, per-table jobs carry table lineage only."""
    source.config.include_column_lineage = False
    connector = _connector(3)

    for datajob in _table_datajobs(source, connector):
        assert not datajob.fine_grained_lineages


def test_table_lineage_truncation_warns(source):
    """Exceeding max_table_lineage_per_connector emits a truncation warning."""
    source.config.max_table_lineage_per_connector = 2
    connector = _connector(3)

    list(source._get_connector_workunits(connector))

    warnings = [
        w
        for w in source.report._structured_logs.warnings
        if w.title == "Table lineage truncated"
    ]
    assert len(warnings) == 1


def test_undetermined_destination_urn_skips_edges_with_one_warning(source):
    """When the destination URN can't be built (e.g. a path-style destination
    with no database), all pairs are skipped and exactly one dedup'd warning
    is emitted for the destination."""
    # Path-style platform with no database -> build_destination_urn raises.
    source.config.destination_to_platform_instance[DESTINATION_ID] = PlatformDetail(
        platform="s3"
    )
    connector = _connector(3)

    datajobs = _table_datajobs(source, connector)

    assert datajobs == []
    # Every skipped edge is counted, even though the warning is deduped to one.
    assert source.report.num_lineage_edges_skipped == 3
    warnings = [
        w
        for w in source.report._structured_logs.warnings
        if w.title == "Destination URN could not be constructed"
    ]
    assert len(warnings) == 1


def test_no_column_lineage_when_source_urn_unresolved(source):
    """A pair with column lineage but no resolvable input dataset URN yields no
    fine-grained edges (an FGL with empty upstreams would be malformed)."""
    lineage = TableLineage(
        source_table="dbo.orders",
        destination_table="dbo.orders",
        column_lineage=[ColumnLineage("id", "id")],
    )
    destination_details = source.resolve_destination_details(DESTINATION_ID)
    output_urn = source.build_destination_urn("dbo.orders", destination_details)

    fgls = source._build_fine_grained_lineages(
        lineage, input_dataset_urn=None, output_dataset_urn=output_urn
    )

    assert fgls == []


def test_stale_removal_processor_is_wired(source):
    """Per-pair DataJob URNs churn as table pairs are added/dropped, so stale
    removal must be active: the manual processor is registered and the
    framework's automatic one is excluded to avoid double processing."""
    assert AutoStaleEntityRemovalProcessor in source.get_excluded_workunit_processors()
    manual = [
        p
        for p in source.get_workunit_processors()
        if isinstance(p, partial) and p.func is auto_stale_entity_removal
    ]
    assert len(manual) == 1
    assert manual[0].args == (source.stale_entity_removal_handler,)
