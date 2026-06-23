import json
import pathlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Union
from unittest.mock import MagicMock, Mock, patch

import time_machine

from datahub.emitter.mce_builder import make_dataset_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.redshift.usage import (
    RedshiftAccessEvent,
    RedshiftUsageExtractor,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import OperationClass, OperationTypeClass
from datahub.testing import mce_helpers

FROZEN_TIME = "2021-09-15 09:00:00"


def test_redshift_usage_config():
    config = RedshiftConfig.model_validate(
        dict(
            host_port="xxxxx",
            database="xxxxx",
            username="xxxxx",
            password="xxxxx",
            email_domain="xxxxx",
            include_views=True,
            include_tables=True,
        )
    )

    assert config.host_port == "xxxxx"
    assert config.database == "xxxxx"
    assert config.username == "xxxxx"
    assert config.email_domain == "xxxxx"
    assert config.include_views
    assert config.include_tables


def _access_event(query: int, table: str) -> RedshiftAccessEvent:
    return RedshiftAccessEvent(
        userid=1,
        username="alice",
        query=query,
        querytxt=f"select col_a from public.{table}",
        tbl=1,
        database="dev",
        schema="public",
        table=table,
        starttime=datetime(2021, 9, 15, 9, 0, 0, tzinfo=timezone.utc),
        endtime=datetime(2021, 9, 15, 9, 0, 1, tzinfo=timezone.utc),
    )


def test_usage_via_sql_parsing_feeds_deduped_observed_queries(monkeypatch):
    """With usage_via_sql_parsing, each distinct query is parsed once (observed
    query), not once per (query, table) row — otherwise a query scanning N tables
    would be parsed N times and over-count usage across its parsed upstreams."""
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="acryl.io",
        include_usage_statistics=True,
        usage_via_sql_parsing=True,
        include_operational_stats=False,
        start_time="2021-09-15T00:00:00Z",
        end_time="2021-09-16T00:00:00Z",
    )
    extractor = RedshiftUsageExtractor(
        config=config,
        connection=MagicMock(),
        report=RedshiftReport(),
        dataset_urn_builder=lambda name: make_dataset_urn("redshift", name),
    )

    # query 1 scans two tables (two rows); query 2 scans one.
    events = [_access_event(1, "t1"), _access_event(1, "t2"), _access_event(2, "t3")]
    monkeypatch.setattr(
        extractor,
        "_gen_access_events_from_history_query",
        lambda *a, **k: iter(events),
    )

    fake_aggregator = MagicMock()
    fake_aggregator.gen_metadata.return_value = iter([])
    monkeypatch.setattr(extractor, "_make_usage_aggregator", lambda: fake_aggregator)

    list(extractor._get_workunits_internal(all_tables={}))

    # Parsed once per distinct query id (1, 2), not once per row; the preparsed
    # (stl_scan) path must be unused in parsing mode.
    assert fake_aggregator.add_observed_query.call_count == 2
    assert fake_aggregator.add_preparsed_query.call_count == 0


def test_usage_via_sql_parsing_produces_column_level_usage(monkeypatch):
    """End-to-end through the real aggregator: parsing mode attributes
    column-level usage (fieldCounts) from the query text, which the default
    stl_scan/preparsed path cannot produce."""
    from datahub.metadata.schema_classes import DatasetUsageStatisticsClass

    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="acryl.io",
        include_usage_statistics=True,
        usage_via_sql_parsing=True,
        include_operational_stats=False,
        start_time="2021-09-15T00:00:00Z",
        end_time="2021-09-16T00:00:00Z",
    )
    extractor = RedshiftUsageExtractor(
        config=config,
        connection=MagicMock(),
        report=RedshiftReport(),
        dataset_urn_builder=lambda name: make_dataset_urn("redshift", name),
    )
    event = RedshiftAccessEvent(
        userid=1,
        username="alice",
        query=1,
        querytxt="select col_a, col_b from public.t1",
        tbl=1,
        database="dev",
        schema="public",
        table="t1",
        starttime=datetime(2021, 9, 15, 9, 0, 0, tzinfo=timezone.utc),
        endtime=datetime(2021, 9, 15, 9, 0, 1, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        extractor,
        "_gen_access_events_from_history_query",
        lambda *a, **k: iter([event]),
    )

    field_paths: set = set()
    for wu in extractor._get_workunits_internal(all_tables={}):
        asp = getattr(wu.metadata, "aspect", None)
        if isinstance(asp, DatasetUsageStatisticsClass):
            field_paths.update(f.fieldPath for f in (asp.fieldCounts or []))

    # Explicit column references resolve to column-level usage without needing a
    # schema (sqlglot derives them from the single source table).
    assert {"col_a", "col_b"} <= field_paths, field_paths


def test_usage_via_sql_parsing_select_star_uses_registered_schema(monkeypatch):
    """SELECT * needs the table schema to expand columns. The schemas registered
    from the current run (schema_metadata_stream) must let the aggregator resolve
    column-level usage without depending on a pre-populated backend graph."""
    import datahub.metadata.schema_classes as m
    from datahub.emitter.mcp import MetadataChangeProposalWrapper

    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        email_domain="acryl.io",
        include_usage_statistics=True,
        usage_via_sql_parsing=True,
        include_operational_stats=False,
        start_time="2021-09-15T00:00:00Z",
        end_time="2021-09-16T00:00:00Z",
    )
    extractor = RedshiftUsageExtractor(
        config=config,
        connection=MagicMock(),
        report=RedshiftReport(),
        dataset_urn_builder=lambda name: make_dataset_urn("redshift", name),
    )
    event = RedshiftAccessEvent(
        userid=1,
        username="alice",
        query=1,
        querytxt="select * from public.t1",
        tbl=1,
        database="dev",
        schema="public",
        table="t1",
        starttime=datetime(2021, 9, 15, 9, 0, 0, tzinfo=timezone.utc),
        endtime=datetime(2021, 9, 15, 9, 0, 1, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        extractor,
        "_gen_access_events_from_history_query",
        lambda *a, **k: iter([event]),
    )

    urn = make_dataset_urn("redshift", "dev.public.t1")
    schema_wu = MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=m.SchemaMetadataClass(
            schemaName="dev.public.t1",
            platform="urn:li:dataPlatform:redshift",
            version=0,
            hash="",
            platformSchema=m.OtherSchemaClass(rawSchema=""),
            fields=[
                m.SchemaFieldClass(
                    fieldPath="col_a",
                    type=m.SchemaFieldDataTypeClass(type=m.NumberTypeClass()),
                    nativeDataType="int",
                ),
                m.SchemaFieldClass(
                    fieldPath="col_b",
                    type=m.SchemaFieldDataTypeClass(type=m.StringTypeClass()),
                    nativeDataType="varchar",
                ),
            ],
        ),
    ).as_workunit()

    field_paths: set = set()
    for wu in extractor._get_workunits_internal(
        all_tables={}, schema_metadata_stream=[schema_wu]
    ):
        asp = getattr(wu.metadata, "aspect", None)
        if isinstance(asp, m.DatasetUsageStatisticsClass):
            field_paths.update(f.fieldPath for f in (asp.fieldCounts or []))

    # Without the registered schema, SELECT * could not expand to columns.
    assert {"col_a", "col_b"} <= field_paths, field_paths


@time_machine.travel(FROZEN_TIME, tick=False)
@patch("redshift_connector.Cursor")
@patch("redshift_connector.Connection")
def test_redshift_usage_source(mock_cursor, mock_connection, pytestconfig, tmp_path):
    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/redshift"
    )
    generate_mcps_path = Path(f"{tmp_path}/redshift_usages.json")
    mock_usage_query_result = open(f"{test_resources_dir}/usage_events_history.json")
    mock_operational_query_result = open(
        f"{test_resources_dir}/operational_events_history.json"
    )
    mock_usage_query_result_dict = json.load(mock_usage_query_result)
    mock_operational_query_result_dict = json.load(mock_operational_query_result)

    mock_cursor.execute.return_value = None
    mock_connection.cursor.return_value = mock_cursor

    mock_cursor_usage_query = Mock()
    mock_cursor_operational_query = Mock()
    mock_cursor_usage_query.execute.return_value = None
    mock_cursor_operational_query.execute.return_value = None
    mock_cursor_usage_query.fetchmany.side_effect = [
        [list(row.values()) for row in mock_usage_query_result_dict],
        [],
    ]
    mock_cursor_operational_query.fetchmany.side_effect = [
        [list(row.values()) for row in mock_operational_query_result_dict],
        [],
    ]

    mock_cursor_usage_query.description = [
        [key] for key in mock_usage_query_result_dict[0]
    ]
    mock_cursor_operational_query.description = [
        [key] for key in mock_operational_query_result_dict[0]
    ]

    mock_connection.cursor.side_effect = [
        mock_cursor_operational_query,
        mock_cursor_usage_query,
    ]

    config = RedshiftConfig(host_port="test:1234", email_domain="acryl.io")
    source_report = RedshiftReport()
    usage_extractor = RedshiftUsageExtractor(
        config=config,
        connection=mock_connection,
        report=source_report,
        dataset_urn_builder=lambda table: make_dataset_urn("redshift", table),
    )

    all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]] = {
        "db1": {
            "schema1": [
                RedshiftTable(
                    name="category",
                    schema="schema1",
                    type="BASE TABLE",
                    created=None,
                    comment="",
                ),
            ]
        },
        "dev": {
            "public": [
                RedshiftTable(
                    name="users",
                    schema="public",
                    type="BASE TABLE",
                    created=None,
                    comment="",
                ),
                RedshiftTable(
                    name="orders",
                    schema="public",
                    type="BASE TABLE",
                    created=None,
                    comment="",
                ),
            ]
        },
    }
    mwus = usage_extractor.get_usage_workunits(all_tables=all_tables)
    metadata: List[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = []

    for mwu in mwus:
        metadata.append(mwu.metadata)

    # There should be 2 calls (usage aspects -1, operation aspects -1).
    assert mock_connection.cursor.call_count == 2
    assert source_report.num_usage_workunits_emitted == 3
    assert source_report.num_operational_stats_workunits_emitted == 3

    write_metadata_file(generate_mcps_path, metadata)
    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=tmp_path / "redshift_usages.json",
        golden_path=test_resources_dir / "redshift_usages_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@patch("redshift_connector.Cursor")
@patch("redshift_connector.Connection")
def test_redshift_usage_filtering(mock_cursor, mock_connection, pytestconfig, tmp_path):
    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/redshift"
    )
    generate_mcps_path = Path(f"{tmp_path}/redshift_usages.json")
    mock_usage_query_result = open(f"{test_resources_dir}/usage_events_history.json")
    mock_operational_query_result = open(
        f"{test_resources_dir}/operational_events_history.json"
    )
    mock_usage_query_result_dict = json.load(mock_usage_query_result)
    mock_operational_query_result_dict = json.load(mock_operational_query_result)

    mock_cursor.execute.return_value = None
    mock_connection.cursor.return_value = mock_cursor

    mock_cursor_usage_query = Mock()
    mock_cursor_operational_query = Mock()
    mock_cursor_usage_query.execute.return_value = None
    mock_cursor_operational_query.execute.return_value = None
    mock_cursor_usage_query.fetchmany.side_effect = [
        [list(row.values()) for row in mock_usage_query_result_dict],
        [],
    ]
    mock_cursor_operational_query.fetchmany.side_effect = [
        [list(row.values()) for row in mock_operational_query_result_dict],
        [],
    ]

    mock_cursor_usage_query.description = [
        [key] for key in mock_usage_query_result_dict[0]
    ]
    mock_cursor_operational_query.description = [
        [key] for key in mock_operational_query_result_dict[0]
    ]

    mock_connection.cursor.side_effect = [
        mock_cursor_operational_query,
        mock_cursor_usage_query,
    ]

    config = RedshiftConfig(host_port="test:1234", email_domain="acryl.io")
    usage_extractor = RedshiftUsageExtractor(
        config=config,
        connection=mock_connection,
        report=RedshiftReport(),
        dataset_urn_builder=lambda table: make_dataset_urn("redshift", table),
    )

    all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]] = {
        "dev": {
            "public": [
                RedshiftTable(
                    name="users",
                    schema="public",
                    type="BASE TABLE",
                    created=None,
                    comment="",
                ),
            ]
        },
    }
    mwus = usage_extractor.get_usage_workunits(all_tables=all_tables)
    metadata: List[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = []

    for mwu in mwus:
        metadata.append(mwu.metadata)

    write_metadata_file(generate_mcps_path, metadata)
    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=tmp_path / "redshift_usages.json",
        golden_path=test_resources_dir / "redshift_usages_filtered_golden.json",
    )


def load_access_events(test_resources_dir: pathlib.Path) -> List[Dict]:
    access_events_history_file = test_resources_dir / "usage_events_history.json"
    with access_events_history_file.open() as access_events_json:
        access_events = json.loads(access_events_json.read())
    return access_events


def test_duplicate_operations_dropped():
    report = RedshiftReport()
    usage_extractor = RedshiftUsageExtractor(
        config=MagicMock(),
        connection=MagicMock(),
        report=report,
        dataset_urn_builder=MagicMock(),
        redundant_run_skip_handler=None,
    )

    user = make_user_urn("jdoe")
    urnA = "urn:li:dataset:(urn:li:dataPlatform:redshift,db.schema.tableA,PROD)"
    urnB = "urn:li:dataset:(urn:li:dataPlatform:redshift,db.schema.tableB,PROD)"

    opA1 = MetadataChangeProposalWrapper(
        entityUrn=urnA,
        aspect=OperationClass(
            timestampMillis=100 * 1000,
            lastUpdatedTimestamp=95 * 1000,
            actor=user,
            operationType=OperationTypeClass.INSERT,
        ),
    )
    opB1 = MetadataChangeProposalWrapper(
        entityUrn=urnB,
        aspect=OperationClass(
            timestampMillis=101 * 1000,
            lastUpdatedTimestamp=94 * 1000,
            actor=user,
            operationType=OperationTypeClass.INSERT,
        ),
    )
    opA2 = MetadataChangeProposalWrapper(
        entityUrn=urnA,
        aspect=OperationClass(
            timestampMillis=102 * 1000,
            lastUpdatedTimestamp=90 * 1000,
            actor=user,
            operationType=OperationTypeClass.INSERT,
        ),
    )

    dedups = list(usage_extractor._drop_repeated_operations([opA1, opB1, opA2]))
    assert dedups == [
        opA1,
        opB1,
    ]
