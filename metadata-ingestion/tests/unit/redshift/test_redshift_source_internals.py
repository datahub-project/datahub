"""RedshiftSource behaviour that sits between the Redshift system tables and the
emitted metadata: connection handling, schema field typing, dataset property
composition and the lineage/usage/profiling orchestration in _extract_metadata.

The redshift_connector boundary is stubbed; everything downstream runs for real.
"""

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Union, cast
from unittest.mock import MagicMock, patch

import pytest
import redshift_connector

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift import RedshiftSource
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftColumn,
    RedshiftDatabase,
    RedshiftDataDictionary,
    RedshiftSchema,
    RedshiftTable,
    RedshiftView,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    DomainsClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import KnownLineageMapping

DB = "dev"


def _config(**overrides: object) -> RedshiftConfig:
    base: Dict[str, object] = dict(
        host_port="localhost:5439",
        database=DB,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return RedshiftConfig.model_validate(base)


def _source(config: Optional[RedshiftConfig] = None) -> RedshiftSource:
    return RedshiftSource(config or _config(), ctx=PipelineContext(run_id="test"))


def _aspects(workunits: Iterable[MetadataWorkUnit], aspect_type: type) -> list:
    return [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, aspect_type)
    ]


def _table(name: str = "t1", schema: str = "public", **kw: object) -> RedshiftTable:
    base: Dict[str, object] = dict(
        name=name, schema=schema, type="BASE TABLE", created=None, comment=None
    )
    base.update(kw)
    return RedshiftTable(**base)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Connection handling
# --------------------------------------------------------------------------- #


def test_get_redshift_connection_passes_config_and_enables_autocommit() -> None:
    config = _config(username="u", password="p", extra_client_options={"ssl": False})
    conn = MagicMock()
    with patch.object(redshift_connector, "connect", return_value=conn) as connect:
        assert RedshiftSource.get_redshift_connection(config) is conn

    connect.assert_called_once_with(
        host="localhost",
        port=5439,
        user="u",
        database=DB,
        password="p",
        ssl=False,
    )
    assert conn.autocommit is True


@pytest.mark.parametrize(
    "error_message, expected_title",
    [
        ("FATAL: password authentication failed for user", "Invalid credentials"),
        ("connection timeout after 30s", "Unable to connect"),
        ("communication error: host unreachable", "Unable to connect"),
        ('FATAL: database "nope" does not exist', "Database does not exist"),
        ("something else entirely", "Unable to connect"),
    ],
)
def test_try_get_redshift_connection_classifies_driver_errors(
    error_message: str, expected_title: str
) -> None:
    source = _source()
    with patch.object(
        RedshiftSource,
        "get_redshift_connection",
        side_effect=redshift_connector.Error(error_message),
    ):
        assert source._try_get_redshift_connection(source.config) is None

    assert [f.title for f in source.report.failures] == [expected_title]


def test_get_workunits_internal_short_circuits_without_connection() -> None:
    source = _source()
    with patch.object(source, "_try_get_redshift_connection", return_value=None):
        assert list(source.get_workunits_internal()) == []
    assert source.db is None


def test_get_workunits_internal_reports_driver_errors_as_failure() -> None:
    source = _source()
    conn = MagicMock()
    with (
        patch.object(source, "_try_get_redshift_connection", return_value=conn),
        patch.object(
            source.data_dictionary,
            "get_database_details",
            return_value=RedshiftDatabase(name=DB, type="local"),
        ),
        patch.object(
            source,
            "cache_tables_and_views",
            side_effect=redshift_connector.Error(
                "permission denied for relation svv_table_info"
            ),
        ),
    ):
        workunits = list(source.get_workunits_internal())

    # The database container was emitted before the failure, then we stopped.
    assert workunits
    assert [f.title for f in source.report.failures] == ["Permission denied"]


def test_test_connection_reports_capabilities() -> None:
    conn = MagicMock()
    with (
        patch.object(RedshiftSource, "get_redshift_connection", return_value=conn),
        patch.object(RedshiftDataDictionary, "get_schemas", return_value=[]),
    ):
        report = RedshiftSource.test_connection(
            {"host_port": "localhost:5439", "database": DB}
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    assert report.capability_report[SourceCapability.SCHEMA_METADATA].capable


def test_test_connection_reports_schema_listing_failure() -> None:
    conn = MagicMock()
    with (
        patch.object(RedshiftSource, "get_redshift_connection", return_value=conn),
        patch.object(
            RedshiftDataDictionary,
            "get_schemas",
            side_effect=RuntimeError("permission denied"),
        ),
    ):
        report = RedshiftSource.test_connection(
            {"host_port": "localhost:5439", "database": DB}
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    schema_cap = report.capability_report[SourceCapability.SCHEMA_METADATA]
    assert not schema_cap.capable
    assert schema_cap.failure_reason


def test_test_connection_reports_connection_failure() -> None:
    with patch.object(
        RedshiftSource,
        "get_redshift_connection",
        side_effect=RuntimeError("timeout"),
    ):
        report = RedshiftSource.test_connection(
            {"host_port": "localhost:5439", "database": DB}
        )

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason


def test_create_builds_source_from_dict() -> None:
    source = RedshiftSource.create(
        {"host_port": "localhost:5439", "database": DB}, PipelineContext(run_id="x")
    )
    assert source.config.database == DB
    assert source.get_platform_instance_id() == "redshift"


# --------------------------------------------------------------------------- #
# Config warnings
# --------------------------------------------------------------------------- #


def test_warns_about_legacy_schema_pattern_matching() -> None:
    source = _source(
        _config(
            schema_pattern=AllowDenyPattern(deny=["internal"]),
            match_fully_qualified_names=False,
        )
    )
    source._warn_deprecated_configs()
    assert "Config option deprecation warning" in [
        w.title for w in source.report.warnings
    ]


def test_warns_when_column_usage_requested_without_usage_statistics() -> None:
    source = _source(_config(include_column_usage_stats=True))
    source._warn_deprecated_configs()
    assert "Config option has no effect" in [w.title for w in source.report.warnings]


def test_process_table_drops_tables_outside_table_pattern() -> None:
    source = _source(_config(table_pattern=AllowDenyPattern(deny=[f"{DB}.public.x"])))
    assert list(source._process_table(_table("x"), DB)) == []
    assert f"{DB}.public.x" in source.report.filtered


# --------------------------------------------------------------------------- #
# Schema fields
# --------------------------------------------------------------------------- #


def _column(name: str, data_type: str, **kw: object) -> RedshiftColumn:
    base: Dict[str, object] = dict(
        name=name,
        ordinal_position=1,
        data_type=data_type,
        comment=None,
        is_nullable=True,
    )
    base.update(kw)
    return RedshiftColumn(**base)  # type: ignore[arg-type]


def test_gen_schema_fields_types_tags_and_nested_columns() -> None:
    source = _source()
    fields = source.gen_schema_fields(
        [
            _column("id", "INTEGER", dist_key=True, sort_key=True),
            _column("label", "CHARACTER VARYING(256)"),
            _column("blob", "CUSTOMTYPE"),
            _column("tags", "ARRAY<STRING>"),
        ]
    )
    by_path = {f.fieldPath: f for f in fields}

    assert isinstance(by_path["id"].type.type, NumberTypeClass)
    assert by_path["id"].globalTags is not None
    assert {t.tag for t in by_path["id"].globalTags.tags} == {
        "urn:li:tag:DIST_KEY",
        "urn:li:tag:SORT_KEY",
    }
    # Precision-qualified postgres types resolve through the modified-type map.
    assert isinstance(by_path["label"].type.type, StringTypeClass)
    assert by_path["label"].nativeDataType == "CHARACTER VARYING(256)"
    # Unknown types degrade to NullType rather than failing the table.
    assert isinstance(by_path["blob"].type.type, NullTypeClass)
    # Complex hive types are expanded via the hive schema converter.
    array_fields = [f for f in fields if "tags" in f.fieldPath]
    assert array_fields
    assert isinstance(array_fields[0].type.type, ArrayTypeClass)


# --------------------------------------------------------------------------- #
# Dataset work units
# --------------------------------------------------------------------------- #


def test_gen_table_dataset_workunits_collects_external_table_properties() -> None:
    source = _source(_config(patch_custom_properties=False))
    table = _table(
        "ext",
        schema="spectrum",
        type="EXTERNAL_TABLE",
        location="s3://bucket/ext/",
        input_parameters="org.apache.hadoop.mapred.TextInputFormat",
        output_parameters="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        dist_style="EVEN",
        parameters="{}",
        serde_parameters="{}",
        created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        last_altered=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    props = _aspects(
        source.gen_table_dataset_workunits(table, DB, f"{DB}.spectrum.ext"),
        DatasetPropertiesClass,
    )
    assert len(props) == 1
    assert props[0].customProperties == {
        "table_type": "EXTERNAL_TABLE",
        "location": "s3://bucket/ext/",
        "input_parameters": "org.apache.hadoop.mapred.TextInputFormat",
        "output_parameters": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "dist_style": "EVEN",
        "parameters": "{}",
        "serde_parameters": "{}",
    }
    assert props[0].created is not None
    assert props[0].lastModified is not None


def test_gen_view_dataset_workunits_emits_view_properties() -> None:
    source = _source()
    view = RedshiftView(
        name="v", comment=None, created=None, ddl="select 1", materialized=True
    )
    view_props = _aspects(
        source.gen_view_dataset_workunits(view, DB, "public"), ViewPropertiesClass
    )
    assert len(view_props) == 1
    assert view_props[0].materialized
    assert view_props[0].viewLogic == "select 1"


def test_gen_dataset_workunits_emits_platform_instance_and_domain() -> None:
    source = _source(
        _config(
            platform_instance="prod_cluster",
            domain={"urn:li:domain:sales": AllowDenyPattern(allow=[f"{DB}.public.*"])},
        )
    )
    workunits = list(
        source.gen_dataset_workunits(
            table=_table("orders"), database=DB, schema="public", sub_type="Table"
        )
    )
    aspect_names = {
        wu.metadata.aspectName
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    }
    assert "dataPlatformInstance" in aspect_names
    domains = _aspects(workunits, DomainsClass)
    assert len(domains) == 1
    assert domains[0].domains == ["urn:li:domain:sales"]


# --------------------------------------------------------------------------- #
# Schema processing / caching
# --------------------------------------------------------------------------- #


def test_process_schema_emits_tables_and_views_with_their_columns() -> None:
    source = _source()
    schema = RedshiftSchema(name="public", database=DB, type="local")
    source.db_tables = {DB: {"public": [_table("orders")]}}
    source.db_views = {
        DB: {"public": [RedshiftView(name="v", comment=None, created=None, ddl="x")]}
    }
    columns = {
        "orders": [_column("id", "INTEGER")],
        "v": [_column("id", "INTEGER"), _column("n", "VARCHAR")],
    }
    with patch.object(
        source.data_dictionary, "get_columns_for_schema", return_value=columns
    ):
        workunits = list(source.process_schema(MagicMock(), DB, schema))

    schemas = {s.schemaName: s for s in _aspects(workunits, SchemaMetadataClass)}
    assert [f.fieldPath for f in schemas[f"{DB}.public.orders"].fields] == ["id"]
    assert [f.fieldPath for f in schemas[f"{DB}.public.v"].fields] == ["id", "n"]
    assert source.report.table_processed[f"{DB}.public"] == 1
    assert source.report.view_processed[f"{DB}.public"] == 1


def test_process_schema_skips_tables_and_views_when_disabled() -> None:
    source = _source(_config(include_tables=False, include_views=False))
    schema = RedshiftSchema(name="public", database=DB, type="local")
    source.db_tables = {DB: {"public": [_table("orders")]}}
    source.db_views = {DB: {"public": []}}
    with patch.object(
        source.data_dictionary, "get_columns_for_schema", return_value={}
    ):
        workunits = list(source.process_schema(MagicMock(), DB, schema))

    assert _aspects(workunits, SchemaMetadataClass) == []


def test_process_schemas_drops_schemas_outside_pattern() -> None:
    source = _source(_config(schema_pattern=AllowDenyPattern(deny=["internal"])))
    schemas = [
        RedshiftSchema(name="public", database=DB, type="local"),
        RedshiftSchema(name="internal", database=DB, type="local"),
    ]
    source.db_schemas = {DB: {}}
    with (
        patch.object(source.data_dictionary, "get_schemas", return_value=schemas),
        patch.object(source, "process_schema", return_value=iter([])),
    ):
        list(source.process_schemas(MagicMock(), DB))

    assert list(source.db_schemas[DB]) == ["public"]
    assert f"{DB}.internal" in source.report.filtered


def test_cache_tables_and_views_applies_schema_and_name_patterns() -> None:
    source = _source(
        _config(
            schema_pattern=AllowDenyPattern(deny=["hidden"]),
            table_pattern=AllowDenyPattern(deny=[f"{DB}.public.secret"]),
            view_pattern=AllowDenyPattern(deny=[f"{DB}.public.secret_view"]),
        )
    )
    source.db_tables = {DB: {}}
    source.db_views = {DB: {}}
    tables = {
        "public": [_table("orders"), _table("secret")],
        "hidden": [_table("x", schema="hidden")],
    }
    views = {
        "public": [
            RedshiftView(name="v", comment=None, created=None),
            RedshiftView(name="secret_view", comment=None, created=None),
        ],
        "hidden": [RedshiftView(name="hv", comment=None, created=None)],
    }
    with patch.object(
        source.data_dictionary, "get_tables_and_views", return_value=(tables, views)
    ):
        source.cache_tables_and_views(MagicMock(), DB)

    assert [t.name for t in source.db_tables[DB]["public"]] == ["orders"]
    assert [v.name for v in source.db_views[DB]["public"]] == ["v"]
    assert "hidden" not in source.db_tables[DB]
    assert "hidden" not in source.db_views[DB]
    assert source.report.table_filtered[f"{DB}.public"] == 1
    assert source.report.view_filtered[f"{DB}.public"] == 1


# --------------------------------------------------------------------------- #
# Usage / lineage / profiling orchestration
# --------------------------------------------------------------------------- #


def _usage_cursor(rows: List[tuple]) -> MagicMock:
    cursor = MagicMock()
    cursor.description = [
        ("userid",),
        ("username",),
        ("query",),
        ("querytxt",),
        ("tbl",),
        ("database",),
        ("schema",),
        ("table",),
        ("starttime",),
        ("endtime",),
    ]
    cursor.fetchmany.side_effect = [rows, []]
    return cursor


def test_extract_usage_attributes_reads_from_query_history() -> None:
    source = _source(
        _config(
            email_domain="example.com",
            include_usage_statistics=True,
            include_operational_stats=False,
        )
    )
    all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]] = {
        DB: {"public": [_table("orders"), _table("quiet")]}
    }
    cursor = _usage_cursor(
        [
            (
                100,
                "alice",
                1,
                "select * from public.orders",
                7,
                DB,
                "public",
                "orders",
                datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 10, 0, 1, tzinfo=timezone.utc),
            )
        ]
    )
    with patch.object(RedshiftDataDictionary, "get_query_result", return_value=cursor):
        workunits = list(source.extract_usage(MagicMock(), DB, all_tables))

    usage = {
        str(getattr(wu.metadata, "entityUrn", "")): wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DatasetUsageStatisticsClass)
    }
    orders = usage[source.gen_dataset_urn(f"{DB}.public.orders")]
    assert orders.totalSqlQueries == 1
    assert orders.userCounts is not None
    assert orders.userCounts[0].user == "urn:li:corpuser:alice"
    # Tables with no reads still get an (empty) usage aspect.
    assert usage[source.gen_dataset_urn(f"{DB}.public.quiet")].totalSqlQueries == 0
    assert DB in source.report.usage_extraction_sec


def _run_extract_metadata(source: RedshiftSource) -> List[MetadataWorkUnit]:
    with (
        patch.object(source, "cache_tables_and_views", return_value=None),
        patch.object(source, "process_schemas", return_value=iter([])),
        patch.object(source, "extract_usage", return_value=iter([])),
        patch.object(source, "extract_lineage_v2", return_value=iter([])),
    ):
        return list(source._extract_metadata(MagicMock(), DB))


def test_extract_metadata_backfills_empty_usage_in_column_usage_mode() -> None:
    source = _source(
        _config(
            email_domain="example.com",
            include_usage_statistics=True,
            include_column_usage_stats=True,
        )
    )
    source.db_tables = {DB: {"public": [_table("orders")]}}
    source.db_views = {DB: {}}
    workunits = _run_extract_metadata(source)
    usage = _aspects(workunits, DatasetUsageStatisticsClass)
    assert len(usage) == 1
    assert usage[0].totalSqlQueries == 0


def test_extract_metadata_runs_profiler_when_profiling_enabled() -> None:
    source = _source(_config(profiling={"enabled": True}))
    source.db_tables = {DB: {}}
    source.db_views = {DB: {}}
    with patch(
        "datahub.ingestion.source.redshift.redshift.RedshiftProfiler.get_workunits",
        return_value=iter([]),
    ) as profiler_get_workunits:
        _run_extract_metadata(source)
    profiler_get_workunits.assert_called_once_with(source.db_tables)


def _lineage_extractor_for(source: RedshiftSource) -> MagicMock:
    extractor = MagicMock()
    extractor.start_time = source.config.start_time
    extractor.end_time = source.config.end_time
    extractor.generate.return_value = iter([])
    return extractor


def test_extract_lineage_v2_warns_when_inbound_share_options_are_truncated() -> None:
    source = _source(_config(include_share_lineage=True, include_table_lineage=False))
    source.db = RedshiftDatabase(name=DB, type="shared", options="garbage")
    with patch.object(
        source.data_dictionary, "get_outbound_datashares", return_value=iter([])
    ):
        list(
            source.extract_lineage_v2(
                MagicMock(), DB, cast(MagicMock, _lineage_extractor_for(source))
            )
        )
    assert "Upstream lineage of inbound datashare will be missing" in [
        w.title for w in source.report.warnings
    ]


def test_extract_lineage_v2_feeds_datashare_lineage_into_aggregator() -> None:
    source = _source(_config(include_share_lineage=True))
    source.db = RedshiftDatabase(
        name=DB,
        type="shared",
        options='{"datashare_name":"s","datashare_producer_namespace":"ns"}',
    )
    source.db_tables = {DB: {"public": [_table("orders")]}}
    source.db_views = {DB: {}}
    mapping = KnownLineageMapping(
        upstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,producer.public.orders,PROD)",
        downstream_urn=source.gen_dataset_urn(f"{DB}.public.orders"),
    )
    extractor = _lineage_extractor_for(source)
    with (
        patch.object(
            source.data_dictionary, "get_outbound_datashares", return_value=iter([])
        ),
        patch.object(
            source.datashares_helper, "generate_lineage", return_value=iter([mapping])
        ),
        patch.object(source, "_should_ingest_lineage", return_value=False),
    ):
        list(source.extract_lineage_v2(MagicMock(), DB, cast(MagicMock, extractor)))

    extractor.aggregator.add.assert_called_once_with(mapping)
    # Skipped run: the per-database lineage build never happened.
    extractor.build.assert_not_called()


def test_extract_lineage_v2_updates_stateful_checkpoint() -> None:
    source = _source(_config(include_share_lineage=False))
    handler = MagicMock()
    handler.should_skip_this_run.return_value = False
    source.redundant_lineage_run_skip_handler = handler
    extractor = _lineage_extractor_for(source)
    source.db_tables = {DB: {}}
    source.db_views = {DB: {}}

    list(source.extract_lineage_v2(MagicMock(), DB, cast(MagicMock, extractor)))

    extractor.build.assert_called_once()
    handler.update_state.assert_called_once_with(
        source.config.start_time, source.config.end_time
    )
    assert DB in source.report.lineage_extraction_sec


def test_should_ingest_lineage_skips_already_covered_window() -> None:
    source = _source()
    handler = MagicMock()
    handler.should_skip_this_run.return_value = True
    source.redundant_lineage_run_skip_handler = handler
    assert not source._should_ingest_lineage()
    assert source.report.warnings


def test_extract_lineage_v2_emits_platform_resources_for_outbound_shares() -> None:
    source = _source(_config(include_share_lineage=True, include_table_lineage=False))
    source.db = RedshiftDatabase(name=DB, type="local")
    cursor = MagicMock()
    cursor.fetchall.return_value = [("OUTBOUND", "share1", "ns1", DB)]
    with (
        patch.object(RedshiftDataDictionary, "get_query_result", return_value=cursor),
        patch.object(source, "_should_ingest_lineage", return_value=False),
    ):
        workunits = list(
            source.extract_lineage_v2(
                MagicMock(), DB, cast(MagicMock, _lineage_extractor_for(source))
            )
        )
    assert source.report.outbound_shares_count == 1
    assert any(
        "urn:li:platformResource:" in str(getattr(wu.metadata, "entityUrn", ""))
        for wu in workunits
    )
    assert not _aspects(workunits, UpstreamLineageClass)
