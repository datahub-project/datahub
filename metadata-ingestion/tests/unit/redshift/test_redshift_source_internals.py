"""RedshiftSource behaviour that sits between the Redshift system tables and the
emitted metadata: connection handling, schema field typing, dataset property
composition and the lineage/usage/profiling orchestration in _extract_metadata.

The redshift_connector boundary is stubbed; everything downstream runs for real.
"""

from datetime import datetime, timezone
from typing import Tuple
from unittest.mock import MagicMock, patch

import pytest
import redshift_connector

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.redshift.redshift import RedshiftSource
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftDatabase,
    RedshiftDataDictionary,
    RedshiftSchema,
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
from tests.unit.redshift.conftest import (
    DB,
    HOST_PORT,
    AllTables,
    aspects_of,
    make_column,
    make_config,
    make_cursor,
    make_source,
    make_table,
    run_extract_metadata,
    stub_lineage_extractor,
)

# --------------------------------------------------------------------------- #
# Connection handling
# --------------------------------------------------------------------------- #


def test_get_redshift_connection_passes_config_and_enables_autocommit() -> None:
    config = make_config(
        username="u", password="p", extra_client_options={"ssl": False}
    )
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
    source = make_source()
    with patch.object(
        RedshiftSource,
        "get_redshift_connection",
        side_effect=redshift_connector.Error(error_message),
    ):
        assert source._try_get_redshift_connection(source.config) is None

    assert expected_title in {f.title for f in source.report.failures}


def test_get_workunits_internal_short_circuits_without_connection() -> None:
    source = make_source()
    with patch.object(source, "_try_get_redshift_connection", return_value=None):
        assert list(source.get_workunits_internal()) == []
    assert source.db is None


def test_get_workunits_internal_reports_driver_errors_as_failure() -> None:
    """A driver error after the database container was emitted is reported as
    a structured failure instead of propagating out of the generator."""
    source = make_source()
    with (
        patch.object(source, "_try_get_redshift_connection", return_value=MagicMock()),
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

    assert workunits
    assert "Permission denied" in {f.title for f in source.report.failures}


def test_test_connection_reports_capabilities() -> None:
    with (
        patch.object(
            RedshiftSource, "get_redshift_connection", return_value=MagicMock()
        ),
        patch.object(RedshiftDataDictionary, "get_schemas", return_value=[]),
    ):
        report = RedshiftSource.test_connection(
            {"host_port": HOST_PORT, "database": DB}
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    assert report.capability_report[SourceCapability.SCHEMA_METADATA].capable


def test_test_connection_reports_schema_listing_failure() -> None:
    with (
        patch.object(
            RedshiftSource, "get_redshift_connection", return_value=MagicMock()
        ),
        patch.object(
            RedshiftDataDictionary,
            "get_schemas",
            side_effect=RuntimeError("permission denied"),
        ),
    ):
        report = RedshiftSource.test_connection(
            {"host_port": HOST_PORT, "database": DB}
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
            {"host_port": HOST_PORT, "database": DB}
        )

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason


def test_create_builds_source_from_dict() -> None:
    source = RedshiftSource.create(
        {"host_port": HOST_PORT, "database": DB}, PipelineContext(run_id="x")
    )
    assert source.config.database == DB
    assert source.get_platform_instance_id() == "redshift"


def test_process_table_drops_tables_outside_table_pattern() -> None:
    source = make_source(table_pattern=AllowDenyPattern(deny=[f"{DB}.public.x"]))
    assert list(source._process_table(make_table("x"), DB)) == []
    assert f"{DB}.public.x" in source.report.filtered


# --------------------------------------------------------------------------- #
# Schema fields
# --------------------------------------------------------------------------- #


def test_gen_schema_fields_types_tags_and_nested_columns() -> None:
    """Precision-qualified postgres types resolve through the modified-type
    map, unknown types degrade to NullType rather than failing the table, and
    hive-style complex types are expanded into nested fields."""
    source = make_source()
    fields = source.gen_schema_fields(
        [
            make_column("id", "INTEGER", dist_key=True, sort_key=True),
            make_column("label", "CHARACTER VARYING(256)"),
            make_column("blob", "CUSTOMTYPE"),
            make_column("tags", "ARRAY<STRING>"),
        ]
    )
    by_path = {f.fieldPath: f for f in fields}

    assert isinstance(by_path["id"].type.type, NumberTypeClass)
    assert by_path["id"].globalTags is not None
    assert {t.tag for t in by_path["id"].globalTags.tags} == {
        "urn:li:tag:DIST_KEY",
        "urn:li:tag:SORT_KEY",
    }
    assert isinstance(by_path["label"].type.type, StringTypeClass)
    assert by_path["label"].nativeDataType == "CHARACTER VARYING(256)"
    assert isinstance(by_path["blob"].type.type, NullTypeClass)
    array_fields = [f for f in fields if "tags" in f.fieldPath]
    assert array_fields
    assert isinstance(array_fields[0].type.type, ArrayTypeClass)


# --------------------------------------------------------------------------- #
# Dataset work units
# --------------------------------------------------------------------------- #


def test_gen_table_dataset_workunits_collects_external_table_properties() -> None:
    source = make_source(patch_custom_properties=False)
    table = make_table(
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
    props = aspects_of(
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
    source = make_source()
    view = RedshiftView(
        name="v", comment=None, created=None, ddl="select 1", materialized=True
    )
    view_props = aspects_of(
        source.gen_view_dataset_workunits(view, DB, "public"), ViewPropertiesClass
    )
    assert len(view_props) == 1
    assert view_props[0].materialized
    assert view_props[0].viewLogic == "select 1"


def test_gen_dataset_workunits_emits_platform_instance_and_domain() -> None:
    source = make_source(
        platform_instance="prod_cluster",
        domain={"urn:li:domain:sales": AllowDenyPattern(allow=[f"{DB}.public.*"])},
    )
    workunits = list(
        source.gen_dataset_workunits(
            table=make_table("orders"), database=DB, schema="public", sub_type="Table"
        )
    )
    aspect_names = {
        wu.metadata.aspectName
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    }
    assert "dataPlatformInstance" in aspect_names
    domains = aspects_of(workunits, DomainsClass)
    assert len(domains) == 1
    assert domains[0].domains == ["urn:li:domain:sales"]


# --------------------------------------------------------------------------- #
# Schema processing / caching
# --------------------------------------------------------------------------- #


def test_process_schema_emits_tables_and_views_with_their_columns() -> None:
    source = make_source()
    schema = RedshiftSchema(name="public", database=DB, type="local")
    source.db_tables = {DB: {"public": [make_table("orders")]}}
    source.db_views = {
        DB: {"public": [RedshiftView(name="v", comment=None, created=None, ddl="x")]}
    }
    columns = {
        "orders": [make_column("id", "INTEGER")],
        "v": [make_column("id", "INTEGER"), make_column("n", "VARCHAR")],
    }
    with patch.object(
        source.data_dictionary, "get_columns_for_schema", return_value=columns
    ):
        workunits = list(source.process_schema(MagicMock(), DB, schema))

    schemas = {s.schemaName: s for s in aspects_of(workunits, SchemaMetadataClass)}
    assert [f.fieldPath for f in schemas[f"{DB}.public.orders"].fields] == ["id"]
    assert [f.fieldPath for f in schemas[f"{DB}.public.v"].fields] == ["id", "n"]
    assert source.report.table_processed[f"{DB}.public"] == 1
    assert source.report.view_processed[f"{DB}.public"] == 1


def test_process_schema_skips_tables_and_views_when_disabled() -> None:
    source = make_source(include_tables=False, include_views=False)
    schema = RedshiftSchema(name="public", database=DB, type="local")
    source.db_tables = {DB: {"public": [make_table("orders")]}}
    source.db_views = {DB: {"public": []}}
    with patch.object(
        source.data_dictionary, "get_columns_for_schema", return_value={}
    ):
        workunits = list(source.process_schema(MagicMock(), DB, schema))

    assert aspects_of(workunits, SchemaMetadataClass) == []


def test_process_schemas_drops_schemas_outside_pattern() -> None:
    source = make_source(schema_pattern=AllowDenyPattern(deny=["internal"]))
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
    source = make_source(
        schema_pattern=AllowDenyPattern(deny=["hidden"]),
        table_pattern=AllowDenyPattern(deny=[f"{DB}.public.secret"]),
        view_pattern=AllowDenyPattern(deny=[f"{DB}.public.secret_view"]),
    )
    source.db_tables = {DB: {}}
    source.db_views = {DB: {}}
    tables = {
        "public": [make_table("orders"), make_table("secret")],
        "hidden": [make_table("x", schema="hidden")],
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

USAGE_COLUMNS = [
    "userid",
    "username",
    "query",
    "querytxt",
    "tbl",
    "database",
    "schema",
    "table",
    "starttime",
    "endtime",
]


def test_extract_usage_attributes_reads_from_query_history() -> None:
    """The default (stl_scan) usage path attributes each history row to its
    table, and tables with no reads in the window still get an empty usage
    aspect so their popularity is reported as zero rather than missing."""
    source = make_source(
        email_domain="example.com",
        include_usage_statistics=True,
        include_operational_stats=False,
    )
    all_tables: AllTables = {
        DB: {"public": [make_table("orders"), make_table("quiet")]}
    }
    row: Tuple = (
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
    with patch.object(
        RedshiftDataDictionary,
        "get_query_result",
        return_value=make_cursor(USAGE_COLUMNS, [row]),
    ):
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
    assert usage[source.gen_dataset_urn(f"{DB}.public.quiet")].totalSqlQueries == 0
    assert DB in source.report.usage_extraction_sec


def test_extract_metadata_backfills_empty_usage_in_column_usage_mode() -> None:
    """In column-usage (v2) mode the lineage aggregator owns usage; the source
    must still wrap its output so tables with no queries get an empty usage
    aspect for the window."""
    source = make_source(
        email_domain="example.com",
        include_usage_statistics=True,
        include_column_usage_stats=True,
    )
    source.db_tables = {DB: {"public": [make_table("orders")]}}
    source.db_views = {DB: {}}
    usage = aspects_of(
        run_extract_metadata(source).workunits, DatasetUsageStatisticsClass
    )
    assert len(usage) == 1
    assert usage[0].totalSqlQueries == 0


def test_extract_metadata_runs_profiler_when_profiling_enabled() -> None:
    source = make_source(profiling={"enabled": True})
    source.db_tables = {DB: {}}
    source.db_views = {DB: {}}
    with patch(
        "datahub.ingestion.source.redshift.redshift.RedshiftProfiler.get_workunits",
        return_value=iter([]),
    ) as profiler_get_workunits:
        run_extract_metadata(source)
    profiler_get_workunits.assert_called_once_with(source.db_tables)


def test_extract_lineage_v2_warns_when_inbound_share_options_are_truncated() -> None:
    source = make_source(include_share_lineage=True, include_table_lineage=False)
    source.db = RedshiftDatabase(name=DB, type="shared", options="garbage")
    with patch.object(
        source.data_dictionary, "get_outbound_datashares", return_value=iter([])
    ):
        list(source.extract_lineage_v2(MagicMock(), DB, stub_lineage_extractor(source)))
    assert "Upstream lineage of inbound datashare will be missing" in {
        w.title for w in source.report.warnings
    }


def test_extract_lineage_v2_feeds_datashare_lineage_into_aggregator() -> None:
    """Datashare lineage is definition-level and is registered with the
    aggregator even when the audit-log window is skipped as already covered."""
    source = make_source(include_share_lineage=True)
    source.db = RedshiftDatabase(
        name=DB,
        type="shared",
        options='{"datashare_name":"s","datashare_producer_namespace":"ns"}',
    )
    source.db_tables = {DB: {"public": [make_table("orders")]}}
    source.db_views = {DB: {}}
    mapping = KnownLineageMapping(
        upstream_urn="urn:li:dataset:(urn:li:dataPlatform:redshift,producer.public.orders,PROD)",
        downstream_urn=source.gen_dataset_urn(f"{DB}.public.orders"),
    )
    extractor = stub_lineage_extractor(source)
    with (
        patch.object(
            source.data_dictionary, "get_outbound_datashares", return_value=iter([])
        ),
        patch.object(
            source.datashares_helper, "generate_lineage", return_value=iter([mapping])
        ),
        patch.object(source, "_should_ingest_lineage", return_value=False),
    ):
        list(source.extract_lineage_v2(MagicMock(), DB, extractor))

    extractor.aggregator.add.assert_called_once_with(mapping)
    extractor.build.assert_not_called()


def test_extract_lineage_v2_updates_stateful_checkpoint(
    lineage_skip_handler: MagicMock,
) -> None:
    source = make_source(include_share_lineage=False)
    lineage_skip_handler.should_skip_this_run.return_value = False
    source.redundant_lineage_run_skip_handler = lineage_skip_handler
    extractor = stub_lineage_extractor(source)
    source.db_tables = {DB: {}}
    source.db_views = {DB: {}}

    list(source.extract_lineage_v2(MagicMock(), DB, extractor))

    extractor.build.assert_called_once()
    lineage_skip_handler.update_state.assert_called_once_with(
        source.config.start_time, source.config.end_time
    )
    assert DB in source.report.lineage_extraction_sec


def test_should_ingest_lineage_skips_already_covered_window(
    lineage_skip_handler: MagicMock,
) -> None:
    source = make_source()
    lineage_skip_handler.should_skip_this_run.return_value = True
    source.redundant_lineage_run_skip_handler = lineage_skip_handler
    assert not source._should_ingest_lineage()
    assert any("lineage-extraction" in w.context for w in source.report.warnings)


def test_extract_lineage_v2_emits_platform_resources_for_outbound_shares() -> None:
    source = make_source(include_share_lineage=True, include_table_lineage=False)
    source.db = RedshiftDatabase(name=DB, type="local")
    cursor = make_cursor(
        ["share_type", "share_name", "producer_namespace", "source_database"],
        [("OUTBOUND", "share1", "ns1", DB)],
    )
    with (
        patch.object(RedshiftDataDictionary, "get_query_result", return_value=cursor),
        patch.object(source, "_should_ingest_lineage", return_value=False),
    ):
        workunits = list(
            source.extract_lineage_v2(MagicMock(), DB, stub_lineage_extractor(source))
        )
    assert source.report.outbound_shares_count == 1
    assert any(
        "urn:li:platformResource:" in str(getattr(wu.metadata, "entityUrn", ""))
        for wu in workunits
    )
    assert not aspects_of(workunits, UpstreamLineageClass)
