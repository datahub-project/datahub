"""Fetch fallbacks and failure handling in SnowflakeSchemaGenerator.

Each object kind (database, schema, table, view, semantic view, stream, procedure,
Streamlit app) has a fetch path with a database-wide fast path, a per-schema
fallback, and distinct handling for permission errors versus everything else.
These tests pin down which failures abort the run, which only warn, and which
objects still get emitted when a side lookup (tags, keys, columns) fails.
"""

import logging
from unittest.mock import NonCallableMock, create_autospec, patch

import pytest

from datahub.ingestion.source.snowflake.constants import (
    GENERIC_PERMISSION_ERROR_KEY,
    SNOWFLAKE_DATABASE,
    SnowflakeObjectDomain,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeTableIdentifier,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_semantic_model import (
    SnowflakeSemanticModelMapper,
)
from datahub.ingestion.source.snowflake.snowflake_tag import SnowflakeTagExtractor
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    SchemaMetadataClass,
    StatusClass,
    TagPropertiesClass,
)
from tests.unit.snowflake.conftest import (
    DB,
    SCHEMA,
    as_mock,
    aspects,
    column,
    database,
    dataset_urn,
    dynamic_table,
    log_keys,
    make_aggregator,
    make_gen,
    procedure,
    schema,
    semantic_view,
    stream,
    streamlit_app,
    table,
    tag,
    view,
)

SCHEMA_GEN_LOGGER = "datahub.ingestion.source.snowflake.snowflake_schema_gen"


def _stub_tag_extractor(gen: SnowflakeSchemaGenerator) -> NonCallableMock:
    gen.tag_extractor = create_autospec(SnowflakeTagExtractor, instance=True)
    return as_mock(gen.tag_extractor)


# --- Databases ---------------------------------------------------------------


def test_listing_databases_failure_is_fatal_and_yields_nothing() -> None:
    gen = make_gen()
    as_mock(gen.data_dictionary).show_databases.side_effect = RuntimeError("down")

    assert list(gen.get_workunits_internal()) == []
    assert "Failed to list databases" in log_keys(gen.report.failures)


def test_no_readable_databases_is_a_permission_failure() -> None:
    gen = make_gen()
    dd = as_mock(gen.data_dictionary)
    dd.show_databases.return_value = [database(DB)]
    dd.get_databases.return_value = []

    assert list(gen.get_workunits_internal()) == []
    assert GENERIC_PERMISSION_ERROR_KEY in log_keys(gen.report.failures)


def test_information_schema_probe_skips_databases_without_usage(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``show databases`` lists everything; the first readable information_schema
    enumerates. The SNOWFLAKE database always shows up and is skipped quietly;
    any other unreadable database is called out as a MANAGE GRANTS symptom.
    """
    gen = make_gen()
    dd = as_mock(gen.data_dictionary)
    dd.get_databases.side_effect = [
        RuntimeError("no usage"),
        RuntimeError("no usage"),
        [database(DB)],
    ]

    with caplog.at_level(logging.INFO, logger=SCHEMA_GEN_LOGGER):
        databases = gen.get_databases_from_ischema(
            [database(SNOWFLAKE_DATABASE), database("LOCKED"), database(DB)]
        )

    assert [d.name for d in databases] == [DB]
    assert dd.get_databases.call_count == 3
    manage_grants_notes = [
        record for record in caplog.records if "MANAGE GRANTS" in record.getMessage()
    ]
    # Once for LOCKED; nothing for SNOWFLAKE, whose missing USAGE is expected.
    assert len(manage_grants_notes) == 1


def test_pushdown_patterns_keep_filtering_on_the_server() -> None:
    gen = make_gen(
        push_down_metadata_patterns=True,
        database_pattern={"deny": ["DENIED"]},
        schema_pattern={"deny": [".*\\.HIDDEN"]},
    )
    dd = as_mock(gen.data_dictionary)
    dd.show_databases.return_value = [database(DB)]
    dd.get_databases.return_value = [database(DB)]
    dd.get_schemas_for_database.return_value = [schema(SCHEMA)]
    dd.get_tables_for_database.return_value = {}
    dd.get_views_for_database.return_value = {}

    list(gen.get_workunits_internal())

    # The filters were rendered into SQL rather than applied in Python.
    assert "DENIED" in dd.get_databases.call_args.args[1]
    assert "HIDDEN" in dd.get_schemas_for_database.call_args.args[1]
    assert gen.report.filtered == []


def test_structured_property_templates_are_created_before_scanning() -> None:
    gen = make_gen(
        extract_tags_as_structured_properties=True,
        structured_properties_template_cache_invalidation_interval=0,
    )
    dd = as_mock(gen.data_dictionary)
    dd.get_all_tags.return_value = [tag()]
    dd.show_databases.return_value = []
    dd.get_databases.return_value = []

    with patch(f"{SCHEMA_GEN_LOGGER}.time.sleep"):
        workunits = list(gen.get_workunits_internal())

    assert [wu.get_urn() for wu in workunits] == [
        f"urn:li:structuredProperty:snowflake.{DB.lower()}.{SCHEMA.lower()}.pii"
    ]
    assert gen.report.num_structured_property_templates_created == 1


# --- Database and schema level -----------------------------------------------


def test_database_and_schema_tag_failures_do_not_block_containers() -> None:
    gen = make_gen(extract_tags="without_lineage")
    dd = as_mock(gen.data_dictionary)
    dd.get_schemas_for_database.return_value = [schema(SCHEMA)]
    dd.get_tables_for_database.return_value = {}
    dd.get_views_for_database.return_value = {}
    _stub_tag_extractor(gen).get_tags_on_object.side_effect = RuntimeError("tag refs")

    workunits = list(gen._process_database(database(DB)))

    containers = {wu.get_urn() for wu in workunits}
    assert len(containers) == 2
    assert {
        "Failed to get tags for database",
        "Failed to get tags for schema",
    } <= log_keys(gen.report.warnings)


def test_database_tags_are_emitted_as_tag_entities() -> None:
    gen = make_gen(extract_tags="without_lineage")
    as_mock(gen.data_dictionary).get_schemas_for_database.return_value = []
    _stub_tag_extractor(gen).get_tags_on_object.return_value = [tag("OWNER")]

    workunits = list(gen._process_database(database(DB)))

    tag_urns = list(aspects(workunits, TagPropertiesClass))
    assert len(tag_urns) == 1
    assert tag_urns[0].endswith("owner:yes")


def test_non_permission_schema_failure_warns_and_leaves_database_empty() -> None:
    gen = make_gen()
    db = database(DB)
    as_mock(gen.data_dictionary).get_schemas_for_database.side_effect = RuntimeError(
        "timeout"
    )

    gen.fetch_schemas_for_database(db, DB)

    assert db.schemas == []
    assert "Failed to get schemas for database" in log_keys(gen.report.warnings)


def test_empty_schema_is_reported_as_info() -> None:
    gen = make_gen()
    dd = as_mock(gen.data_dictionary)
    dd.get_tables_for_database.return_value = {}
    dd.get_views_for_database.return_value = {}

    list(gen._process_schema(schema(), DB, {}))

    assert "No tables/views/semantic views/streams found in schema" in log_keys(
        gen.report.infos
    )


# --- Tables and views ---------------------------------------------------------


def test_table_side_lookups_fail_independently_but_table_is_still_emitted() -> None:
    """Columns, keys and tags are looked up separately; each failure is its own
    warning and none of them stops the dataset itself from being emitted."""
    gen = make_gen(extract_tags="without_lineage")
    dd = as_mock(gen.data_dictionary)
    dd.get_columns_for_schema.side_effect = RuntimeError("columns")
    dd.get_pk_constraints_for_schema.side_effect = RuntimeError("pk")
    dd.get_fk_constraints_for_schema.side_effect = RuntimeError("fk")
    tags = _stub_tag_extractor(gen)
    tags.get_column_tags_for_table.side_effect = RuntimeError("ctags")
    tags.get_tags_on_object.side_effect = RuntimeError("tags")

    workunits = list(gen._process_table(table("T1"), schema(), DB))

    assert dataset_urn(gen, "T1") in aspects(workunits, StatusClass)
    assert aspects(workunits, SchemaMetadataClass)[dataset_urn(gen, "T1")].fields == []
    assert {
        "Failed to get columns for table",
        "Failed to get primary key for table",
        "Failed to get foreign keys for table",
        "Failed to get column tags for table",
        "Failed to get tags for table",
    } <= log_keys(gen.report.warnings)


def test_view_side_lookups_fail_independently_but_view_is_still_emitted() -> None:
    gen = make_gen(extract_tags="without_lineage")
    as_mock(gen.data_dictionary).get_columns_for_schema.side_effect = RuntimeError(
        "columns"
    )
    _stub_tag_extractor(gen).get_tags_on_object.side_effect = RuntimeError("tags")

    workunits = list(gen._process_view(view("V1"), schema(), DB))

    assert dataset_urn(gen, "V1") in aspects(workunits, StatusClass)
    assert {
        "Failed to get columns for view",
        "Failed to get tags for view",
    } <= log_keys(gen.report.warnings)


def test_table_listing_errors_warn_but_permission_errors_propagate() -> None:
    gen = make_gen()
    dd = as_mock(gen.data_dictionary)
    dd.get_tables_for_database.return_value = None
    dd.get_tables_for_schema.side_effect = RuntimeError("timeout")

    assert gen.fetch_tables_for_schema(schema(), DB, SCHEMA) == []
    assert "Failed to get tables for schema" in log_keys(gen.report.warnings)

    dd.get_tables_for_schema.side_effect = SnowflakePermissionError("denied")
    with pytest.raises(SnowflakePermissionError):
        gen.fetch_tables_for_schema(schema(), DB, SCHEMA)


def test_view_listing_falls_back_per_schema_and_filters_in_python() -> None:
    gen = make_gen(view_pattern={"deny": [".*\\.hidden_.*"]})
    dd = as_mock(gen.data_dictionary)
    dd.get_views_for_database.return_value = None
    dd.get_views_for_schema.return_value = [view("V1"), view("HIDDEN_V")]
    snowflake_schema = schema()

    views = gen.fetch_views_for_schema(snowflake_schema, DB, SCHEMA)

    assert [v.name for v in views] == ["V1"]
    assert snowflake_schema.views == ["V1"]
    assert gen.report.num_get_views_for_schema_queries == 1

    dd.get_views_for_schema.side_effect = RuntimeError("timeout")
    assert gen.fetch_views_for_schema(schema(), DB, SCHEMA) == []
    dd.get_views_for_schema.side_effect = SnowflakePermissionError("denied")
    with pytest.raises(SnowflakePermissionError):
        gen.fetch_views_for_schema(schema(), DB, SCHEMA)


def test_table_listing_falls_back_per_schema_when_database_wide_fails() -> None:
    gen = make_gen()
    dd = as_mock(gen.data_dictionary)
    dd.get_tables_for_database.return_value = None
    dd.get_tables_for_schema.return_value = [table("T1")]

    assert [t.name for t in gen.get_tables_for_schema(SCHEMA, DB)] == ["T1"]
    assert gen.report.num_get_tables_for_schema_queries == 1


# --- Dynamic tables -----------------------------------------------------------


def test_dynamic_table_without_definition_uses_inputs_minus_itself() -> None:
    """Without DDL the SHOW-reported inputs are the only lineage source, and the
    table's own name (Snowflake lists it among its inputs) must be dropped."""
    aggregator = make_aggregator()
    gen = make_gen(aggregator=aggregator, include_technical_schema=False)
    self_name = f"{DB}.{SCHEMA}.DT"
    dt = dynamic_table("DT", "", [f"{DB}.{SCHEMA}.SRC", self_name])

    list(gen._process_tables([dt], schema(), DB, SCHEMA))

    aggregator.add_view_definition.assert_not_called()
    aggregator.add_known_lineage_mapping.assert_called_once_with(
        upstream_urn=dataset_urn(gen, "SRC"),
        downstream_urn=dataset_urn(gen, "DT"),
        lineage_type=DatasetLineageTypeClass.VIEW,
    )
    assert gen.report.num_dynamic_tables_missing_definition == 1
    assert gen.dynamic_table_identifiers == {self_name.lower()}


def test_dynamic_table_lineage_registration_failure_is_a_warning() -> None:
    aggregator = make_aggregator()
    aggregator.add_view_definition.side_effect = RuntimeError("parser")
    gen = make_gen(aggregator=aggregator, include_technical_schema=False)
    dt = dynamic_table("DT", "select * from src", [f"{DB}.{SCHEMA}.SRC"])

    list(gen._process_tables([dt], schema(), DB, SCHEMA))

    assert "Failed to register dynamic table lineage" in log_keys(gen.report.warnings)


def test_unknown_dynamic_table_input_kind_falls_back_to_table() -> None:
    assert (
        SnowflakeSchemaGenerator._resolve_input_kind("MATERIALIZED_VIEW")
        == SnowflakeObjectDomain.MATERIALIZED_VIEW
    )
    assert (
        SnowflakeSchemaGenerator._resolve_input_kind("SOMETHING_NEW")
        == SnowflakeObjectDomain.TABLE
    )


# --- Semantic views -----------------------------------------------------------


def test_semantic_views_are_filtered_and_recorded_on_schema() -> None:
    gen = make_gen(
        semantic_views={"enabled": True},
        semantic_view_pattern={"deny": [".*\\.internal_.*"]},
    )
    dd = as_mock(gen.data_dictionary)
    dd.get_semantic_views_for_database.return_value = {
        SCHEMA: [semantic_view("SV"), semantic_view("INTERNAL_SV")]
    }
    snowflake_schema = schema()

    kept = gen.fetch_semantic_views_for_schema(snowflake_schema, DB)

    assert [sv.name for sv in kept] == ["SV"]
    assert snowflake_schema.semantic_views == ["SV"]
    assert f"{DB}.{SCHEMA}.INTERNAL_SV".lower() in gen.report.filtered


def test_semantic_view_listing_falls_back_to_information_schema() -> None:
    gen = make_gen(semantic_views={"enabled": True})
    dd = as_mock(gen.data_dictionary)
    dd.get_semantic_views_for_database.return_value = None
    dd.get_semantic_views_for_schema_using_information_schema.return_value = [
        semantic_view("SV")
    ]

    assert [sv.name for sv in gen.get_semantic_views_for_schema(SCHEMA, DB)] == ["SV"]


def test_semantic_view_listing_errors_are_soft_except_permission_errors() -> None:
    gen = make_gen(semantic_views={"enabled": True})
    dd = as_mock(gen.data_dictionary)
    dd.get_semantic_views_for_database.side_effect = RuntimeError("unsupported")

    assert gen.fetch_semantic_views_for_schema(schema(), DB) == []
    assert "Could not fetch semantic views for schema" in log_keys(gen.report.infos)

    dd.get_semantic_views_for_database.side_effect = SnowflakePermissionError("no")
    with pytest.raises(SnowflakePermissionError):
        gen.fetch_semantic_views_for_schema(schema(), DB)


def test_semantic_view_without_base_tables_warns_and_skips_lineage() -> None:
    aggregator = make_aggregator()
    gen = make_gen(aggregator=aggregator, semantic_views={"enabled": True})
    as_mock(gen.data_dictionary).get_columns_for_schema.return_value = {}
    sv = semantic_view("SV", columns=[column("DIM")])

    workunits = list(gen._process_semantic_views([sv], schema(), DB, SCHEMA))

    assert dataset_urn(gen, "SV") in aspects(workunits, SchemaMetadataClass)
    assert "Semantic view lineage skipped" in log_keys(gen.report.warnings)
    # The YAML definition is still registered for reference.
    aggregator.add_view_definition.assert_called_once()
    aggregator.add_known_query_lineage.assert_not_called()


def test_semantic_view_tag_lookup_failures_are_tolerated() -> None:
    gen = make_gen(semantic_views={"enabled": True}, extract_tags="without_lineage")
    as_mock(gen.data_dictionary).get_columns_for_schema.side_effect = RuntimeError(
        "no columns"
    )
    tags = _stub_tag_extractor(gen)
    tags.get_column_tags_for_table.side_effect = RuntimeError("ctags")
    tags.get_tags_on_object.side_effect = RuntimeError("tags")
    sv = semantic_view("SV")

    workunits = list(gen._process_semantic_view(sv, schema(), DB))

    assert sv.columns == []
    assert sv.column_tags == {}
    assert sv.tags is None
    assert dataset_urn(gen, "SV") in aspects(workunits, StatusClass)
    assert gen.report.warnings == []


def test_semantic_model_mode_emits_tag_entities_and_tolerates_lineage_errors() -> None:
    """In semanticModel mode the mapper owns the entity; the generator still
    emits the tag entities and hands the mapper an empty lineage list when the
    column-lineage step blows up, rather than dropping the model."""
    gen = make_gen(
        semantic_views={
            "enabled": True,
            "emit_semantic_model_entities": True,
            "column_lineage": True,
        },
        extract_tags="without_lineage",
    )
    tags = _stub_tag_extractor(gen)
    tags.get_column_tags_for_table.return_value = {"DIM": [tag("COL")]}
    tags.get_tags_on_object.return_value = [tag("VIEW")]
    gen.semantic_model_mapper = create_autospec(
        SnowflakeSemanticModelMapper, instance=True
    )
    mapper = as_mock(gen.semantic_model_mapper)
    mapper.gen_workunits.return_value = iter([])
    sv = semantic_view(
        "SV",
        columns=[column("DIM")],
        base_tables=[SnowflakeTableIdentifier(DB, SCHEMA, "BASE")],
        column_table_mappings={"DIM": ["BASE"]},
        logical_to_physical_table={"BASE": (DB, SCHEMA, "BASE")},
    )

    with patch.object(
        gen, "_generate_column_lineage_for_semantic_view", side_effect=RuntimeError
    ):
        workunits = list(gen._process_semantic_views([sv], schema(), DB, SCHEMA))

    tag_urns = sorted(aspects(workunits, TagPropertiesClass))
    assert len(tag_urns) == 2
    assert "Failed to generate column lineage for semantic view" in log_keys(
        gen.report.warnings
    )
    mapper.gen_workunits.assert_called_once()
    assert mapper.gen_workunits.call_args.kwargs["fine_grained_lineages"] == []
    assert gen.report.num_semantic_model_lineage_edges_scanned == 1


# --- Streams, procedures, Streamlit apps -------------------------------------


def test_stream_listing_falls_back_per_schema_and_permission_errors_propagate() -> None:
    gen = make_gen(include_streams=True, stream_pattern={"deny": [".*\\.old_.*"]})
    dd = as_mock(gen.data_dictionary)
    dd.get_streams_for_database.return_value = None
    dd.get_streams_for_schema_using_show.return_value = [
        stream("S1", "SRC"),
        stream("OLD_S", "SRC"),
    ]
    snowflake_schema = schema()

    streams = gen.fetch_streams_for_schema(snowflake_schema, DB)

    assert [s.name for s in streams] == ["S1"]
    assert snowflake_schema.streams == ["S1"]

    dd.get_streams_for_schema_using_show.side_effect = RuntimeError("timeout")
    assert gen.fetch_streams_for_schema(schema(), DB) == []
    assert "Failed to get streams for schema" in log_keys(gen.report.warnings)
    dd.get_streams_for_schema_using_show.side_effect = SnowflakePermissionError("no")
    with pytest.raises(SnowflakePermissionError):
        gen.fetch_streams_for_schema(schema(), DB)


def test_stream_processing_failure_is_a_warning() -> None:
    gen = make_gen(include_streams=True)
    as_mock(gen.data_dictionary).get_columns_for_schema.side_effect = RuntimeError(
        "columns"
    )

    assert list(gen._process_streams([stream("S1", "SRC")], schema(), DB)) == []
    assert "Failed to get columns for stream:" in log_keys(gen.report.warnings)


def test_procedure_processing_failure_is_a_warning() -> None:
    gen = make_gen(include_procedures=True)

    with patch(
        f"{SCHEMA_GEN_LOGGER}.generate_procedure_workunits",
        side_effect=RuntimeError("bad definition"),
    ):
        workunits = list(gen._process_procedures([procedure("P")], schema(), DB))

    # The procedure flow container is still emitted even though the procedure failed.
    assert workunits
    assert all(wu.get_urn().startswith("urn:li:dataFlow:") for wu in workunits)
    assert "Failed to ingest stored procedure" in log_keys(gen.report.warnings)


def test_streamlit_apps_are_filtered_and_emitted_as_dashboards() -> None:
    gen = make_gen(
        include_streamlits=True, streamlit_pattern={"deny": [".*\\.INTERNAL_.*"]}
    )
    dd = as_mock(gen.data_dictionary)
    dd.get_streamlit_apps_for_database.return_value = {
        SCHEMA: [streamlit_app("APP"), streamlit_app("INTERNAL_APP")]
    }

    apps = gen.fetch_streamlit_apps(schema(), DB)
    assert [a.name for a in apps] == ["APP"]
    assert f"{DB}.{SCHEMA}.INTERNAL_APP" in gen.report.filtered

    workunits = list(gen._process_streamlit_apps(apps))
    dashboard_urns = {
        wu.get_urn() for wu in workunits if wu.get_urn().startswith("urn:li:dashboard:")
    }
    assert len(dashboard_urns) == 1

    dd.get_streamlit_apps_for_database.side_effect = SnowflakePermissionError("no")
    assert gen.fetch_streamlit_apps(schema(), DB) == []
    dd.get_streamlit_apps_for_database.side_effect = RuntimeError("boom")
    assert gen.fetch_streamlit_apps(schema(), DB) == []
    assert {
        "Permission denied for Streamlit apps",
        "Failed to get Streamlit apps",
    } <= log_keys(gen.report.warnings)


def test_schema_scan_dispatches_to_every_enabled_object_kind() -> None:
    gen = make_gen(
        include_streams=True,
        include_procedures=True,
        include_streamlits=True,
        semantic_views={"enabled": True},
        extract_tags="without_lineage",
    )
    dd = as_mock(gen.data_dictionary)
    dd.get_tables_for_database.return_value = {SCHEMA: [table("T1")]}
    dd.get_views_for_database.return_value = {}
    dd.get_semantic_views_for_database.return_value = {}
    dd.get_streams_for_database.return_value = {}
    dd.get_procedures_for_database.return_value = {}
    dd.get_streamlit_apps_for_database.return_value = {}
    tags = _stub_tag_extractor(gen)
    tags.get_tags_on_object.return_value = [tag("SCHEMA_TAG")]
    tags.get_column_tags_for_table.return_value = {}
    snowflake_schema = schema()

    workunits = list(gen._process_schema(snowflake_schema, DB, {}))

    assert snowflake_schema.tables == ["T1"]
    assert gen.report.num_get_streams_for_schema_queries == 1
    # Schema-level tags are emitted as tag entities after the schema's datasets.
    assert any(
        u.endswith("schema_tag:yes") for u in aspects(workunits, TagPropertiesClass)
    )


def test_external_table_ddl_lineage_failure_is_a_warning() -> None:
    gen = make_gen()
    as_mock(gen.connection).query.side_effect = RuntimeError("show failed")

    assert list(gen._external_tables_ddl_lineage([])) == []
    assert "External table ddl lineage extraction failed" in log_keys(
        gen.report.warnings
    )
