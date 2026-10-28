"""Fetch fallbacks and failure handling in SnowflakeSchemaGenerator.

Each object kind (database, schema, table, view, semantic view, stream, procedure,
Streamlit app) has a fetch path with a database-wide fast path, a per-schema
fallback, and distinct handling for permission errors versus everything else.
These tests pin down which failures abort the run, which only warn, and which
objects still get emitted when a side lookup (tags, keys, columns) fails.
"""

from typing import List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.snowflake.constants import (
    SNOWFLAKE_DATABASE,
    SnowflakeObjectDomain,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDynamicTable,
    SnowflakeDynamicTableInput,
    SnowflakeSemanticView,
    SnowflakeStream,
    SnowflakeStreamlitApp,
    SnowflakeTableIdentifier,
    SnowflakeTag,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    SchemaMetadataClass,
    StatusClass,
    TagPropertiesClass,
)
from tests.unit.snowflake.test_snowflake_schema_gen_processing import (  # type: ignore[import-untyped]
    CREATED,
    DB,
    SCHEMA,
    _as_mock,
    _aspects,
    _column,
    _database,
    _make_gen,
    _schema,
    _table,
    _urn,
    _view,
)


def _semantic_view(name: str, **overrides: object) -> SnowflakeSemanticView:
    return SnowflakeSemanticView(
        name=name,
        comment=None,
        created=CREATED,
        last_altered=None,
        view_definition="create semantic view ...",
        **overrides,  # type: ignore[arg-type]
    )


def _stream(name: str, source: str) -> SnowflakeStream:
    return SnowflakeStream(
        name=name,
        created=CREATED,
        owner="OWNER",
        source_type="Table",
        type="DELTA",
        stale="false",
        mode="DEFAULT",
        invalid_reason="",
        owner_role_type="ROLE",
        database_name=DB,
        schema_name=SCHEMA,
        table_name=f"{DB}.{SCHEMA}.{source}",
        comment=None,
        stale_after=CREATED,
    )


def _app(name: str) -> SnowflakeStreamlitApp:
    return SnowflakeStreamlitApp(
        name=name,
        created=CREATED,
        owner="OWNER",
        database_name=DB,
        schema_name=SCHEMA,
        title=None,
        comment="an app",
        url_id="abc123",
        owner_role_type="ROLE",
    )


def _procedure(name: str) -> BaseProcedure:
    return BaseProcedure(
        name=name,
        procedure_definition="begin return 1; end",
        created=None,
        last_altered=None,
        comment=None,
        argument_signature="()",
        return_type="NUMBER",
        language="SQL",
        extra_properties=None,
    )


def _tag(name: str = "PII") -> SnowflakeTag:
    return SnowflakeTag(database=DB, schema=SCHEMA, name=name, value="yes")


# --- Databases ---------------------------------------------------------------


def test_listing_databases_failure_is_fatal_and_yields_nothing() -> None:
    gen = _make_gen()
    _as_mock(gen.data_dictionary).show_databases.side_effect = RuntimeError("down")

    assert list(gen.get_workunits_internal()) == []
    assert len(gen.report.failures) == 1


def test_no_readable_databases_is_a_permission_failure() -> None:
    gen = _make_gen()
    dd = _as_mock(gen.data_dictionary)
    dd.show_databases.return_value = [_database(DB)]
    dd.get_databases.return_value = []

    assert list(gen.get_workunits_internal()) == []
    assert len(gen.report.failures) == 1


def test_information_schema_probe_skips_databases_without_usage() -> None:
    # `show databases` lists everything; the information_schema of the first
    # readable database is what actually enumerates. SNOWFLAKE always shows up
    # and is skipped quietly; any other unreadable database is noted.
    gen = _make_gen()
    dd = _as_mock(gen.data_dictionary)
    dd.get_databases.side_effect = [
        RuntimeError("no usage"),
        RuntimeError("no usage"),
        [_database(DB)],
    ]

    databases = gen.get_databases_from_ischema(
        [_database(SNOWFLAKE_DATABASE), _database("LOCKED"), _database(DB)]
    )

    assert [d.name for d in databases] == [DB]
    assert dd.get_databases.call_count == 3


def test_pushdown_patterns_keep_filtering_on_the_server() -> None:
    gen = _make_gen(
        push_down_metadata_patterns=True,
        database_pattern={"deny": ["DENIED"]},
        schema_pattern={"deny": [".*\\.HIDDEN"]},
    )
    dd = _as_mock(gen.data_dictionary)
    dd.show_databases.return_value = [_database(DB)]
    dd.get_databases.return_value = [_database(DB)]
    dd.get_schemas_for_database.return_value = [_schema(SCHEMA)]
    dd.get_tables_for_database.return_value = {}
    dd.get_views_for_database.return_value = {}

    list(gen.get_workunits_internal())

    # The filters were rendered into SQL rather than applied in Python.
    assert "DENIED" in dd.get_databases.call_args.args[1]
    assert "HIDDEN" in dd.get_schemas_for_database.call_args.args[1]
    assert gen.report.filtered == []


def test_structured_property_templates_are_created_before_scanning() -> None:
    gen = _make_gen(
        extract_tags_as_structured_properties=True,
        structured_properties_template_cache_invalidation_interval=0,
    )
    dd = _as_mock(gen.data_dictionary)
    gen.tag_extractor.data_dictionary = dd
    dd.get_all_tags.return_value = [_tag()]
    dd.show_databases.return_value = []
    dd.get_databases.return_value = []

    with patch("datahub.ingestion.source.snowflake.snowflake_schema_gen.time.sleep"):
        workunits = list(gen.get_workunits_internal())

    assert [wu.get_urn() for wu in workunits] == [
        f"urn:li:structuredProperty:snowflake.{DB.lower()}.{SCHEMA.lower()}.pii"
    ]
    assert gen.report.num_structured_property_templates_created == 1


# --- Database and schema level -----------------------------------------------


def test_database_and_schema_tag_failures_do_not_block_containers() -> None:
    gen = _make_gen(extract_tags="without_lineage")
    dd = _as_mock(gen.data_dictionary)
    dd.get_schemas_for_database.return_value = [_schema(SCHEMA)]
    dd.get_tables_for_database.return_value = {}
    dd.get_views_for_database.return_value = {}
    gen.tag_extractor = MagicMock()
    gen.tag_extractor.get_tags_on_object.side_effect = RuntimeError("tag refs")

    workunits = list(gen._process_database(_database(DB)))

    containers = {wu.get_urn() for wu in workunits}
    assert len(containers) == 2
    assert len(gen.report.warnings) == 2  # database tags, schema tags


def test_database_tags_are_emitted_as_tag_entities() -> None:
    gen = _make_gen(extract_tags="without_lineage")
    dd = _as_mock(gen.data_dictionary)
    dd.get_schemas_for_database.return_value = []
    gen.tag_extractor = MagicMock()
    gen.tag_extractor.get_tags_on_object.return_value = [_tag("OWNER")]

    workunits = list(gen._process_database(_database(DB)))

    tag_urns = list(_aspects(workunits, TagPropertiesClass))
    assert len(tag_urns) == 1
    assert tag_urns[0].endswith("owner:yes")


def test_non_permission_schema_failure_warns_and_leaves_database_empty() -> None:
    gen = _make_gen()
    db = _database(DB)
    _as_mock(gen.data_dictionary).get_schemas_for_database.side_effect = RuntimeError(
        "timeout"
    )

    gen.fetch_schemas_for_database(db, DB)

    assert db.schemas == []
    assert len(gen.report.warnings) == 2  # the error, then "no schemas found"


def test_empty_schema_is_reported_as_info() -> None:
    gen = _make_gen()
    dd = _as_mock(gen.data_dictionary)
    dd.get_tables_for_database.return_value = {}
    dd.get_views_for_database.return_value = {}

    list(gen._process_schema(_schema(), DB, {}))

    assert [i.title for i in gen.report.infos] == [
        "No tables/views/semantic views/streams found in schema"
    ]


# --- Tables and views ---------------------------------------------------------


def test_table_side_lookups_fail_independently_but_table_is_still_emitted() -> None:
    gen = _make_gen(extract_tags="without_lineage")
    dd = _as_mock(gen.data_dictionary)
    dd.get_columns_for_schema.side_effect = RuntimeError("columns")
    dd.get_pk_constraints_for_schema.side_effect = RuntimeError("pk")
    dd.get_fk_constraints_for_schema.side_effect = RuntimeError("fk")
    gen.tag_extractor = MagicMock()
    gen.tag_extractor.get_column_tags_for_table.side_effect = RuntimeError("ctags")
    gen.tag_extractor.get_tags_on_object.side_effect = RuntimeError("tags")

    workunits = list(gen._process_table(_table("T1"), _schema(), DB))

    assert _urn(gen, "T1") in _aspects(workunits, StatusClass)
    assert _aspects(workunits, SchemaMetadataClass)[_urn(gen, "T1")].fields == []
    assert len(gen.report.warnings) == 5


def test_view_side_lookups_fail_independently_but_view_is_still_emitted() -> None:
    gen = _make_gen(extract_tags="without_lineage")
    _as_mock(gen.data_dictionary).get_columns_for_schema.side_effect = RuntimeError(
        "columns"
    )
    gen.tag_extractor = MagicMock()
    gen.tag_extractor.get_tags_on_object.side_effect = RuntimeError("tags")

    workunits = list(gen._process_view(_view("V1"), _schema(), DB))

    assert _urn(gen, "V1") in _aspects(workunits, StatusClass)
    assert len(gen.report.warnings) == 2


def test_table_listing_errors_warn_but_permission_errors_propagate() -> None:
    gen = _make_gen()
    dd = _as_mock(gen.data_dictionary)
    dd.get_tables_for_database.return_value = None
    dd.get_tables_for_schema.side_effect = RuntimeError("timeout")

    assert gen.fetch_tables_for_schema(_schema(), DB, SCHEMA) == []
    assert len(gen.report.warnings) == 1

    dd.get_tables_for_schema.side_effect = SnowflakePermissionError("denied")
    with pytest.raises(SnowflakePermissionError):
        gen.fetch_tables_for_schema(_schema(), DB, SCHEMA)


def test_view_listing_falls_back_per_schema_and_filters_in_python() -> None:
    gen = _make_gen(view_pattern={"deny": [".*\\.hidden_.*"]})
    dd = _as_mock(gen.data_dictionary)
    dd.get_views_for_database.return_value = None
    dd.get_views_for_schema.return_value = [_view("V1"), _view("HIDDEN_V")]
    schema = _schema()

    views = gen.fetch_views_for_schema(schema, DB, SCHEMA)

    assert [v.name for v in views] == ["V1"]
    assert schema.views == ["V1"]
    assert gen.report.num_get_views_for_schema_queries == 1

    dd.get_views_for_schema.side_effect = RuntimeError("timeout")
    assert gen.fetch_views_for_schema(_schema(), DB, SCHEMA) == []
    dd.get_views_for_schema.side_effect = SnowflakePermissionError("denied")
    with pytest.raises(SnowflakePermissionError):
        gen.fetch_views_for_schema(_schema(), DB, SCHEMA)


def test_table_listing_falls_back_per_schema_when_database_wide_fails() -> None:
    gen = _make_gen()
    dd = _as_mock(gen.data_dictionary)
    dd.get_tables_for_database.return_value = None
    dd.get_tables_for_schema.return_value = [_table("T1")]

    assert [t.name for t in gen.get_tables_for_schema(SCHEMA, DB)] == ["T1"]
    assert gen.report.num_get_tables_for_schema_queries == 1


# --- Dynamic tables -----------------------------------------------------------


def _dynamic_table(
    name: str, definition: str, inputs: List[str]
) -> SnowflakeDynamicTable:
    return SnowflakeDynamicTable(
        name=name,
        comment=None,
        created=CREATED,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        definition=definition,
        upstream_tables=[
            SnowflakeDynamicTableInput(name=i, kind="TABLE") for i in inputs
        ],
    )


def test_dynamic_table_without_definition_uses_inputs_minus_itself() -> None:
    aggregator = MagicMock()
    gen = _make_gen(aggregator=aggregator, include_technical_schema=False)
    self_name = f"{DB}.{SCHEMA}.DT"
    table = _dynamic_table("DT", "", [f"{DB}.{SCHEMA}.SRC", self_name])

    list(gen._process_tables([table], _schema(), DB, SCHEMA))

    aggregator.add_view_definition.assert_not_called()
    aggregator.add_known_lineage_mapping.assert_called_once_with(
        upstream_urn=_urn(gen, "SRC"),
        downstream_urn=_urn(gen, "DT"),
        lineage_type=DatasetLineageTypeClass.VIEW,
    )
    assert gen.report.num_dynamic_tables_missing_definition == 1
    assert gen.dynamic_table_identifiers == {self_name.lower()}


def test_dynamic_table_lineage_registration_failure_is_a_warning() -> None:
    aggregator = MagicMock()
    aggregator.add_view_definition.side_effect = RuntimeError("parser")
    gen = _make_gen(aggregator=aggregator, include_technical_schema=False)
    table = _dynamic_table("DT", "select * from src", [f"{DB}.{SCHEMA}.SRC"])

    list(gen._process_tables([table], _schema(), DB, SCHEMA))

    assert len(gen.report.warnings) == 1


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
    gen = _make_gen(
        semantic_views={"enabled": True},
        semantic_view_pattern={"deny": [".*\\.internal_.*"]},
    )
    dd = _as_mock(gen.data_dictionary)
    dd.get_semantic_views_for_database.return_value = {
        SCHEMA: [_semantic_view("SV"), _semantic_view("INTERNAL_SV")]
    }
    schema = _schema()

    kept = gen.fetch_semantic_views_for_schema(schema, DB)

    assert [sv.name for sv in kept] == ["SV"]
    assert schema.semantic_views == ["SV"]
    assert f"{DB}.{SCHEMA}.INTERNAL_SV".lower() in gen.report.filtered


def test_semantic_view_listing_falls_back_to_information_schema() -> None:
    gen = _make_gen(semantic_views={"enabled": True})
    dd = _as_mock(gen.data_dictionary)
    dd.get_semantic_views_for_database.return_value = None
    dd.get_semantic_views_for_schema_using_information_schema.return_value = [
        _semantic_view("SV")
    ]

    assert [sv.name for sv in gen.get_semantic_views_for_schema(SCHEMA, DB)] == ["SV"]


def test_semantic_view_listing_errors_are_soft_except_permission_errors() -> None:
    gen = _make_gen(semantic_views={"enabled": True})
    dd = _as_mock(gen.data_dictionary)
    dd.get_semantic_views_for_database.side_effect = RuntimeError("unsupported")

    assert gen.fetch_semantic_views_for_schema(_schema(), DB) == []
    assert len(gen.report.infos) == 1

    dd.get_semantic_views_for_database.side_effect = SnowflakePermissionError("no")
    with pytest.raises(SnowflakePermissionError):
        gen.fetch_semantic_views_for_schema(_schema(), DB)


def test_semantic_view_without_base_tables_warns_and_skips_lineage() -> None:
    aggregator = MagicMock()
    gen = _make_gen(aggregator=aggregator, semantic_views={"enabled": True})
    _as_mock(gen.data_dictionary).get_columns_for_schema.return_value = {}
    sv = _semantic_view("SV", columns=[_column("DIM")])

    workunits = list(gen._process_semantic_views([sv], _schema(), DB, SCHEMA))

    assert _urn(gen, "SV") in _aspects(workunits, SchemaMetadataClass)
    assert [w.title for w in gen.report.warnings] == ["Semantic view lineage skipped"]
    # The YAML definition is still registered for reference.
    aggregator.add_view_definition.assert_called_once()
    aggregator.add_known_query_lineage.assert_not_called()


def test_semantic_view_tag_lookup_failures_are_tolerated() -> None:
    gen = _make_gen(semantic_views={"enabled": True}, extract_tags="without_lineage")
    _as_mock(gen.data_dictionary).get_columns_for_schema.side_effect = RuntimeError(
        "no columns"
    )
    gen.tag_extractor = MagicMock()
    gen.tag_extractor.get_column_tags_for_table.side_effect = RuntimeError("ctags")
    gen.tag_extractor.get_tags_on_object.side_effect = RuntimeError("tags")
    sv = _semantic_view("SV")

    workunits = list(gen._process_semantic_view(sv, _schema(), DB))

    assert sv.columns == []
    assert sv.column_tags == {}
    assert sv.tags is None
    assert _urn(gen, "SV") in _aspects(workunits, StatusClass)
    assert gen.report.warnings == []


def test_semantic_model_mode_emits_tag_entities_and_tolerates_lineage_errors() -> None:
    gen = _make_gen(
        semantic_views={
            "enabled": True,
            "emit_semantic_model_entities": True,
            "column_lineage": True,
        },
        extract_tags="without_lineage",
    )
    gen.tag_extractor = MagicMock()
    gen.tag_extractor.get_column_tags_for_table.return_value = {"DIM": [_tag("COL")]}
    gen.tag_extractor.get_tags_on_object.return_value = [_tag("VIEW")]
    gen.semantic_model_mapper = MagicMock()
    gen.semantic_model_mapper.gen_workunits.return_value = iter([])
    sv = _semantic_view(
        "SV",
        columns=[_column("DIM")],
        base_tables=[SnowflakeTableIdentifier(DB, SCHEMA, "BASE")],
        column_table_mappings={"DIM": ["BASE"]},
        logical_to_physical_table={"BASE": (DB, SCHEMA, "BASE")},
    )

    with patch.object(
        gen, "_generate_column_lineage_for_semantic_view", side_effect=RuntimeError
    ):
        workunits = list(gen._process_semantic_views([sv], _schema(), DB, SCHEMA))

    tag_urns = sorted(_aspects(workunits, TagPropertiesClass))
    assert len(tag_urns) == 2
    assert [w.title for w in gen.report.warnings] == [
        "Failed to generate column lineage for semantic view"
    ]
    gen.semantic_model_mapper.gen_workunits.assert_called_once()
    assert (
        gen.semantic_model_mapper.gen_workunits.call_args.kwargs[
            "fine_grained_lineages"
        ]
        == []
    )
    assert gen.report.num_semantic_model_lineage_edges_scanned == 1


# --- Streams, procedures, Streamlit apps -------------------------------------


def test_stream_listing_falls_back_per_schema_and_permission_errors_propagate() -> None:
    gen = _make_gen(include_streams=True, stream_pattern={"deny": [".*\\.old_.*"]})
    dd = _as_mock(gen.data_dictionary)
    dd.get_streams_for_database.return_value = None
    dd.get_streams_for_schema_using_show.return_value = [
        _stream("S1", "SRC"),
        _stream("OLD_S", "SRC"),
    ]
    schema = _schema()

    streams = gen.fetch_streams_for_schema(schema, DB)

    assert [s.name for s in streams] == ["S1"]
    assert schema.streams == ["S1"]

    dd.get_streams_for_schema_using_show.side_effect = RuntimeError("timeout")
    assert gen.fetch_streams_for_schema(_schema(), DB) == []
    assert len(gen.report.warnings) == 1
    dd.get_streams_for_schema_using_show.side_effect = SnowflakePermissionError("no")
    with pytest.raises(SnowflakePermissionError):
        gen.fetch_streams_for_schema(_schema(), DB)


def test_stream_processing_failure_is_a_warning() -> None:
    gen = _make_gen(include_streams=True)
    _as_mock(gen.data_dictionary).get_columns_for_schema.side_effect = RuntimeError(
        "columns"
    )

    assert list(gen._process_streams([_stream("S1", "SRC")], _schema(), DB)) == []
    assert len(gen.report.warnings) == 1


def test_procedure_processing_failure_is_a_warning() -> None:
    gen = _make_gen(include_procedures=True)

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_schema_gen.generate_procedure_workunits",
        side_effect=RuntimeError("bad definition"),
    ):
        workunits = list(gen._process_procedures([_procedure("P")], _schema(), DB))

    # The procedure flow container is still emitted even though the procedure failed.
    assert workunits
    assert all(wu.get_urn().startswith("urn:li:dataFlow:") for wu in workunits)
    assert [w.title for w in gen.report.warnings] == [
        "Failed to ingest stored procedure"
    ]


def test_streamlit_apps_are_filtered_and_emitted_as_dashboards() -> None:
    gen = _make_gen(
        include_streamlits=True, streamlit_pattern={"deny": [".*\\.INTERNAL_.*"]}
    )
    dd = _as_mock(gen.data_dictionary)
    dd.get_streamlit_apps_for_database.return_value = {
        SCHEMA: [_app("APP"), _app("INTERNAL_APP")]
    }

    apps = gen.fetch_streamlit_apps(_schema(), DB)
    assert [a.name for a in apps] == ["APP"]
    assert f"{DB}.{SCHEMA}.INTERNAL_APP" in gen.report.filtered

    workunits = list(gen._process_streamlit_apps(apps))
    dashboard_urns = {
        wu.get_urn() for wu in workunits if wu.get_urn().startswith("urn:li:dashboard:")
    }
    assert len(dashboard_urns) == 1

    dd.get_streamlit_apps_for_database.side_effect = SnowflakePermissionError("no")
    assert gen.fetch_streamlit_apps(_schema(), DB) == []
    dd.get_streamlit_apps_for_database.side_effect = RuntimeError("boom")
    assert gen.fetch_streamlit_apps(_schema(), DB) == []
    assert [w.title for w in gen.report.warnings] == [
        "Permission denied for Streamlit apps",
        "Failed to get Streamlit apps",
    ]


def test_schema_scan_dispatches_to_every_enabled_object_kind() -> None:
    gen = _make_gen(
        include_streams=True,
        include_procedures=True,
        include_streamlits=True,
        semantic_views={"enabled": True},
        extract_tags="without_lineage",
    )
    dd = _as_mock(gen.data_dictionary)
    dd.get_tables_for_database.return_value = {SCHEMA: [_table("T1")]}
    dd.get_views_for_database.return_value = {}
    dd.get_semantic_views_for_database.return_value = {}
    dd.get_streams_for_database.return_value = {}
    dd.get_procedures_for_database.return_value = {}
    dd.get_streamlit_apps_for_database.return_value = {}
    gen.tag_extractor = MagicMock()
    gen.tag_extractor.get_tags_on_object.return_value = [_tag("SCHEMA_TAG")]
    gen.tag_extractor.get_column_tags_for_table.return_value = {}
    schema = _schema()

    workunits = list(gen._process_schema(schema, DB, {}))

    assert schema.tables == ["T1"]
    assert gen.report.num_get_streams_for_schema_queries == 1
    # Schema-level tags are emitted as tag entities after the schema's datasets.
    assert any(
        u.endswith("schema_tag:yes") for u in _aspects(workunits, TagPropertiesClass)
    )


def test_external_table_ddl_lineage_failure_is_a_warning() -> None:
    gen = _make_gen()
    _as_mock(gen.connection).query.side_effect = RuntimeError("show failed")

    assert list(gen._external_tables_ddl_lineage([])) == []
    assert len(gen.report.warnings) == 1
