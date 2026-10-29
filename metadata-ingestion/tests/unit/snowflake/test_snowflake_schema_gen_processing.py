"""What SnowflakeSchemaGenerator emits for the objects it discovers.

The data dictionary is replaced by canned objects (see ``make_gen`` in
conftest.py) so these tests exercise filtering, aspect construction and the
lineage registered with the aggregator rather than row parsing.
"""

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.snowflake.constants import GENERIC_PERMISSION_ERROR_KEY
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDynamicTable,
    SnowflakeFK,
    SnowflakePK,
    SnowflakeTable,
    SnowflakeTag,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    GlobalTagsClass,
    SchemaMetadataClass,
    StatusClass,
    StructuredPropertiesClass,
    SubTypesClass,
    TagPropertiesClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sql_parsing_aggregator import KnownLineageMapping
from tests.unit.snowflake.conftest import (
    CREATED,
    DB,
    SCHEMA,
    as_mock,
    aspects,
    column,
    database,
    dataset_urn,
    log_keys,
    make_aggregator,
    make_gen,
    procedure,
    schema,
    stream,
    table,
    view,
)


def test_databases_schemas_and_datasets_are_filtered_and_emitted() -> None:
    aggregator = make_aggregator()
    gen = make_gen(
        aggregator=aggregator,
        database_pattern={"deny": ["DENIED_DB"]},
        schema_pattern={"deny": ["HIDDEN"]},
    )
    dd = as_mock(gen.data_dictionary)
    dd.show_databases.return_value = [database(DB), database("DENIED_DB")]
    dd.get_databases.return_value = [database(DB), database("DENIED_DB")]
    dd.get_schemas_for_database.return_value = [schema(SCHEMA), schema("HIDDEN")]
    dd.get_tables_for_database.return_value = {SCHEMA: [table("T1")]}
    dd.get_views_for_database.return_value = {SCHEMA: [view("V1")]}
    dd.get_columns_for_schema.return_value = {
        "T1": [column("ID")],
        "V1": [column("ID")],
    }

    workunits = list(gen.get_workunits_internal())

    assert [db.name for db in gen.databases] == [DB]
    assert [s.name for s in gen.databases[0].schemas] == [SCHEMA]
    assert gen.databases[0].schemas[0].tables == ["T1"]
    assert gen.databases[0].schemas[0].views == ["V1"]
    assert "DENIED_DB.*" in gen.report.filtered
    assert f"{DB}.HIDDEN.*" in gen.report.filtered

    subtypes = aspects(workunits, SubTypesClass)
    assert subtypes[dataset_urn(gen, "T1")].typeNames == [DatasetSubTypes.TABLE]
    assert subtypes[dataset_urn(gen, "V1")].typeNames == [DatasetSubTypes.VIEW]
    assert set(aspects(workunits, StatusClass)) >= {
        dataset_urn(gen, "T1"),
        dataset_urn(gen, "V1"),
    }
    # Containers for the database and the schema, but nothing for the dropped ones.
    container_urns = {
        wu.get_urn() for wu in workunits if wu.get_urn().startswith("urn:li:container:")
    }
    assert len(container_urns) == 2
    aggregator.add_view_definition.assert_called_once()
    assert aggregator.add_view_definition.call_args.kwargs["view_urn"] == dataset_urn(
        gen, "V1"
    )
    assert gen.report.failures == []


def test_permission_error_listing_schemas_is_a_reported_failure() -> None:
    gen = make_gen()
    dd = as_mock(gen.data_dictionary)
    dd.show_databases.return_value = [database(DB)]
    dd.get_databases.return_value = [database(DB)]
    dd.get_schemas_for_database.side_effect = SnowflakePermissionError("not authorized")

    workunits = list(gen.get_workunits_internal())

    assert aspects(workunits, SchemaMetadataClass) == {}
    assert GENERIC_PERMISSION_ERROR_KEY in log_keys(gen.report.failures)


def test_primary_and_foreign_keys_land_in_schema_metadata() -> None:
    gen = make_gen()
    dd = as_mock(gen.data_dictionary)
    dd.get_columns_for_schema.return_value = {
        "T1": [column("ID", 1), column("PARENT_ID", 2)]
    }
    dd.get_pk_constraints_for_schema.return_value = {
        "T1": SnowflakePK(name="PK_T1", column_names=["ID"])
    }
    dd.get_fk_constraints_for_schema.return_value = {
        "T1": [
            SnowflakeFK(
                name="FK_T1_T0",
                column_names=["PARENT_ID"],
                referred_database=DB,
                referred_schema=SCHEMA,
                referred_table="T0",
                referred_column_names=["ID"],
            )
        ]
    }

    workunits = list(gen._process_table(table("T1"), schema(), DB))

    schema_metadata = aspects(workunits, SchemaMetadataClass)[dataset_urn(gen, "T1")]
    assert [(f.fieldPath, f.isPartOfKey) for f in schema_metadata.fields] == [
        ("id", True),
        ("parent_id", False),
    ]
    assert schema_metadata.foreignKeys is not None
    assert len(schema_metadata.foreignKeys) == 1
    fk = schema_metadata.foreignKeys[0]
    assert fk.foreignDataset == dataset_urn(gen, "T0")
    assert fk.foreignFields == [f"urn:li:schemaField:({dataset_urn(gen, 'T0')},id)"]
    assert fk.sourceFields == [
        f"urn:li:schemaField:({dataset_urn(gen, 'T1')},parent_id)"
    ]


def test_dataset_properties_describe_table_kind() -> None:
    gen = make_gen()
    dynamic = SnowflakeDynamicTable(
        name="DT",
        comment="refreshed hourly",
        created=CREATED,
        last_altered=CREATED,
        size_in_bytes=None,
        rows_count=None,
        clustering_key="LINEAR(ID)",
        is_dynamic=True,
        is_iceberg=True,
        retention_time=3,
        target_lag="1 hour",
    )

    props = gen.get_dataset_properties(dynamic, SCHEMA, DB)

    assert props.qualifiedName == f"{DB}.{SCHEMA}.DT"
    assert props.description == "refreshed hourly"
    assert props.created is not None
    assert props.created.time == int(CREATED.timestamp() * 1000)
    assert props.customProperties == {
        "CLUSTERING_KEY": "LINEAR(ID)",
        "IS_DYNAMIC": "true",
        "IS_ICEBERG": "true",
        "RETENTION_TIME": "3",
        "TARGET_LAG": "1 hour",
    }

    secure = gen.get_dataset_properties(view("V1", is_secure=True), SCHEMA, DB)
    assert secure.customProperties == {"IS_SECURE": "true"}
    assert secure.lastModified is None


_PII = SnowflakeTag(database=DB, schema=SCHEMA, name="PII", value="HIGH")
_PII_TAG_URN = "urn:li:tag:test_db.public.pii:high"


def _tagged_table() -> SnowflakeTable:
    return table("T1", tags=[_PII], column_tags={"ID": [_PII]}, columns=[column("ID")])


def test_tags_are_emitted_once_as_tag_entities_or_as_structured_properties() -> None:
    gen = make_gen()
    workunits = list(gen.gen_dataset_workunits(_tagged_table(), SCHEMA, DB))

    tag_properties = aspects(workunits, TagPropertiesClass)
    assert list(tag_properties) == [_PII_TAG_URN]
    assert tag_properties[_PII_TAG_URN].name == "PII: HIGH"
    dataset_tags = aspects(workunits, GlobalTagsClass)[dataset_urn(gen, "T1")]
    assert [t.tag for t in dataset_tags.tags] == [_PII_TAG_URN]
    schema_metadata = aspects(workunits, SchemaMetadataClass)[dataset_urn(gen, "T1")]
    assert schema_metadata.fields[0].globalTags is not None
    assert [t.tag for t in schema_metadata.fields[0].globalTags.tags] == [_PII_TAG_URN]

    sp_gen = make_gen(extract_tags_as_structured_properties=True)
    workunits = list(sp_gen.gen_dataset_workunits(_tagged_table(), SCHEMA, DB))

    assert aspects(workunits, TagPropertiesClass) == {}
    assert aspects(workunits, GlobalTagsClass) == {}
    structured = aspects(workunits, StructuredPropertiesClass)
    dataset_props = structured[dataset_urn(sp_gen, "T1")]
    assert [(p.propertyUrn, p.values) for p in dataset_props.properties] == [
        ("urn:li:structuredProperty:snowflake.test_db.public.pii", ["HIGH"])
    ]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "gen_column_tags_as_structured_properties builds the schemaField URN from "
        "the raw column name, while gen_schema_metadata emits the field under "
        "snowflake_column_identifier(name) (lowercased by default), so the "
        "column-level structured property points at a field that is never emitted."
    ),
)
def test_column_structured_properties_target_the_emitted_schema_field() -> None:
    """A column tag as a structured property must land on the field the schema emits."""
    gen = make_gen(extract_tags_as_structured_properties=True)

    workunits = list(gen.gen_dataset_workunits(_tagged_table(), SCHEMA, DB))

    schema_metadata = aspects(workunits, SchemaMetadataClass)[dataset_urn(gen, "T1")]
    assert [f.fieldPath for f in schema_metadata.fields] == ["id"]
    field_urns = [
        urn
        for urn in aspects(workunits, StructuredPropertiesClass)
        if urn.startswith("urn:li:schemaField:")
    ]
    assert field_urns == [make_schema_field_urn(dataset_urn(gen, "T1"), "id")]


def test_stream_gets_metadata_columns_and_copy_lineage_from_source() -> None:
    aggregator = make_aggregator()
    gen = make_gen(aggregator=aggregator, include_streams=True)
    as_mock(gen.data_dictionary).get_columns_for_schema.return_value = {
        "SRC": [column("ID")]
    }

    workunits = list(gen._process_stream(stream("S1", "SRC"), schema(), DB))

    schema_metadata = aspects(workunits, SchemaMetadataClass)[dataset_urn(gen, "S1")]
    assert [f.fieldPath for f in schema_metadata.fields] == [
        "id",
        "metadata$action",
        "metadata$isupdate",
        "metadata$row_id",
    ]
    props = aspects(workunits, DatasetPropertiesClass)[dataset_urn(gen, "S1")]
    assert props.customProperties["TABLE_NAME"] == f"{DB}.{SCHEMA}.SRC"
    assert props.customProperties["STALE_AFTER"] == CREATED.isoformat()
    assert "INVALID_REASON" not in props.customProperties
    aggregator.add_known_lineage_mapping.assert_called_once()
    call = aggregator.add_known_lineage_mapping.call_args.kwargs
    assert call["upstream_urn"] == dataset_urn(gen, "SRC")
    assert call["downstream_urn"] == dataset_urn(gen, "S1")
    assert gen.report.num_streams_with_known_upstreams == 1


def test_external_table_ddl_lineage_only_for_discovered_s3_tables() -> None:
    """Only discovered tables on S3 get a DDL edge, and each edge is counted once."""
    gen = make_gen()

    def row(name: str, location: str) -> dict:
        return {
            "name": name,
            "schema_name": SCHEMA,
            "database_name": DB,
            "location": location,
        }

    as_mock(gen.connection).query.side_effect = lambda _sql: [
        row("EXT1", "s3://bucket/ext1/"),
        row("EXT2", "azure://container/ext2/"),
        row("EXT3", "s3://bucket/ext3/"),
    ]
    discovered = [f"{DB}.{SCHEMA}.EXT1".lower(), f"{DB}.{SCHEMA}.EXT2".lower()]

    mappings = list(gen._external_tables_ddl_lineage(discovered))

    assert len(mappings) == 1
    assert isinstance(mappings[0], KnownLineageMapping)
    assert mappings[0].downstream_urn == dataset_urn(gen, "EXT1")
    assert DatasetUrn.from_string(mappings[0].upstream_urn).platform.endswith("s3")
    # The counter mirrors the copy-history one in the lineage extractor: one per
    # edge yielded, not one per external table scanned plus one per S3 edge.
    assert gen.report.num_external_table_edges_scanned == 1


def test_secure_view_definition_lookup_degrades_gracefully() -> None:
    gen = make_gen()
    as_mock(gen.data_dictionary).get_secure_view_definitions.return_value = {
        DB: {SCHEMA: {"V1": "select 1"}}
    }

    assert gen.fetch_secure_view_definition("V1", SCHEMA, DB) == "select 1"
    assert gen.fetch_secure_view_definition("V2", SCHEMA, DB) is None
    assert "Secure view definition not found" in log_keys(gen.report.infos)

    as_mock(
        gen.data_dictionary
    ).get_secure_view_definitions.side_effect = SnowflakePermissionError(
        "not authorized"
    )
    assert gen.fetch_secure_view_definition("V1", SCHEMA, DB) is None
    assert "Failed to get secure views definitions" in log_keys(gen.report.warnings)


def test_procedures_are_filtered_by_pattern_and_permission_errors_warn() -> None:
    gen = make_gen(
        include_procedures=True, procedure_pattern={"deny": [".*\\.internal_.*"]}
    )
    as_mock(gen.data_dictionary).get_procedures_for_database.return_value = {
        SCHEMA: [procedure("PUBLIC_PROC"), procedure("INTERNAL_PROC")]
    }

    kept = gen.fetch_procedures_for_schema(schema(), DB)

    assert [p.name for p in kept] == ["PUBLIC_PROC"]
    assert f"{DB}.{SCHEMA}.INTERNAL_PROC".lower() in gen.report.filtered

    as_mock(
        gen.data_dictionary
    ).get_procedures_for_database.side_effect = SnowflakePermissionError(
        "not authorized"
    )
    assert gen.fetch_procedures_for_schema(schema(), DB) == []
    assert "Failed to get procedures for schema" in log_keys(gen.report.warnings)
