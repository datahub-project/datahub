from datetime import datetime, timezone
from typing import Dict, List, Optional, Type, TypeVar
from unittest.mock import MagicMock

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeDatabase,
    SnowflakeDynamicTable,
    SnowflakeFK,
    SnowflakePK,
    SnowflakeSchema,
    SnowflakeStream,
    SnowflakeTable,
    SnowflakeTag,
    SnowflakeView,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
)
from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    GlobalTagsClass,
    SchemaMetadataClass,
    StatusClass,
    StructuredPropertiesClass,
    SubTypesClass,
    TagPropertiesClass,
    _Aspect,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sql_parsing_aggregator import KnownLineageMapping

DB = "TEST_DB"
SCHEMA = "PUBLIC"
CREATED = datetime(2023, 1, 15, 10, 30, tzinfo=timezone.utc)

A = TypeVar("A", bound=_Aspect)


def _as_mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


def _make_gen(
    aggregator: Optional[MagicMock] = None, **config_overrides: object
) -> SnowflakeSchemaGenerator:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "user",
            "password": "pass",
            "include_streams": False,
            "include_procedures": False,
            "include_streamlits": False,
            **config_overrides,
        }
    )
    report = SnowflakeV2Report()
    connection = MagicMock()
    connection.query.return_value = []
    gen = SnowflakeSchemaGenerator(
        config=config,
        report=report,
        connection=connection,
        filters=SnowflakeFilter(filter_config=config, structured_reporter=report),
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        domain_registry=None,
        profiler=None,
        aggregator=aggregator,
        snowsight_url_builder=None,
    )
    # The data dictionary is the query layer; its row parsing is covered by
    # test_snowflake_schema.py, so here it is replaced by canned objects.
    data_dictionary = MagicMock()
    data_dictionary.get_pk_constraints_for_schema.return_value = {}
    data_dictionary.get_fk_constraints_for_schema.return_value = {}
    data_dictionary.get_columns_for_schema.return_value = {}
    gen.data_dictionary = data_dictionary
    return gen


def _column(name: str, position: int = 1) -> SnowflakeColumn:
    return SnowflakeColumn(
        name=name,
        ordinal_position=position,
        is_nullable=True,
        data_type="VARCHAR",
        comment=None,
        character_maximum_length=None,
        numeric_precision=None,
        numeric_scale=None,
    )


def _table(name: str, **overrides: object) -> SnowflakeTable:
    return SnowflakeTable(
        name=name,
        comment=None,
        created=CREATED,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        **overrides,  # type: ignore[arg-type]
    )


def _view(name: str, **overrides: object) -> SnowflakeView:
    return SnowflakeView(
        name=name,
        comment=None,
        created=CREATED,
        last_altered=None,
        view_definition="select 1",
        **overrides,  # type: ignore[arg-type]
    )


def _schema(name: str = SCHEMA) -> SnowflakeSchema:
    return SnowflakeSchema(name=name, created=CREATED, last_altered=None, comment=None)


def _database(name: str = DB) -> SnowflakeDatabase:
    return SnowflakeDatabase(name=name, created=CREATED, comment=None)


def _aspects(workunits: List[MetadataWorkUnit], aspect_type: Type[A]) -> Dict[str, A]:
    found: Dict[str, A] = {}
    for wu in workunits:
        aspect = wu.get_aspect_of_type(aspect_type)
        if aspect is not None:
            found[wu.get_urn()] = aspect
    return found


def _urn(gen: SnowflakeSchemaGenerator, name: str) -> str:
    return gen.identifiers.gen_dataset_urn(f"{DB}.{SCHEMA}.{name}".lower())


def test_databases_schemas_and_datasets_are_filtered_and_emitted() -> None:
    aggregator = MagicMock()
    gen = _make_gen(
        aggregator=aggregator,
        database_pattern={"deny": ["DENIED_DB"]},
        schema_pattern={"deny": ["HIDDEN"]},
    )
    dd = _as_mock(gen.data_dictionary)
    dd.show_databases.return_value = [_database(DB), _database("DENIED_DB")]
    dd.get_databases.return_value = [_database(DB), _database("DENIED_DB")]
    dd.get_schemas_for_database.return_value = [_schema(SCHEMA), _schema("HIDDEN")]
    dd.get_tables_for_database.return_value = {SCHEMA: [_table("T1")]}
    dd.get_views_for_database.return_value = {SCHEMA: [_view("V1")]}
    dd.get_columns_for_schema.return_value = {
        "T1": [_column("ID")],
        "V1": [_column("ID")],
    }

    workunits = list(gen.get_workunits_internal())

    assert [db.name for db in gen.databases] == [DB]
    assert [s.name for s in gen.databases[0].schemas] == [SCHEMA]
    assert gen.databases[0].schemas[0].tables == ["T1"]
    assert gen.databases[0].schemas[0].views == ["V1"]
    assert "DENIED_DB.*" in gen.report.filtered
    assert f"{DB}.HIDDEN.*" in gen.report.filtered

    subtypes = _aspects(workunits, SubTypesClass)
    assert subtypes[_urn(gen, "T1")].typeNames == [DatasetSubTypes.TABLE]
    assert subtypes[_urn(gen, "V1")].typeNames == [DatasetSubTypes.VIEW]
    assert set(_aspects(workunits, StatusClass)) >= {_urn(gen, "T1"), _urn(gen, "V1")}
    # Containers for the database and the schema, but nothing for the dropped ones.
    container_urns = {
        wu.get_urn() for wu in workunits if wu.get_urn().startswith("urn:li:container:")
    }
    assert len(container_urns) == 2
    aggregator.add_view_definition.assert_called_once()
    assert aggregator.add_view_definition.call_args.kwargs["view_urn"] == _urn(
        gen, "V1"
    )
    assert gen.report.failures == []


def test_permission_error_listing_schemas_is_a_reported_failure() -> None:
    gen = _make_gen()
    dd = _as_mock(gen.data_dictionary)
    dd.show_databases.return_value = [_database(DB)]
    dd.get_databases.return_value = [_database(DB)]
    dd.get_schemas_for_database.side_effect = SnowflakePermissionError("not authorized")

    workunits = list(gen.get_workunits_internal())

    assert _aspects(workunits, SchemaMetadataClass) == {}
    assert len(gen.report.failures) == 1


def test_primary_and_foreign_keys_land_in_schema_metadata() -> None:
    gen = _make_gen()
    dd = _as_mock(gen.data_dictionary)
    dd.get_columns_for_schema.return_value = {
        "T1": [_column("ID", 1), _column("PARENT_ID", 2)]
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

    workunits = list(gen._process_table(_table("T1"), _schema(), DB))

    schema = _aspects(workunits, SchemaMetadataClass)[_urn(gen, "T1")]
    assert [(f.fieldPath, f.isPartOfKey) for f in schema.fields] == [
        ("id", True),
        ("parent_id", False),
    ]
    assert schema.foreignKeys is not None and len(schema.foreignKeys) == 1
    fk = schema.foreignKeys[0]
    assert fk.foreignDataset == _urn(gen, "T0")
    assert fk.foreignFields == [f"urn:li:schemaField:({_urn(gen, 'T0')},id)"]
    assert fk.sourceFields == [f"urn:li:schemaField:({_urn(gen, 'T1')},parent_id)"]


def test_dataset_properties_describe_table_kind() -> None:
    gen = _make_gen()
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

    secure = gen.get_dataset_properties(_view("V1", is_secure=True), SCHEMA, DB)
    assert secure.customProperties == {"IS_SECURE": "true"}
    assert secure.lastModified is None


def test_tags_are_emitted_once_as_tag_entities_or_as_structured_properties() -> None:
    tag = SnowflakeTag(database=DB, schema=SCHEMA, name="PII", value="HIGH")
    tag_urn = "urn:li:tag:test_db.public.pii:high"

    def tagged_table() -> SnowflakeTable:
        return _table(
            "T1", tags=[tag], column_tags={"ID": [tag]}, columns=[_column("ID")]
        )

    gen = _make_gen()
    workunits = list(gen.gen_dataset_workunits(tagged_table(), SCHEMA, DB))

    tag_properties = _aspects(workunits, TagPropertiesClass)
    assert list(tag_properties) == [tag_urn]
    assert tag_properties[tag_urn].name == "PII: HIGH"
    dataset_tags = _aspects(workunits, GlobalTagsClass)[_urn(gen, "T1")]
    assert [t.tag for t in dataset_tags.tags] == [tag_urn]
    schema = _aspects(workunits, SchemaMetadataClass)[_urn(gen, "T1")]
    assert schema.fields[0].globalTags is not None
    assert [t.tag for t in schema.fields[0].globalTags.tags] == [tag_urn]

    sp_gen = _make_gen(extract_tags_as_structured_properties=True)
    workunits = list(sp_gen.gen_dataset_workunits(tagged_table(), SCHEMA, DB))

    assert _aspects(workunits, TagPropertiesClass) == {}
    assert _aspects(workunits, GlobalTagsClass) == {}
    structured = _aspects(workunits, StructuredPropertiesClass)
    dataset_props = structured[_urn(sp_gen, "T1")]
    assert [(p.propertyUrn, p.values) for p in dataset_props.properties] == [
        ("urn:li:structuredProperty:snowflake.test_db.public.pii", ["HIGH"])
    ]
    field_urns = [urn for urn in structured if urn.startswith("urn:li:schemaField:")]
    assert len(field_urns) == 1


def test_stream_gets_metadata_columns_and_copy_lineage_from_source() -> None:
    aggregator = MagicMock()
    gen = _make_gen(aggregator=aggregator, include_streams=True)
    _as_mock(gen.data_dictionary).get_columns_for_schema.return_value = {
        "SRC": [_column("ID")]
    }
    stream = SnowflakeStream(
        name="S1",
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
        table_name=f"{DB}.{SCHEMA}.SRC",
        comment=None,
        stale_after=CREATED,
    )

    workunits = list(gen._process_stream(stream, _schema(), DB))

    schema = _aspects(workunits, SchemaMetadataClass)[_urn(gen, "S1")]
    assert [f.fieldPath for f in schema.fields] == [
        "id",
        "metadata$action",
        "metadata$isupdate",
        "metadata$row_id",
    ]
    props = _aspects(workunits, DatasetPropertiesClass)[_urn(gen, "S1")]
    assert props.customProperties["TABLE_NAME"] == f"{DB}.{SCHEMA}.SRC"
    assert props.customProperties["STALE_AFTER"] == CREATED.isoformat()
    assert "INVALID_REASON" not in props.customProperties
    aggregator.add_known_lineage_mapping.assert_called_once()
    call = aggregator.add_known_lineage_mapping.call_args.kwargs
    assert call["upstream_urn"] == _urn(gen, "SRC")
    assert call["downstream_urn"] == _urn(gen, "S1")
    assert gen.report.num_streams_with_known_upstreams == 1


def test_external_table_ddl_lineage_only_for_discovered_s3_tables() -> None:
    gen = _make_gen()

    def row(name: str, location: str) -> dict:
        return {
            "name": name,
            "schema_name": SCHEMA,
            "database_name": DB,
            "location": location,
        }

    _as_mock(gen.connection).query.return_value = [
        row("EXT1", "s3://bucket/ext1/"),
        row("EXT2", "azure://container/ext2/"),
        row("EXT3", "s3://bucket/ext3/"),
    ]
    discovered = [f"{DB}.{SCHEMA}.EXT1".lower(), f"{DB}.{SCHEMA}.EXT2".lower()]

    mappings = list(gen._external_tables_ddl_lineage(discovered))

    assert len(mappings) == 1
    assert isinstance(mappings[0], KnownLineageMapping)
    assert mappings[0].downstream_urn == _urn(gen, "EXT1")
    assert DatasetUrn.from_string(mappings[0].upstream_urn).platform.endswith("s3")


def test_secure_view_definition_lookup_degrades_gracefully() -> None:
    gen = _make_gen()
    _as_mock(gen.data_dictionary).get_secure_view_definitions.return_value = {
        DB: {SCHEMA: {"V1": "select 1"}}
    }

    assert gen.fetch_secure_view_definition("V1", SCHEMA, DB) == "select 1"
    assert gen.fetch_secure_view_definition("V2", SCHEMA, DB) is None
    assert len(gen.report.infos) == 1

    _as_mock(
        gen.data_dictionary
    ).get_secure_view_definitions.side_effect = SnowflakePermissionError(
        "not authorized"
    )
    assert gen.fetch_secure_view_definition("V1", SCHEMA, DB) is None
    assert len(gen.report.warnings) == 1


def test_procedures_are_filtered_by_pattern_and_permission_errors_warn() -> None:
    def procedure(name: str) -> BaseProcedure:
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

    gen = _make_gen(
        include_procedures=True, procedure_pattern={"deny": [".*\\.internal_.*"]}
    )
    _as_mock(gen.data_dictionary).get_procedures_for_database.return_value = {
        SCHEMA: [procedure("PUBLIC_PROC"), procedure("INTERNAL_PROC")]
    }

    kept = gen.fetch_procedures_for_schema(_schema(), DB)

    assert [p.name for p in kept] == ["PUBLIC_PROC"]
    assert f"{DB}.{SCHEMA}.INTERNAL_PROC".lower() in gen.report.filtered

    _as_mock(
        gen.data_dictionary
    ).get_procedures_for_database.side_effect = SnowflakePermissionError(
        "not authorized"
    )
    assert gen.fetch_procedures_for_schema(_schema(), DB) == []
    assert len(gen.report.warnings) == 1
