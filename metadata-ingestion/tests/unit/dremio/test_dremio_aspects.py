from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dremio.dremio_aspects import (
    DremioAspects,
    SchemaFieldTypeMapper,
)
from datahub.ingestion.source.dremio.dremio_models import DremioDatasetType
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


def _make_aspects() -> DremioAspects:
    return DremioAspects(
        platform="dremio",
        ui_url="https://dremio.example.com",
        env="PROD",
        ingest_owner=False,
    )


def _make_dataset(**kwargs):
    d = Mock()
    d.path = kwargs.get("path", ["src"])
    d.resource_name = kwargs.get("resource_name", "tbl")
    d.dataset_type = kwargs.get("dataset_type", DremioDatasetType.TABLE)
    d.description = kwargs.get("description")
    d.format_type = kwargs.get("format_type")
    d.created = kwargs.get("created", "")
    d.owner = kwargs.get("owner")
    d.owner_type = kwargs.get("owner_type")
    return d


class TestSchemaFieldTypeMapper:
    @pytest.mark.parametrize(
        "data_type, expected_class",
        [
            ("boolean", BooleanTypeClass),
            ("binary varying", BytesTypeClass),
            ("decimal", NumberTypeClass),
            ("integer", NumberTypeClass),
            ("bigint", NumberTypeClass),
            ("float", NumberTypeClass),
            ("double", NumberTypeClass),
            ("timestamp", DateTypeClass),
            ("date", DateTypeClass),
            ("time", TimeTypeClass),
            ("char", StringTypeClass),
            ("character", StringTypeClass),
            ("character varying", StringTypeClass),
            ("row", RecordTypeClass),
            ("struct", RecordTypeClass),
            ("list", RecordTypeClass),
            ("map", RecordTypeClass),
            ("array", ArrayTypeClass),
        ],
    )
    def test_known_type_mapping(self, data_type, expected_class):
        field_type, native = SchemaFieldTypeMapper.get_field_type(data_type)
        assert isinstance(field_type.type, expected_class)
        assert native == data_type

    def test_unknown_type_falls_back_to_null(self):
        field_type, native = SchemaFieldTypeMapper.get_field_type("jsonb")
        assert isinstance(field_type.type, NullTypeClass)
        assert native == "jsonb"

    def test_empty_type_falls_back_to_null(self):
        field_type, native = SchemaFieldTypeMapper.get_field_type("")
        assert isinstance(field_type.type, NullTypeClass)
        assert native == "NULL"

    def test_data_size_appended_to_native_type(self):
        _, native = SchemaFieldTypeMapper.get_field_type(
            "character varying", data_size=255
        )
        assert native == "character varying(255)"

    def test_data_size_none_omitted(self):
        _, native = SchemaFieldTypeMapper.get_field_type("integer", data_size=None)
        assert native == "integer"

    def test_type_lookup_is_case_insensitive(self):
        """The mapper lowercases input before lookup, so uppercase types resolve correctly."""
        field_type, _ = SchemaFieldTypeMapper.get_field_type("INTEGER")
        assert isinstance(field_type.type, NumberTypeClass)


class TestDremioAspectsExternalUrl:
    @pytest.fixture
    def aspects(self):
        return DremioAspects(
            platform="dremio",
            ui_url="https://dremio.example.com",
            env="PROD",
            ingest_owner=False,
        )

    def _make_dataset(self, path, resource_name, dataset_type=DremioDatasetType.TABLE):
        d = Mock()
        d.path = path
        d.resource_name = resource_name
        d.dataset_type = dataset_type
        return d

    def test_physical_table_uses_source_prefix(self, aspects):
        dataset = self._make_dataset(["mysource", "myschema"], "mytable")
        url = aspects._create_external_url(dataset)
        assert url.startswith("https://dremio.example.com/source/")
        assert '"mytable"' in url

    def test_view_uses_space_prefix(self, aspects):
        dataset = self._make_dataset(
            ["myspace", "folder"], "myview", DremioDatasetType.VIEW
        )
        url = aspects._create_external_url(dataset)
        assert url.startswith("https://dremio.example.com/space/")

    def test_home_path_uses_home_prefix(self, aspects):
        dataset = self._make_dataset(["@john.doe", "analysis"], "report")
        url = aspects._create_external_url(dataset)
        assert url.startswith("https://dremio.example.com/home/")

    def test_single_level_path(self, aspects):
        dataset = self._make_dataset(["mysource"], "mytable")
        url = aspects._create_external_url(dataset)
        assert url == 'https://dremio.example.com/source/"mysource"/"mytable"'

    def test_multi_level_path_correctly_quoted(self, aspects):
        dataset = self._make_dataset(["source", "folder1", "folder2"], "table")
        url = aspects._create_external_url(dataset)
        assert (
            url
            == 'https://dremio.example.com/source/"source"/"folder1"."folder2"."table"'
        )


class TestDremioAspectsDatasetProperties:
    """Tests for _create_dataset_properties() — custom_properties and timestamp handling."""

    def test_format_type_emitted_in_custom_properties(self):
        aspects = _make_aspects()
        dataset = _make_dataset(
            path=["src", "schema"],
            resource_name="tbl",
            format_type="PARQUET",
        )
        props = aspects._create_dataset_properties(dataset)
        assert props.customProperties is not None
        assert props.customProperties.get("format_type") == "PARQUET"

    def test_no_format_type_omits_custom_properties(self):
        aspects = _make_aspects()
        dataset = _make_dataset(format_type=None)
        props = aspects._create_dataset_properties(dataset)
        assert not props.customProperties

    def test_valid_created_timestamp_parsed(self):
        aspects = _make_aspects()
        dataset = _make_dataset(created="2024-06-15 12:30:45.000")
        props = aspects._create_dataset_properties(dataset)
        assert props.created is not None
        assert props.created.time > 0

    def test_empty_created_produces_no_timestamp(self):
        aspects = _make_aspects()
        dataset = _make_dataset(created="")
        props = aspects._create_dataset_properties(dataset)
        assert props.created is None

    def test_malformed_created_produces_no_timestamp(self):
        aspects = _make_aspects()
        dataset = _make_dataset(created="not-a-date")
        props = aspects._create_dataset_properties(dataset)
        assert props.created is None

    def test_qualified_name_includes_full_path(self):
        aspects = _make_aspects()
        dataset = _make_dataset(path=["db", "schema"], resource_name="orders")
        props = aspects._create_dataset_properties(dataset)
        assert props.qualifiedName == "db.schema.orders"
