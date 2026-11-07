import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from typing import Dict
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.conversions import to_bytes
from pyiceberg.manifest import DataFileContent
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable, Table
from pyiceberg.types import LongType
from requests import Response

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.iceberg.iceberg import IcebergSource
from datahub.ingestion.source.iceberg.iceberg_probe import IcebergMetadataProbe
from datahub.ingestion.source.iceberg.iceberg_profiler import IcebergProfiler
from datahub.ingestion.source.iceberg.iceberg_types import (
    load_table_with_variant_support,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BytesTypeClass,
    DatasetProfileClass,
    SchemaMetadataClass,
    StringTypeClass,
)


def _metadata() -> Dict[str, object]:
    return {
        "format-version": 3,
        "location": "s3://warehouse/ns/events",
        "last-column-id": 10,
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": True},
                    {"id": 2, "name": "payload", "type": "variant"},
                    {
                        "id": 3,
                        "name": "obj",
                        "type": {
                            "type": "struct",
                            "fields": [{"id": 4, "name": "nested", "type": "variant"}],
                        },
                    },
                    {
                        "id": 5,
                        "name": "entries",
                        "type": {
                            "type": "list",
                            "element-id": 6,
                            "element": "variant",
                            "element-required": False,
                        },
                    },
                    {
                        "id": 7,
                        "name": "labels",
                        "type": {
                            "type": "map",
                            "key-id": 8,
                            "key": "string",
                            "value-id": 9,
                            "value": "variant",
                            "value-required": False,
                        },
                    },
                    {
                        "id": 10,
                        "name": "variant",
                        "type": "string",
                        "doc": "variant",
                        "write-default": "variant",
                    },
                ],
            }
        ],
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
    }


@pytest.mark.parametrize("backend", ["rest", "file"])
@patch("requests.Session.get")
def test_variant_table_schema(
    http_get: MagicMock, backend: str, tmp_path: Path
) -> None:
    metadata = _metadata()
    original = json.dumps(metadata)
    catalog: Catalog
    if backend == "rest":
        uri = "https://catalog.example"
        responses = {
            f"{uri}/v1/config": {"defaults": {}, "overrides": {}},
            f"{uri}/v1/namespaces": {"namespaces": [["ns"]]},
            f"{uri}/v1/namespaces?parent=ns": {"namespaces": []},
            f"{uri}/v1/namespaces/ns": {"namespace": ["ns"], "properties": {}},
            f"{uri}/v1/namespaces/ns/tables": {
                "identifiers": [{"namespace": ["ns"], "name": "events"}]
            },
            f"{uri}/v1/namespaces/ns/tables/events": {
                "metadata-location": "s3://warehouse/ns/events/metadata/v1.json",
                "metadata": metadata,
            },
        }

        def respond(url: str, **kwargs: object) -> Response:
            response = Response()
            response.status_code = 200
            response._content = json.dumps(responses[url]).encode()
            return response

        http_get.side_effect = respond
        catalog = RestCatalog("test", uri=uri)
    else:
        metadata_file = tmp_path / "v1.metadata.json"
        metadata_file.write_text(original)
        mock_catalog = MagicMock(spec=Catalog)
        mock_catalog.load_table.side_effect = lambda identifier: (
            StaticTable.from_metadata(str(metadata_file))
        )
        mock_catalog.list_namespaces.side_effect = lambda ns=(): (
            [("ns",)] if not ns else []
        )
        mock_catalog.list_tables.return_value = [("ns", "events")]
        mock_catalog.load_namespace_properties.return_value = {}
        catalog = mock_catalog

    # Exercise the real catalog parsers, not preconstructed PyIceberg schema objects.
    table = load_table_with_variant_support(catalog, ("ns", "events"))
    source = IcebergSource.create(
        {"catalog": {"test": {"type": "rest"}}},
        PipelineContext(run_id="variant-test"),
    )
    with patch.object(type(source.config), "get_catalog", return_value=catalog):
        schemas = [
            wu.metadata.aspect
            for wu in source.get_workunits_internal()
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, SchemaMetadataClass)
        ]
    assert len(schemas) == 1
    fields = schemas[0].fields
    variants = [f for f in fields if f.nativeDataType == "variant"]
    assert len(variants) == 3
    assert all(isinstance(f.type.type, BytesTypeClass) for f in variants)
    assert all(f.nullable for f in variants)
    assert str(table.schema().find_type(6)) == "variant"
    assert str(table.schema().find_type(9)) == "variant"
    array_field = next(f for f in fields if f.fieldPath.endswith(".entries"))
    assert isinstance(array_field.type.type, ArrayTypeClass)
    assert array_field.type.type.nestedType == ["bytes"]
    assert array_field.nativeDataType == "list<variant>"
    ordinary_field = next(f for f in fields if f.fieldPath.endswith(".variant"))
    assert isinstance(ordinary_field.type.type, StringTypeClass)
    assert ordinary_field.description == "variant"
    assert table.schema().find_field(10).write_default == "variant"
    columns = IcebergMetadataProbe(catalog).columns("ns", "events")
    assert next(c for c in columns if c["name"] == "payload")["type"] == "variant"
    assert json.dumps(metadata) == original

    profiled_table = MagicMock()
    profiled_table.schema.return_value = table.schema()
    snapshot = profiled_table.current_snapshot.return_value
    snapshot.summary.additional_properties = {"total-records": "10"}
    data_file = MagicMock(
        content=DataFileContent.DATA,
        null_value_counts={1: 0, 2: 2},
        lower_bounds={1: to_bytes(LongType(), 0), 2: b"opaque variant"},
        upper_bounds={1: to_bytes(LongType(), 20), 2: b"opaque variant"},
    )
    manifest = MagicMock()
    manifest.fetch_manifest_entry.return_value = [MagicMock(data_file=data_file)]
    snapshot.manifests.return_value = [manifest]
    profile = next(
        iter(
            IcebergProfiler(source.report, source.config.profiling).profile_table(
                "ns.events", profiled_table
            )
        )
    )
    assert isinstance(profile, DatasetProfileClass)
    assert profile.fieldProfiles is not None
    profiles = {f.fieldPath: f for f in profile.fieldProfiles}
    assert profiles["id"].min == "0"
    assert profiles["id"].max == "20"
    assert profiles["payload"].nullCount == 2
    assert profiles["payload"].min is None
    assert profiles["payload"].max is None


def test_variant_adapter_leaves_other_callers_and_invalid_types_unchanged() -> None:
    schema = {
        "type": "struct",
        "fields": [{"id": 1, "name": "payload", "type": "variant"}],
    }
    catalog = MagicMock(spec=Catalog)
    catalog.load_table.side_effect = lambda identifier: Schema.model_validate(schema)
    load_table_with_variant_support(catalog, ("ns", "events"))
    assert schema["fields"] == [{"id": 1, "name": "payload", "type": "variant"}]
    with pytest.raises(ValidationError):
        Schema.model_validate(schema)
    schema["fields"] = [{"id": 1, "name": "payload", "type": "invalid_type"}]
    with pytest.raises(ValidationError):
        load_table_with_variant_support(catalog, ("ns", "events"))
    schema["fields"] = [{"id": 1, "name": "payload", "type": "variant"}]
    with pytest.raises(ValidationError):
        Schema.model_validate(schema)


def test_variant_adapter_isolates_concurrent_catalog_loads() -> None:
    loading, release = Event(), Event()
    schema = {"fields": [{"id": 1, "name": "payload", "type": "variant"}]}
    catalog = MagicMock(spec=Catalog)

    def load(identifier: object) -> Table:
        loading.set()
        assert release.wait(timeout=5)
        table = MagicMock(spec=Table)
        table.schema.return_value = Schema.model_validate(schema)
        return table

    catalog.load_table.side_effect = load
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            load_table_with_variant_support, catalog, ("ns", "events")
        )
        try:
            assert loading.wait(timeout=5)
            with pytest.raises(ValidationError):
                Schema.model_validate(schema)
        finally:
            release.set()
        assert str(future.result().schema().find_type(1)) == "variant"
