from typing import Dict, List, Optional, Sequence, Tuple

import pytest
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.exceptions import (
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.types import LongType, NestedField, StringType

from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.iceberg.iceberg_common import IcebergSourceConfig

Identifier = Tuple[str, ...]


def _table(
    identifier: Identifier, io_properties: Optional[Dict[str, str]] = None
) -> Table:
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=False, doc="display name"),
        schema_id=0,
    )
    return Table(
        identifier=identifier,
        metadata=TableMetadataV2(
            partition_specs=[PartitionSpec(spec_id=0)],
            location=f"s3://warehouse/{'/'.join(identifier)}",
            last_column_id=2,
            schemas=[schema],
            current_schema_id=0,
            properties={"owner": "someone", "comment": "a table"},
        ),
        metadata_location=f"s3://warehouse/{'/'.join(identifier)}/metadata/v1.json",
        io=PyArrowFileIO(properties=io_properties or {}),
        # Table needs a catalog only for writes.
        catalog=NoopCatalog("test"),
    )


class _FakeCatalog:
    """The four Catalog methods ingestion calls, plus close().

    list_namespaces follows pyiceberg's contract in every catalog
    implementation: with no argument it returns top-level namespaces only.
    """

    def __init__(
        self,
        tables: Dict[Identifier, Sequence[str]],
        not_iceberg: Sequence[Identifier] = (),
        io_properties: Optional[Dict[str, str]] = None,
    ) -> None:
        self._tables = {ns: list(names) for ns, names in tables.items()}
        self._not_iceberg = set(not_iceberg)
        self._io_properties = io_properties
        self.closed = False
        self.list_tables_calls: List[Identifier] = []

    def list_namespaces(self, namespace: Identifier = ()) -> List[Identifier]:
        depth = len(namespace) + 1
        return [
            ns
            for ns in self._tables
            if len(ns) == depth and ns[: depth - 1] == tuple(namespace)
        ]

    def list_tables(self, namespace: Identifier) -> List[Identifier]:
        self.list_tables_calls.append(tuple(namespace))
        if tuple(namespace) not in self._tables:
            raise NoSuchNamespaceError(str(namespace))
        return [(*namespace, name) for name in self._tables[tuple(namespace)]]

    def load_namespace_properties(self, namespace: Identifier) -> Dict[str, str]:
        if tuple(namespace) not in self._tables:
            raise NoSuchNamespaceError(str(namespace))
        return {"location": f"s3://warehouse/{'/'.join(namespace)}"}

    def load_table(self, identifier: Identifier) -> Table:
        identifier = tuple(identifier)
        if identifier in self._not_iceberg:
            raise NoSuchIcebergTableError(str(identifier))
        if identifier[-1] not in self._tables.get(identifier[:-1], []):
            raise NoSuchTableError(str(identifier))
        return _table(identifier, self._io_properties)

    def close(self) -> None:
        self.closed = True


def _config_dict(**overrides: object) -> Dict[str, object]:
    return {
        "catalog": {"test": {"type": "rest", "uri": "http://localhost:8181"}},
        **overrides,
    }


def _patch_catalog(monkeypatch: pytest.MonkeyPatch, catalog: _FakeCatalog) -> None:
    monkeypatch.setattr(IcebergSourceConfig, "get_catalog", lambda self: catalog)


def test_namespaces_lists_denied_namespaces_too(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = _FakeCatalog({("sales",): ["orders"], ("ops",): ["jobs"]})
    _patch_catalog(monkeypatch, catalog)

    result = run_probe_method(
        "iceberg",
        _config_dict(namespace_pattern={"deny": ["^ops$"]}),
        "namespaces",
        {},
    )

    assert result.result == ["ops", "sales"]
    assert result.kind == "Namespace"


def test_the_catalog_is_closed_after_the_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = _FakeCatalog({("sales",): []})
    _patch_catalog(monkeypatch, catalog)

    run_probe_method("iceberg", _config_dict(), "namespaces", {})

    assert catalog.closed
