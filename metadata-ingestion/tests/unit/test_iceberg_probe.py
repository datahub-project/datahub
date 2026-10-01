from typing import Dict, List, Optional, Sequence, Set, Tuple

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

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.iceberg.iceberg import IcebergSource
from datahub.ingestion.source.iceberg.iceberg_common import IcebergSourceConfig
from datahub.utilities.urns.dataset_urn import DatasetUrn

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


def test_tables_returns_bare_names_with_the_namespace_as_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_catalog(monkeypatch, _FakeCatalog({("sales",): ["orders", "orders_tmp"]}))

    result = run_probe_method(
        "iceberg",
        _config_dict(table_pattern={"deny": [".*_tmp$"]}),
        "tables",
        {"namespace": "sales"},
    )

    assert result.result == ["orders", "orders_tmp"]
    assert result.kind == "Table"
    assert result.parent_path == ["sales"]


def test_a_dotted_single_level_namespace_is_addressed_whole(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = _FakeCatalog({("team.a",): ["events"]})
    _patch_catalog(monkeypatch, catalog)

    result = run_probe_method(
        "iceberg", _config_dict(), "tables", {"namespace": "team.a"}
    )

    assert result.result == ["events"]
    assert catalog.list_tables_calls == [("team.a",)]


def test_a_nested_namespace_is_refused_because_ingestion_never_reads_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_catalog(
        monkeypatch,
        _FakeCatalog({("sales",): ["orders"], ("sales", "eu"): ["orders_eu"]}),
    )

    with pytest.raises(ValueError, match="top-level"):
        run_probe_method(
            "iceberg", _config_dict(), "tables", {"namespace": "sales.eu"}
        )


def test_an_unknown_namespace_is_a_caller_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_catalog(monkeypatch, _FakeCatalog({("sales",): []}))

    with pytest.raises(ValueError):
        run_probe_method("iceberg", _config_dict(), "tables", {"namespace": "nope"})


def test_namespace_properties_are_returned(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_catalog(monkeypatch, _FakeCatalog({("sales",): []}))

    result = run_probe_method(
        "iceberg", _config_dict(), "namespace_properties", {"namespace": "sales"}
    )

    assert result.result == {"location": "s3://warehouse/sales"}


def test_columns_reports_the_current_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_catalog(monkeypatch, _FakeCatalog({("sales",): ["orders"]}))

    result = run_probe_method(
        "iceberg",
        _config_dict(),
        "columns",
        {"namespace": "sales", "table": "orders"},
    )

    assert result.result == [
        {"id": 1, "name": "id", "type": "long", "required": True, "doc": None},
        {
            "id": 2,
            "name": "name",
            "type": "string",
            "required": False,
            "doc": "display name",
        },
    ]


def test_columns_of_a_missing_table_is_a_caller_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_catalog(monkeypatch, _FakeCatalog({("sales",): ["orders"]}))

    with pytest.raises(ValueError):
        run_probe_method(
            "iceberg",
            _config_dict(),
            "columns",
            {"namespace": "sales", "table": "nope"},
        )


def test_a_non_iceberg_table_is_reported_as_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_catalog(
        monkeypatch,
        _FakeCatalog({("sales",): ["legacy"]}, not_iceberg=[("sales", "legacy")]),
    )

    with pytest.raises(ValueError, match="not an Iceberg table"):
        run_probe_method(
            "iceberg",
            _config_dict(),
            "columns",
            {"namespace": "sales", "table": "legacy"},
        )


def test_a_file_io_that_cannot_start_is_a_connection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Exit 3, not 2: ingestion skips the table with a warning, and the cause
    # is a FileIO implementation missing from the environment (or named by
    # the catalog server), not an argument the caller can fix.
    catalog = _FakeCatalog({("sales",): ["orders"]})

    def _failing_load_table(identifier: Identifier) -> Table:
        raise ValueError("Could not initialize FileIO: my_module.MyFileIO")

    monkeypatch.setattr(catalog, "load_table", _failing_load_table)
    _patch_catalog(monkeypatch, catalog)

    with pytest.raises(ProbeConnectionError, match="FileIO"):
        run_probe_method(
            "iceberg",
            _config_dict(),
            "table_metadata",
            {"namespace": "sales", "table": "orders"},
        )


def test_table_metadata_never_exposes_file_io_properties(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vended = {
        "s3.access-key-id": "AKIAEXAMPLE",
        "s3.secret-access-key": "vended-secret",
    }
    _patch_catalog(
        monkeypatch, _FakeCatalog({("sales",): ["orders"]}, io_properties=vended)
    )

    for command in ("table_metadata", "columns"):
        result = run_probe_method(
            "iceberg",
            _config_dict(),
            command,
            {"namespace": "sales", "table": "orders"},
        )
        rendered = repr(result.to_dict())
        assert "AKIAEXAMPLE" not in rendered
        assert "vended-secret" not in rendered

    metadata = run_probe_method(
        "iceberg",
        _config_dict(),
        "table_metadata",
        {"namespace": "sales", "table": "orders"},
    ).result
    assert isinstance(metadata, dict)
    assert metadata["properties"] == {"owner": "someone", "comment": "a table"}
    assert metadata["format_version"] == 2


_PARITY_CATALOG: Dict[Identifier, Sequence[str]] = {
    ("sales",): ["orders", "orders_tmp", "refunds"],
    ("ops",): ["jobs"],
}
_PARITY_PATTERNS: Dict[str, object] = {
    "namespace_pattern": {"deny": ["^ops$"]},
    # Written against the qualified name, which is what ingestion matches.
    "table_pattern": {"allow": [r"^sales\.orders.*"], "deny": [r".*_tmp$"]},
}


def _ingested_dataset_names(monkeypatch: pytest.MonkeyPatch) -> Set[str]:
    _patch_catalog(monkeypatch, _FakeCatalog(_PARITY_CATALOG))
    config = IcebergSourceConfig.model_validate(_config_dict(**_PARITY_PATTERNS))
    source = IcebergSource(config, PipelineContext(run_id="iceberg-probe-parity"))
    names: Set[str] = set()
    for wu in source.get_workunits_internal():
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        urn = wu.metadata.entityUrn
        if urn and urn.startswith("urn:li:dataset:"):
            names.add(DatasetUrn.from_string(urn).name)
    return names


def _probe_included_dataset_names(monkeypatch: pytest.MonkeyPatch) -> Set[str]:
    _patch_catalog(monkeypatch, _FakeCatalog(_PARITY_CATALOG))
    config_dict = _config_dict(**_PARITY_PATTERNS)
    namespaces = run_probe_method("iceberg", config_dict, "namespaces", {}).result
    assert isinstance(namespaces, list)
    included: Set[str] = set()
    for namespace in namespaces:
        listing = run_probe_method(
            "iceberg", config_dict, "tables", {"namespace": namespace}
        )
        assert isinstance(listing.result, list)
        verdicts = check_filters(
            source_type="iceberg",
            config_dict=config_dict,
            kind=str(listing.kind),
            parent_path=listing.parent_path,
            names=listing.result,
        )
        # A degraded verdict (bare-name match, ignored parent) would agree
        # with ingestion here only by accident.
        assert not [w for w in verdicts.warnings if "bare name" in w]
        assert not [w for w in verdicts.warnings if "does not declare" in w]
        included |= {f"{namespace}.{r.name}" for r in verdicts.results if r.included}
    return included


def test_probe_filter_agrees_with_ingestion(monkeypatch: pytest.MonkeyPatch) -> None:
    ingested = _ingested_dataset_names(monkeypatch)

    # Pinned so a broken fake cannot make both sides agree on an empty set.
    assert ingested == {"sales.orders"}
    assert _probe_included_dataset_names(monkeypatch) == ingested


def test_a_table_is_judged_on_its_qualified_name() -> None:
    result = check_filters(
        source_type="iceberg",
        config_dict=_config_dict(**_PARITY_PATTERNS),
        kind="Table",
        parent_path=["sales"],
        names=["orders"],
    )

    assert result.pattern_field == "table_pattern"
    assert result.results[0].target == "sales.orders"
    assert result.results[0].included


def test_a_table_in_a_denied_namespace_is_excluded_by_the_namespace() -> None:
    result = check_filters(
        source_type="iceberg",
        config_dict=_config_dict(**_PARITY_PATTERNS),
        kind="Table",
        parent_path=["ops"],
        names=["jobs"],
    )

    assert not result.results[0].included
    assert result.results[0].excluded_by == "namespace_pattern"


def test_a_namespace_is_judged_on_its_own_name() -> None:
    result = check_filters(
        source_type="iceberg",
        config_dict=_config_dict(**_PARITY_PATTERNS),
        kind="Namespace",
        parent_path=[],
        names=["sales", "ops"],
    )

    assert result.pattern_field == "namespace_pattern"
    assert [(r.target, r.included) for r in result.results] == [
        ("sales", True),
        ("ops", False),
    ]
