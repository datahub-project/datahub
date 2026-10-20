from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pytest
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.exceptions import (
    ForbiddenError,
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
)
from pyiceberg.io import PY_IO_IMPL, load_file_io
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadataUtil, TableMetadataV2
from pyiceberg.types import LongType, NestedField, StringType

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
)
from datahub.ingestion.source.iceberg.iceberg_common import (
    IcebergSourceConfig,
    table_custom_properties,
)
from datahub.ingestion.source.iceberg.iceberg_probe import (
    _NAMESPACE_EFFECT,
    _NAMESPACES_EFFECT,
    IcebergMetadataProbe,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass
from tests.test_helpers.iceberg_probe_parity import LISTINGS
from tests.test_helpers.probe_parity import assert_probe_parity, pipeline_ingestion

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

    with pytest.raises(ProbeArgumentError, match="'sales.eu'"):
        run_probe_method("iceberg", _config_dict(), "tables", {"namespace": "sales.eu"})


def test_an_unknown_namespace_is_a_caller_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_catalog(monkeypatch, _FakeCatalog({("sales",): []}))

    with pytest.raises(ProbeArgumentError, match="'nope'"):
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

    with pytest.raises(ProbeArgumentError, match="'nope'") as raised:
        run_probe_method(
            "iceberg",
            _config_dict(),
            "columns",
            {"namespace": "sales", "table": "nope"},
        )
    # Glue and Hive leave non-Iceberg tables out of the listing, so the message
    # must not read as "the table does not exist" to someone who can see it.
    assert "only Iceberg tables" in str(raised.value)


@pytest.mark.parametrize(
    "error, reason",
    [
        # The race: listed as Iceberg, then its type changed before load_table.
        (NoSuchIcebergTableError("table_type is HIVE"), "not an Iceberg table"),
        # Typed Iceberg but registered without metadata_location: it passes
        # the listing, so no race is needed; it is not "not Iceberg".
        (NoSuchPropertyException("metadata_location is missing"), "property"),
    ],
)
def test_an_unloadable_listed_table_is_an_empty_answer_naming_why(
    monkeypatch: pytest.MonkeyPatch, error: Exception, reason: str
) -> None:
    catalog = _FakeCatalog({("sales",): ["legacy"]})
    _fail_load_table(monkeypatch, catalog, error)
    _patch_catalog(monkeypatch, catalog)

    # It exists, so this is the source's, not a wrong argument: an empty
    # answer with a warning naming it and why, as ingestion skips it with one.
    for command, empty in (("columns", []), ("table_metadata", {})):
        result = run_probe_method(
            "iceberg",
            _config_dict(),
            command,
            {"namespace": "sales", "table": "legacy"},
        )
        assert result.result == empty
        named = [w for w in result.warnings if "'sales.legacy'" in w]
        assert named and all(reason in w for w in named)


def _fail_load_table(
    monkeypatch: pytest.MonkeyPatch, catalog: _FakeCatalog, error: Exception
) -> None:
    def load_table(identifier: Identifier) -> Table:
        raise error

    monkeypatch.setattr(catalog, "load_table", load_table)


def test_a_file_io_that_cannot_start_is_an_environment_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Exit 1, as for any missing module: retrying cannot help. The class path
    # is what tells the operator which package is missing.
    catalog = _FakeCatalog({("sales",): ["orders"]})

    def _failing_load_table(identifier: Identifier) -> Table:
        # pyiceberg's own raise, not a copy of its message: it signals this
        # with a bare ValueError, so the probe (like ingestion) matches the
        # text, and a pyiceberg rewording must fail here.
        load_file_io({PY_IO_IMPL: "no_such_module.NoSuchFileIO"})
        raise AssertionError("load_file_io loaded a module that does not exist")

    monkeypatch.setattr(catalog, "load_table", _failing_load_table)
    _patch_catalog(monkeypatch, catalog)

    with pytest.raises(ProbeInternalError) as raised:
        run_probe_method(
            "iceberg",
            _config_dict(),
            "table_metadata",
            {"namespace": "sales", "table": "orders"},
        )
    assert "no_such_module.NoSuchFileIO" in str(raised.value)


def test_a_py_io_impl_that_is_not_a_class_path_is_an_environment_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = _FakeCatalog({("sales",): ["orders"]})

    def _failing_load_table(identifier: Identifier) -> Table:
        load_file_io({PY_IO_IMPL: "NoDots"})
        raise AssertionError("load_file_io accepted a bare class name")

    monkeypatch.setattr(catalog, "load_table", _failing_load_table)
    _patch_catalog(monkeypatch, catalog)

    with pytest.raises(ProbeInternalError) as raised:
        run_probe_method(
            "iceberg",
            _config_dict(),
            "columns",
            {"namespace": "sales", "table": "orders"},
        )
    assert "NoDots" in str(raised.value)


def test_a_source_value_error_on_load_is_the_sources_not_the_callers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Glue raises a plain ValueError for a table missing its Parameters:
    # bad source data, which ingestion records as a table failure.
    catalog = _FakeCatalog({("sales",): ["orders"]})
    _fail_load_table(
        monkeypatch, catalog, ValueError("Glue table is missing Parameters property")
    )
    _patch_catalog(monkeypatch, catalog)

    with pytest.raises(ProbeConnectionError) as raised:
        run_probe_method(
            "iceberg",
            _config_dict(),
            "columns",
            {"namespace": "sales", "table": "orders"},
        )
    assert "'sales.orders'" in str(raised.value)
    assert "Parameters" not in str(raised.value)


def test_a_listed_table_that_cannot_be_loaded_is_the_sources_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = _FakeCatalog({("sales",): ["orders"]})
    _fail_load_table(monkeypatch, catalog, NoSuchTableError("gone"))
    _patch_catalog(monkeypatch, catalog)

    with pytest.raises(ProbeConnectionError, match="'sales.orders'"):
        run_probe_method(
            "iceberg",
            _config_dict(),
            "columns",
            {"namespace": "sales", "table": "orders"},
        )


@pytest.mark.parametrize(
    "command, method",
    [
        ("tables", "list_tables"),
        ("namespace_properties", "load_namespace_properties"),
    ],
)
def test_a_listed_namespace_that_cannot_be_read_is_the_sources_state(
    monkeypatch: pytest.MonkeyPatch, command: str, method: str
) -> None:
    # Dropped since the listing, or one Glue lists but refuses to read (a
    # database named with a dot): not the caller's mistake.
    catalog = _FakeCatalog({("sales",): ["orders"]})

    def fail(namespace: Identifier) -> object:
        raise NoSuchNamespaceError("sales")

    monkeypatch.setattr(catalog, method, fail)
    _patch_catalog(monkeypatch, catalog)

    with pytest.raises(ProbeConnectionError) as raised:
        run_probe_method("iceberg", _config_dict(), command, {"namespace": "sales"})
    assert _NAMESPACE_EFFECT in str(raised.value)


@pytest.mark.parametrize(
    "method, effect",
    [
        ("list_namespaces", _NAMESPACES_EFFECT),
        ("list_tables", _NAMESPACE_EFFECT),
    ],
)
def test_a_failure_names_the_call_that_failed(
    monkeypatch: pytest.MonkeyPatch, method: str, effect: str
) -> None:
    # Ingestion fails the run on the first and skips a namespace on the
    # second, so the two must not read alike.
    catalog = _FakeCatalog({("sales",): ["orders"]})

    def fail(*args: object) -> object:
        raise ForbiddenError("PLANTED-server-text")

    monkeypatch.setattr(catalog, method, fail)
    _patch_catalog(monkeypatch, catalog)

    with pytest.raises(ProbeConnectionError) as raised:
        run_probe_method("iceberg", _config_dict(), "tables", {"namespace": "sales"})
    assert effect in str(raised.value)
    assert "PLANTED" not in str(raised.value)


def test_a_glue_client_error_is_labelled_with_its_code() -> None:
    error = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "PLANTED"}},
        "GetDatabases",
    )
    assert IcebergMetadataProbe.probe_error_code(error) == "AccessDeniedException"
    assert IcebergMetadataProbe.probe_error_code(ValueError("x")) is None


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
    assert metadata["owner"] == "someone"
    assert metadata["format-version"] == "2"


_PARITY_CATALOG: Dict[Identifier, Sequence[str]] = {
    ("sales",): ["orders", "orders_tmp", "refunds"],
    ("ops",): ["jobs"],
}
_PARITY_PATTERNS: Dict[str, object] = {
    "namespace_pattern": {"deny": ["^ops$"]},
    # Written against the qualified name, which is what ingestion matches.
    "table_pattern": {"allow": [r"^sales\.orders.*"], "deny": [r".*_tmp$"]},
}


@pytest.mark.usefixtures("_isolate_secret_registry")
def test_probe_filter_agrees_with_ingestion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_catalog(monkeypatch, _FakeCatalog(_PARITY_CATALOG))

    report = assert_probe_parity(
        "iceberg",
        _config_dict(**_PARITY_PATTERNS),
        pipeline_ingestion("iceberg", tmp_path),
        LISTINGS,
    )

    assert report.excluded_by("namespaces") == {"ops": "namespace_pattern"}
    assert report.excluded_by("tables") == {
        "ops.jobs": "namespace_pattern",
        "sales.orders_tmp": "table_pattern",
        "sales.refunds": "table_pattern",
    }


@pytest.mark.usefixtures("_isolate_secret_registry")
def test_probe_filter_agrees_with_ingestion_on_a_real_catalog(
    tmp_path: Path,
) -> None:
    # The same parity, through the real get_catalog/load_catalog path and a
    # real pyiceberg catalog. The nested namespace's table matches
    # table_pattern, so it is kept out only because ingestion never descends
    # below top-level namespaces -- and the probe must not either.
    pytest.importorskip("pyiceberg.catalog.sql")
    catalog_properties = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": f"file://{tmp_path}/warehouse",
    }
    catalog = load_catalog("test", **catalog_properties)
    schema = Schema(NestedField(1, "id", LongType(), required=True))
    for namespace, names in {
        **_PARITY_CATALOG,
        ("sales", "orders_archive"): ["jan"],
    }.items():
        catalog.create_namespace(namespace)
        for name in names:
            catalog.create_table((*namespace, name), schema)
    catalog.close()

    report = assert_probe_parity(
        "iceberg",
        {"catalog": {"test": catalog_properties}, **_PARITY_PATTERNS},
        pipeline_ingestion("iceberg", tmp_path),
        LISTINGS,
    )

    assert report.kinds["tables"].included == {"sales.orders"}
    assert report.excluded_by("namespaces") == {"ops": "namespace_pattern"}


@pytest.mark.usefixtures("_isolate_secret_registry")
def test_table_metadata_is_the_custom_properties_ingestion_records(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    catalog = _FakeCatalog({("sales",): ["orders"]})
    _patch_catalog(monkeypatch, catalog)
    emitted = pipeline_ingestion("iceberg", tmp_path)(_config_dict())
    recorded = [
        aspect.customProperties
        for aspects in emitted.aspects.values()
        for aspect in aspects
        if isinstance(aspect, DatasetPropertiesClass)
    ]

    probed = run_probe_method(
        "iceberg",
        _config_dict(),
        "table_metadata",
        {"namespace": "sales", "table": "orders"},
    ).result

    assert recorded == [probed]
    assert probed == table_custom_properties(
        catalog.load_table(("sales", "orders")), "[]"
    )


def test_a_v1_default_spec_id_without_its_spec_is_recorded_as_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # pyiceberg validates default-spec-id against partition-specs for format
    # v2 and v3 only, so this v1 metadata loads and Table.spec() raises
    # StopIteration. Ingestion records the spec as None and keeps the rest.
    v1 = TableMetadataUtil.parse_obj(
        {
            "format-version": 1,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": "s3://warehouse/sales/legacy",
            "last-updated-ms": 1,
            "last-column-id": 1,
            "schema": {
                "type": "struct",
                "fields": [{"id": 1, "name": "id", "required": True, "type": "long"}],
            },
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 7,
        }
    )
    loaded = Table(
        identifier=("sales", "legacy"),
        metadata=v1,
        metadata_location="s3://warehouse/sales/legacy/metadata/v1.json",
        io=PyArrowFileIO(),
        catalog=NoopCatalog("test"),
    )
    catalog = _FakeCatalog({("sales",): ["legacy"]})
    monkeypatch.setattr(catalog, "load_table", lambda identifier: loaded)
    _patch_catalog(monkeypatch, catalog)

    result = run_probe_method(
        "iceberg",
        _config_dict(),
        "table_metadata",
        {"namespace": "sales", "table": "legacy"},
    )

    assert result.result == table_custom_properties(loaded, None)
    assert any("partition spec" in w for w in result.warnings)


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


def test_a_table_without_its_namespace_is_judged_bare_and_says_so() -> None:
    # table_pattern is written against `<namespace>.<table>`: a bare name
    # judged against it is not the verdict ingestion makes.
    result = check_filters(
        source_type="iceberg",
        config_dict=_config_dict(**_PARITY_PATTERNS),
        kind="Table",
        parent_path=[],
        names=["orders"],
    )

    assert result.results[0].target == "orders"
    assert len(result.warnings) == 1


def test_a_namespace_is_top_level_so_needs_no_parent() -> None:
    result = check_filters(
        source_type="iceberg",
        config_dict=_config_dict(**_PARITY_PATTERNS),
        kind="Namespace",
        parent_path=[],
        names=["sales"],
    )

    assert result.results[0].target == "sales"
    assert result.warnings == []
