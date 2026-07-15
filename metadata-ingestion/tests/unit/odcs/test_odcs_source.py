"""Unit tests for the ODCS source — path resolution, validation, binding, stateful wiring."""

import pathlib
from functools import partial
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.odcs.odcs_config import ODCSSourceConfig
from datahub.ingestion.source.odcs.odcs_source import ODCSSource
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    auto_stale_entity_removal,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    DataPlatformInfoClass,
    DatasetPropertiesClass,
    LogicalParentClass,
)


def _make_source(
    tmp_path: pathlib.Path,
    graph: Optional[Any] = None,
    **overrides: Any,
) -> ODCSSource:
    config_dict: Dict[str, Any] = {"path": str(tmp_path)}
    config_dict.update(overrides)
    config = ODCSSourceConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test")
    if graph is not None:
        ctx.graph = graph
    return ODCSSource(config=config, ctx=ctx)


def _write(p: pathlib.Path, body: str = "id: x\n") -> pathlib.Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding="utf-8")
    return p


_VALID_CONTRACT_BODY = """\
apiVersion: v3.1.0
kind: DataContract
id: test-contract-1
version: 1.0.0
status: active
servers:
  - server: prod-postgres
    type: postgres
    host: postgres.example.internal
    port: 5432
    database: appdb
    schema: public
schema:
  - name: t
    physicalName: t
    properties:
      - name: id
        logicalType: number
        quality:
          - id: t_id_no_nulls
            metric: nullValues
            mustBe: 0
"""


def _aspects_of(workunits: List, aspect_cls: type) -> List:
    out = []
    for wu in workunits:
        aspect = getattr(getattr(wu, "metadata", None), "aspect", None)
        if isinstance(aspect, aspect_cls):
            out.append(aspect)
    return out


# ---------------------------------------------------------------------------
# _resolve_paths
# ---------------------------------------------------------------------------


def test_resolve_single_file(tmp_path: pathlib.Path) -> None:
    f = _write(tmp_path / "one.odcs.yaml")
    src = _make_source(tmp_path, path=str(f))
    assert [str(p) for p in src._resolve_paths()] == [str(f)]


def test_resolve_directory_returns_multiple(tmp_path: pathlib.Path) -> None:
    f1 = _write(tmp_path / "a.odcs.yaml")
    f2 = _write(tmp_path / "b.odcs.yaml")
    src = _make_source(tmp_path, path=str(tmp_path))
    found = sorted(str(p) for p in src._resolve_paths())
    assert found == sorted([str(f1), str(f2)])


def test_resolve_glob_pattern(tmp_path: pathlib.Path) -> None:
    _write(tmp_path / "a.odcs.yaml")
    _write(tmp_path / "b.odcs.yaml")
    _write(tmp_path / "c.txt")  # filtered: not in default file_extensions
    src = _make_source(tmp_path, path=str(tmp_path / "*.odcs.yaml"))
    found = list(src._resolve_paths())
    assert len(found) == 2


def test_resolve_glob_zero_matches_returns_empty(tmp_path: pathlib.Path) -> None:
    src = _make_source(tmp_path, path=str(tmp_path / "no-such-*.odcs.yaml"))
    assert list(src._resolve_paths()) == []


def test_resolve_dedups_overlapping_paths(tmp_path: pathlib.Path) -> None:
    f = _write(tmp_path / "a.odcs.yaml")
    src = _make_source(tmp_path, path=[str(tmp_path), str(f), str(tmp_path)])
    found = list(src._resolve_paths())
    assert len(found) == 1
    assert str(found[0]) == str(f)


def test_resolve_skips_nonexistent_with_warning(tmp_path: pathlib.Path) -> None:
    missing = tmp_path / "nope" / "missing.yaml"
    src = _make_source(tmp_path, path=str(missing))
    assert list(src._resolve_paths()) == []
    assert len(src.report.warnings) >= 1


def test_resolve_directory_skips_non_yaml_extensions(tmp_path: pathlib.Path) -> None:
    yaml_f = _write(tmp_path / "a.odcs.yaml")
    _write(tmp_path / "ignore.txt")
    _write(tmp_path / "ignore.md")
    src = _make_source(tmp_path, path=str(tmp_path))
    found = [str(p) for p in src._resolve_paths()]
    assert found == [str(yaml_f)]


# ---------------------------------------------------------------------------
# Symlink policy
# ---------------------------------------------------------------------------


def test_symlink_skipped_by_default(tmp_path: pathlib.Path) -> None:
    real = _write(tmp_path / "real.odcs.yaml")
    link = tmp_path / "link.odcs.yaml"
    link.symlink_to(real)

    src = _make_source(tmp_path, path=str(tmp_path))
    found = sorted(str(p) for p in src._resolve_paths())
    assert str(real) in found
    assert str(link) not in found
    assert len(src.report.warnings) >= 1


def test_symlink_followed_when_inside_root(tmp_path: pathlib.Path) -> None:
    real = _write(tmp_path / "real.odcs.yaml")
    link = tmp_path / "link.odcs.yaml"
    link.symlink_to(real)

    src = _make_source(tmp_path, path=str(tmp_path), follow_symlinks=True)
    found = sorted(str(p) for p in src._resolve_paths())
    assert str(real) in found
    assert str(link) in found


def test_symlink_outside_root_rejected(tmp_path: pathlib.Path) -> None:
    outside_dir = tmp_path / "outside"
    inside_dir = tmp_path / "inside"
    outside_dir.mkdir()
    inside_dir.mkdir()
    target = _write(outside_dir / "secret.odcs.yaml")
    link = inside_dir / "link.odcs.yaml"
    link.symlink_to(target)

    src = _make_source(tmp_path, path=str(inside_dir), follow_symlinks=True)
    found = [str(p) for p in src._resolve_paths()]
    assert str(link) not in found
    assert len(src.report.warnings) >= 1


# ---------------------------------------------------------------------------
# max_input_file_bytes
# ---------------------------------------------------------------------------


def test_max_input_file_bytes_skips_large_files(tmp_path: pathlib.Path) -> None:
    big = tmp_path / "big.odcs.yaml"
    small = tmp_path / "small.odcs.yaml"
    big.write_text("a" * 5000, encoding="utf-8")
    small.write_text("id: x\n", encoding="utf-8")

    src = _make_source(tmp_path, path=str(tmp_path), max_input_file_bytes=1000)
    assert src._load_yaml(big) is None
    assert str(big) in src.report.files_skipped

    loaded_small = src._load_yaml(small)
    assert loaded_small is not None


# ---------------------------------------------------------------------------
# Validation modes (default strict_validation=False)
# ---------------------------------------------------------------------------


def test_validate_strict_mode_fails_broken_doc(tmp_path: pathlib.Path) -> None:
    src = _make_source(tmp_path, strict_validation=True)
    bad: dict = {"foo": "bar"}
    ok = src._validate(bad, tmp_path / "bad.yaml")
    assert not ok
    assert len(src.report.warnings) >= 1


def test_validate_lenient_default_proceeds_on_invalid_doc(
    tmp_path: pathlib.Path,
) -> None:
    """Default `strict_validation=False` lets a schema-invalid doc through with
    warnings (real-world files often carry non-conformant extras)."""
    src = _make_source(tmp_path)
    raw_dict: dict = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "x",
        "rule": [{"name": "not-a-spec-field-here"}],
    }
    ok = src._validate(raw_dict, tmp_path / "deprecated.yaml")
    assert ok
    assert len(src.report.warnings) >= 1


# ---------------------------------------------------------------------------
# Unknown-field classification
# ---------------------------------------------------------------------------


def test_unknown_field_increments_counter_and_warns(tmp_path: pathlib.Path) -> None:
    src = _make_source(tmp_path)
    raw_dict: dict = {
        "apiVersion": "v3.1.0",
        "id": "x",
        "frobozz": 42,  # unknown top-level field
    }
    src._warn_unknown_fields(raw_dict, tmp_path / "f.yaml")
    assert src.report.unknown_fields_count >= 1
    assert any(
        "Unknown ODCS field" in str(getattr(w, "title", ""))
        for w in src.report.warnings
    )


def test_spec_valid_unmapped_field_is_info_not_warning(tmp_path: pathlib.Path) -> None:
    """`support` / `slaProperties` etc. are valid ODCS the source deliberately
    does not map — they produce ONE info per file, never a 'check spelling'
    warning."""
    src = _make_source(tmp_path)
    raw_dict: dict = {
        "apiVersion": "v3.1.0",
        "id": "x",
        "support": [{"channel": "slack", "url": "https://x.example/slack"}],
        "slaProperties": [{"property": "latency", "value": 4}],
        "schema": [{"name": "t", "relationships": []}],
    }
    src._warn_unknown_fields(raw_dict, tmp_path / "f.yaml")
    assert src.report.unknown_fields_count == 0
    assert not src.report.warnings
    assert any("support" in entry for entry in src.report.spec_fields_ignored)
    assert any("slaProperties" in entry for entry in src.report.spec_fields_ignored)
    assert any("relationships" in entry for entry in src.report.spec_fields_ignored)


# ---------------------------------------------------------------------------
# Platform registration, binding, and assertion emission
# ---------------------------------------------------------------------------


def test_platform_info_emitted_once_and_not_primary(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    src = _make_source(tmp_path, path=str(contract_file))
    workunits = list(src.get_workunits_internal())
    platform_wus = [
        wu
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), DataPlatformInfoClass)
    ]
    assert len(platform_wus) == 1
    assert platform_wus[0].metadata.aspect.name == "odcs"
    assert not platform_wus[0].is_primary_source


def test_binding_derived_from_server_type(tmp_path: pathlib.Path) -> None:
    """No config mappings at all: the typed postgres server yields a fully
    qualified physical URN and a logicalParent link."""
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    src = _make_source(tmp_path, path=str(contract_file))
    workunits = list(src.get_workunits_internal())

    parent_wus = [
        wu
        for wu in workunits
        if isinstance(getattr(wu.metadata, "aspect", None), LogicalParentClass)
    ]
    assert len(parent_wus) == 1
    wu = parent_wus[0]
    # The link lives on the composed physical URN and points at the logical one.
    assert "urn:li:dataPlatform:postgres,appdb.public.t," in wu.metadata.entityUrn
    assert "urn:li:dataPlatform:odcs" in wu.metadata.aspect.parent.destinationUrn
    # Physical datasets are not owned by ODCS: never primary-source.
    assert not wu.is_primary_source


def test_assertions_target_logical_and_emit_without_binding(
    tmp_path: pathlib.Path,
) -> None:
    """Assertions attach to the logical dataset and are emitted even when the
    schema entry has no physical binding."""
    body = _VALID_CONTRACT_BODY.replace("type: postgres", "type: kafka")
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(body, encoding="utf-8")
    src = _make_source(tmp_path, path=str(contract_file))
    workunits = list(src.get_workunits_internal())

    assert src.report.physical_bindings_resolved == 0
    assert not _aspects_of(workunits, LogicalParentClass)
    assertions = _aspects_of(workunits, AssertionInfoClass)
    # nullValues rule + the schema-compliance assertion.
    assert len(assertions) == 2
    for info in assertions:
        sub = info.fieldAssertion or info.schemaAssertion
        assert sub is not None
        assert "urn:li:dataPlatform:odcs" in sub.entity

    props = _aspects_of(workunits, DatasetPropertiesClass)
    assert props and props[0].customProperties["odcs.qualityRuleCount"] == "1"


def test_emit_flags(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    src = _make_source(
        tmp_path,
        path=str(contract_file),
        emit_logical_parent=False,
        emit_assertions=False,
        emit_schema_assertion=False,
    )
    workunits = list(src.get_workunits_internal())
    assert not _aspects_of(workunits, LogicalParentClass)
    assert not _aspects_of(workunits, AssertionInfoClass)


def test_platform_info_emitted_once_across_multiple_files(
    tmp_path: pathlib.Path,
) -> None:
    body2 = _VALID_CONTRACT_BODY.replace("id: test-contract-1", "id: test-contract-2")
    _write(tmp_path / "a.odcs.yaml", _VALID_CONTRACT_BODY)
    _write(tmp_path / "b.odcs.yaml", body2)
    src = _make_source(tmp_path, path=str(tmp_path))
    workunits = list(src.get_workunits_internal())
    assert len(_aspects_of(workunits, DataPlatformInfoClass)) == 1
    assert src.report.logical_datasets_emitted == 2


def test_duplicate_physical_binding_warns(tmp_path: pathlib.Path) -> None:
    body2 = _VALID_CONTRACT_BODY.replace("id: test-contract-1", "id: test-contract-2")
    _write(tmp_path / "a.odcs.yaml", _VALID_CONTRACT_BODY)
    _write(tmp_path / "b.odcs.yaml", body2)
    src = _make_source(tmp_path, path=str(tmp_path))
    list(src.get_workunits_internal())
    assert any(
        "Physical dataset bound by multiple" in str(getattr(w, "title", ""))
        for w in src.report.warnings
    )


def test_unmatched_override_key_warns(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    src = _make_source(
        tmp_path,
        path=str(contract_file),
        physical_urn_overrides={"test-contract-1": {"no_such_table": ""}},
    )
    list(src.get_workunits_internal())
    assert any(
        "physical_urn_overrides keys match no schema entry"
        in str(getattr(w, "title", ""))
        for w in src.report.warnings
    )


# ---------------------------------------------------------------------------
# Graph existence verification
# ---------------------------------------------------------------------------


def test_verification_missing_urn_unbinds_link_keeps_assertions(
    tmp_path: pathlib.Path,
) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    graph = MagicMock()
    graph.exists.return_value = False
    src = _make_source(tmp_path, graph=graph, path=str(contract_file))
    workunits = list(src.get_workunits_internal())

    assert not _aspects_of(workunits, LogicalParentClass)
    assert _aspects_of(workunits, AssertionInfoClass)
    assert src.report.physical_urns_unverified == 1
    assert src.report.physical_bindings_resolved == 0


def test_verification_existing_urn_links(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    graph = MagicMock()
    graph.exists.return_value = True
    src = _make_source(tmp_path, graph=graph, path=str(contract_file))
    workunits = list(src.get_workunits_internal())

    assert _aspects_of(workunits, LogicalParentClass)
    assert src.report.physical_urns_verified == 1


def test_verification_disabled_links_optimistically(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    graph = MagicMock()
    graph.exists.return_value = False
    src = _make_source(
        tmp_path,
        graph=graph,
        path=str(contract_file),
        verify_physical_urns_exist=False,
    )
    workunits = list(src.get_workunits_internal())
    assert _aspects_of(workunits, LogicalParentClass)
    graph.exists.assert_not_called()


def test_verification_fails_open_on_graph_error(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    graph = MagicMock()
    graph.exists.side_effect = RuntimeError("gms hiccup")
    src = _make_source(tmp_path, graph=graph, path=str(contract_file))
    workunits = list(src.get_workunits_internal())
    # Fail-open: the link is still emitted, with a warning.
    assert _aspects_of(workunits, LogicalParentClass)
    assert any(
        "Could not verify physical dataset existence" in str(getattr(w, "title", ""))
        for w in src.report.warnings
    )


def test_verification_without_graph_links(tmp_path: pathlib.Path) -> None:
    """File-sink runs have no graph: verification degrades to emitting."""
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    src = _make_source(tmp_path, path=str(contract_file))
    workunits = list(src.get_workunits_internal())
    assert _aspects_of(workunits, LogicalParentClass)


# ---------------------------------------------------------------------------
# Stateful ingestion wiring
# ---------------------------------------------------------------------------


def test_stale_removal_processor_wiring(tmp_path: pathlib.Path) -> None:
    src = _make_source(tmp_path)
    assert AutoStaleEntityRemovalProcessor in src.get_excluded_workunit_processors()
    processors = src.get_workunit_processors()
    manual = [
        p
        for p in processors
        if isinstance(p, partial) and p.func is auto_stale_entity_removal
    ]
    assert len(manual) == 1
    assert manual[0].args == (src.stale_entity_removal_handler,)


def test_owned_workunits_are_primary_but_links_are_not(
    tmp_path: pathlib.Path,
) -> None:
    """Stale removal must track logical datasets + assertions (primary) but
    never the logicalParent link on physical datasets or the platform aspect."""
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    src = _make_source(tmp_path, path=str(contract_file))
    for wu in src.get_workunits_internal():
        aspect = getattr(wu.metadata, "aspect", None)
        if isinstance(aspect, (LogicalParentClass, DataPlatformInfoClass)):
            assert not wu.is_primary_source
        elif isinstance(aspect, (AssertionInfoClass, DatasetPropertiesClass)):
            assert wu.is_primary_source
