"""Unit tests for the ODCS source — path resolution, validation, binding, stateful wiring."""

import pathlib
from functools import partial
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    EdgeClass,
    LogicalParentClass,
    OwnershipClass,
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


def _mcp(wu: Any) -> MetadataChangeProposalWrapper:
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    return wu.metadata


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


def test_validate_sorts_mixed_type_error_paths_without_crashing(
    tmp_path: pathlib.Path,
) -> None:
    """JSON Schema error paths mix str property names and int array indices.
    Sorting the raw deques compares str-vs-int and raises TypeError in Py3,
    which would demote actionable validation feedback to an opaque skip. The
    source sorts by the stringified path instead."""
    from collections import deque

    class _FakeErr:
        def __init__(self, path: list, message: str) -> None:
            self.absolute_path = deque(path)
            self.path = deque(path)
            self.message = message

    class _FakeValidator:
        def __init__(self, errors: list) -> None:
            self._errors = errors

        def iter_errors(self, raw_dict: dict) -> Any:
            return iter(self._errors)

    errors = [_FakeErr(["schema", 0], "a"), _FakeErr(["schema", "x"], "b")]
    # Document the underlying footgun the fix guards against.
    with pytest.raises(TypeError):
        sorted(errors, key=lambda e: e.path)

    src = _make_source(tmp_path)
    src._validators = {"3.1.0": _FakeValidator(errors)}  # type: ignore[dict-item]
    ok = src._validate({"apiVersion": "3.1.0", "id": "x"}, tmp_path / "f.yaml")
    assert ok  # lenient default proceeds after reporting
    assert src.report.validation_errors == 2


def test_validate_counts_skips_when_no_validator_available(
    tmp_path: pathlib.Path,
) -> None:
    """With no validators loaded, strict_validation is a silent no-op; the
    per-contract skip is counted so a zero-validation run is visible."""
    src = _make_source(tmp_path)
    src._validators = {}
    ok = src._validate({"apiVersion": "3.1.0", "id": "x"}, tmp_path / "f.yaml")
    assert ok
    assert src.report.contracts_validation_skipped == 1


def test_missing_schema_dir_warns_once(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A vendored-schema packaging/checkout regression must surface a warning,
    not silently disable validation for the whole run."""
    import datahub.ingestion.source.odcs.odcs_source as mod

    src = _make_source(tmp_path)
    monkeypatch.setattr(mod, "_SCHEMA_DIR", tmp_path / "does-not-exist")
    validators = src._load_validators()
    assert validators == {}
    assert any(
        "validators unavailable" in str(getattr(w, "title", "")).lower()
        for w in src.report.warnings
    )


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
    platform_aspect = _mcp(platform_wus[0]).aspect
    assert isinstance(platform_aspect, DataPlatformInfoClass)
    assert platform_aspect.name == "odcs"
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
    mcp = _mcp(wu)
    # The link lives on the composed physical URN and points at the logical one.
    assert mcp.entityUrn is not None
    assert "urn:li:dataPlatform:postgres,appdb.public.t," in mcp.entityUrn
    parent_aspect = mcp.aspect
    assert isinstance(parent_aspect, LogicalParentClass)
    assert parent_aspect.parent is not None
    assert "urn:li:dataPlatform:odcs" in parent_aspect.parent.destinationUrn
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
        "Could not verify URN existence" in str(getattr(w, "title", ""))
        for w in src.report.warnings
    )
    # A fail-open must not be tallied as genuinely verified.
    assert src.report.physical_urns_verify_failed == 1
    assert src.report.physical_urns_verified == 0


def _link_conflict_warnings(src: ODCSSource) -> List:
    return [
        w
        for w in src.report.warnings
        if "already linked to a different ODCS contract" in str(getattr(w, "title", ""))
    ]


def test_cross_run_conflict_warns_and_still_emits(tmp_path: pathlib.Path) -> None:
    """A logicalParent written by a PRIOR run for a different contract would be
    silently overwritten (logicalParent is single-valued and physical URNs are
    kept out of the checkpoint). The overwrite is now surfaced."""
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    graph = MagicMock()
    graph.exists.return_value = True
    other_logical = "urn:li:dataset:(urn:li:dataPlatform:odcs,other-contract.t,PROD)"
    graph.get_aspect.return_value = LogicalParentClass(
        parent=EdgeClass(destinationUrn=other_logical)
    )
    src = _make_source(tmp_path, graph=graph, path=str(contract_file))
    workunits = list(src.get_workunits_internal())

    # Last-writer-wins is preserved, but now visible.
    assert _aspects_of(workunits, LogicalParentClass)
    assert src.report.physical_urns_link_conflicts == 1
    assert len(_link_conflict_warnings(src)) == 1


def test_cross_run_same_contract_relink_is_silent(tmp_path: pathlib.Path) -> None:
    """Re-ingesting the same contract re-writes the same link — idempotent, no
    conflict warning."""
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    same_logical = "urn:li:dataset:(urn:li:dataPlatform:odcs,test-contract-1.t,PROD)"
    graph = MagicMock()
    graph.exists.return_value = True
    graph.get_aspect.return_value = LogicalParentClass(
        parent=EdgeClass(destinationUrn=same_logical)
    )
    src = _make_source(tmp_path, graph=graph, path=str(contract_file))
    list(src.get_workunits_internal())
    assert src.report.physical_urns_link_conflicts == 0
    assert not _link_conflict_warnings(src)


def test_no_prior_link_no_conflict(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    graph = MagicMock()
    graph.exists.return_value = True
    graph.get_aspect.return_value = None
    src = _make_source(tmp_path, graph=graph, path=str(contract_file))
    list(src.get_workunits_internal())
    assert src.report.physical_urns_link_conflicts == 0
    assert not _link_conflict_warnings(src)


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


def test_pipeline_blanket_urn_lowercasing_stays_disabled(
    tmp_path: pathlib.Path,
) -> None:
    """The pipeline-level AutoLowercaseUrnsProcessor duck-types on a source
    config attribute named `convert_urns_to_lowercase`. ODCS must never expose
    that name: blanket lowercasing would rewrite logical URNs (losing the
    contract's casing) and logicalParent targets AFTER existence verification
    ran against the original casing. Physical-name lowercasing is the
    targeted `lowercase_physical_urns` knob instead."""
    from datahub.ingestion.api.workunit_processor import WorkunitProcessorContext
    from datahub.ingestion.workunit_processors.auto_lowercase_urns import (
        AutoLowercaseUrnsProcessor,
    )

    src = _make_source(tmp_path)
    ctx = WorkunitProcessorContext(
        source_report=src.report,
        pipeline_context=src.ctx,
        source_config=src.config,
        platform="odcs",
    )
    assert not AutoLowercaseUrnsProcessor.should_enable(ctx)


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


def test_team_object_form_walker_and_owners(tmp_path: pathlib.Path) -> None:
    """The v3.1 canonical team OBJECT form: members recursed by the
    unknown-field walker, and unknown keys inside it still warn."""
    body = _VALID_CONTRACT_BODY.replace(
        "\nschema:\n",
        """
team:
  name: my-team
  members:
    - username: alice
      role: owner
      frobozz: 1
schema:
""",
    )
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(body, encoding="utf-8")
    src = _make_source(tmp_path, path=str(contract_file))
    workunits = list(src.get_workunits_internal())

    ownerships = _aspects_of(workunits, OwnershipClass)
    assert ownerships and ownerships[0].owners[0].owner == "urn:li:corpuser:alice"
    # The typo'd member field is still caught through the object form.
    assert any(
        "Unknown ODCS field" in str(getattr(w, "title", ""))
        for w in src.report.warnings
    )


# ---------------------------------------------------------------------------
# Owner identity normalization + report-only resolution check
# ---------------------------------------------------------------------------

_CONTRACT_WITH_TEAM = _VALID_CONTRACT_BODY.replace(
    "\nschema:\n",
    """
team:
  - username: alice
    role: owner
schema:
""",
)


def _owner_resolution_warnings(src: ODCSSource) -> List:
    return [
        w
        for w in src.report.warnings
        if "Contract owner not found" in str(getattr(w, "title", ""))
    ]


def test_owner_missing_in_graph_warns_but_still_emits(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_CONTRACT_WITH_TEAM, encoding="utf-8")
    graph = MagicMock()
    # Physical dataset resolves; the corpuser does not.
    graph.exists.side_effect = lambda urn: not urn.startswith("urn:li:corpuser:")
    src = _make_source(tmp_path, graph=graph, path=str(contract_file))
    workunits = list(src.get_workunits_internal())

    ownerships = _aspects_of(workunits, OwnershipClass)
    assert ownerships and ownerships[0].owners[0].owner == "urn:li:corpuser:alice"
    assert src.report.owners_unresolved == 1
    assert len(_owner_resolution_warnings(src)) == 1


def test_owner_exists_in_graph_no_warning(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_CONTRACT_WITH_TEAM, encoding="utf-8")
    graph = MagicMock()
    graph.exists.return_value = True
    src = _make_source(tmp_path, graph=graph, path=str(contract_file))
    workunits = list(src.get_workunits_internal())

    assert _aspects_of(workunits, OwnershipClass)
    assert src.report.owners_unresolved == 0
    assert not _owner_resolution_warnings(src)


def test_owner_check_skipped_without_graph(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_CONTRACT_WITH_TEAM, encoding="utf-8")
    src = _make_source(tmp_path, path=str(contract_file))
    workunits = list(src.get_workunits_internal())

    assert _aspects_of(workunits, OwnershipClass)
    assert src.report.owners_unresolved == 0
    assert not _owner_resolution_warnings(src)


def test_owner_normalization_knobs_reach_emitted_ownership(
    tmp_path: pathlib.Path,
) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_CONTRACT_WITH_TEAM, encoding="utf-8")
    src = _make_source(
        tmp_path, path=str(contract_file), owner_email_domain="@acme.example"
    )
    workunits = list(src.get_workunits_internal())
    ownerships = _aspects_of(workunits, OwnershipClass)
    assert ownerships[0].owners[0].owner == "urn:li:corpuser:alice@acme.example"


def test_unmapped_owner_role_is_recorded(tmp_path: pathlib.Path) -> None:
    """A named role the map does not know is silently coerced to
    TECHNICAL_OWNER; the source records it so the coercion is visible."""
    body = _VALID_CONTRACT_BODY.replace(
        "\nschema:\n",
        """
team:
  - username: alice
    role: producer
schema:
""",
    )
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(body, encoding="utf-8")
    src = _make_source(tmp_path, path=str(contract_file))
    list(src.get_workunits_internal())
    assert src.report.owners_role_defaulted == ["test-contract-1: producer"]


def test_owner_normalization_knobs_are_mutually_exclusive() -> None:
    with pytest.raises(ValidationError, match="mutually"):
        ODCSSourceConfig.model_validate(
            {
                "path": "/tmp/contracts",
                "strip_owner_email_domain": True,
                "owner_email_domain": "acme.example",
            }
        )
