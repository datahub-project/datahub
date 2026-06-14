"""Unit tests for the ODCS source — path resolution, validation, strict gating, soft-delete."""

import json
import pathlib
from typing import Any, Dict, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.odcs.odcs_config import ODCSSourceConfig
from datahub.ingestion.source.odcs.odcs_source import ODCSSource
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    DataPlatformInfoClass,
    LogicalParentClass,
    StatusClass,
)


def _make_source(tmp_path: pathlib.Path, **overrides: Any) -> ODCSSource:
    config_dict: Dict[str, Any] = {
        "path": str(tmp_path),
        "servers_to_platform": [],
    }
    config_dict.update(overrides)
    config = ODCSSourceConfig.model_validate(config_dict)
    return ODCSSource(config=config, ctx=PipelineContext(run_id="test"))


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
schema:
  - name: t
    physicalName: t
    properties:
      - name: id
        logicalType: number
        quality:
          - name: id_not_null
            rule: notNull
"""

_POSTGRES_MAPPING = [{"server": "prod-postgres", "platform": "postgres"}]


def _aspects_of(workunits: List, aspect_cls: type) -> List:
    out = []
    for wu in workunits:
        aspect = getattr(getattr(wu, "metadata", None), "aspect", None)
        if isinstance(aspect, aspect_cls):
            out.append(aspect)
    return out


def _status_removed_urns(workunits: List) -> List[str]:
    out: List[str] = []
    for wu in workunits:
        mcp = getattr(wu, "metadata", None)
        aspect = getattr(mcp, "aspect", None)
        if mcp is not None and isinstance(aspect, StatusClass) and aspect.removed:
            out.append(mcp.entityUrn)
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


def test_validate_lenient_default_proceeds_on_deprecated_form(
    tmp_path: pathlib.Path,
) -> None:
    """Default `strict_validation=False` lets a real-but-deprecated ODCS doc through."""
    src = _make_source(tmp_path)
    raw_dict: dict = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "x",
        "rule": [{"name": "deprecated_form"}],
    }
    ok = src._validate(raw_dict, tmp_path / "deprecated.yaml")
    assert ok
    assert len(src.report.warnings) >= 1


# ---------------------------------------------------------------------------
# Unknown-field detection
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


# ---------------------------------------------------------------------------
# Platform registration + strict gating (the core invariants of the pivot)
# ---------------------------------------------------------------------------


def test_platform_info_emitted_once(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    src = _make_source(
        tmp_path, path=str(contract_file), servers_to_platform=_POSTGRES_MAPPING
    )
    workunits = list(src.get_workunits_internal())
    platform_infos = _aspects_of(workunits, DataPlatformInfoClass)
    assert len(platform_infos) == 1
    assert platform_infos[0].name == "odcs"


def test_binding_emits_logical_parent_and_assertions(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    src = _make_source(
        tmp_path, path=str(contract_file), servers_to_platform=_POSTGRES_MAPPING
    )
    workunits = list(src.get_workunits_internal())

    parents = _aspects_of(workunits, LogicalParentClass)
    assert len(parents) == 1
    # logicalParent points at the logical odcs dataset.
    assert "urn:li:dataPlatform:odcs" in parents[0].parent.destinationUrn

    assertions = _aspects_of(workunits, AssertionInfoClass)
    assert len(assertions) == 1
    # The assertion targets the physical postgres dataset, not the logical one.
    assert "urn:li:dataPlatform:postgres" in assertions[0].fieldAssertion.entity


def test_strict_gating_no_binding_emits_logical_only(tmp_path: pathlib.Path) -> None:
    """The key new invariant: with no physical binding, the logical dataset is
    still emitted but produces zero assertions and no logicalParent link."""
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    # No servers_to_platform mapping -> no physical binding resolves.
    src = _make_source(tmp_path, path=str(contract_file), servers_to_platform=[])
    workunits = list(src.get_workunits_internal())

    assert src.report.logical_datasets_emitted == 1
    assert src.report.physical_bindings_resolved == 0
    assert _aspects_of(workunits, LogicalParentClass) == []
    assert _aspects_of(workunits, AssertionInfoClass) == []
    # Provenance is still kept on the logical dataset via qualityRuleCount.
    from datahub.metadata.schema_classes import DatasetPropertiesClass

    props = _aspects_of(workunits, DatasetPropertiesClass)
    assert props and props[0].customProperties["odcs.qualityRuleCount"] == "1"


def test_emit_logical_parent_false_skips_physical_aspect(
    tmp_path: pathlib.Path,
) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    src = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=_POSTGRES_MAPPING,
        emit_logical_parent=False,
    )
    workunits = list(src.get_workunits_internal())
    assert _aspects_of(workunits, LogicalParentClass) == []
    # Assertions still emit (binding resolved).
    assert _aspects_of(workunits, AssertionInfoClass)


def test_emit_assertions_false_keeps_logical_parent_drops_assertions(
    tmp_path: pathlib.Path,
) -> None:
    contract_file = tmp_path / "c.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    src = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=_POSTGRES_MAPPING,
        emit_assertions=False,
    )
    workunits = list(src.get_workunits_internal())
    assert _aspects_of(workunits, AssertionInfoClass) == []
    # logicalParent still emitted (binding resolved, flag independent).
    assert _aspects_of(workunits, LogicalParentClass)


def test_platform_info_emitted_once_across_multiple_files(
    tmp_path: pathlib.Path,
) -> None:
    body2 = _VALID_CONTRACT_BODY.replace("id: test-contract-1", "id: test-contract-2")
    _write(tmp_path / "a.odcs.yaml", _VALID_CONTRACT_BODY)
    _write(tmp_path / "b.odcs.yaml", body2)
    src = _make_source(
        tmp_path, path=str(tmp_path), servers_to_platform=_POSTGRES_MAPPING
    )
    workunits = list(src.get_workunits_internal())
    assert len(_aspects_of(workunits, DataPlatformInfoClass)) == 1
    # Both contracts were processed.
    assert src.report.logical_datasets_emitted == 2


# ---------------------------------------------------------------------------
# Soft-delete behavior — scoped to logical odcs Dataset + Assertion only
# ---------------------------------------------------------------------------


def test_soft_delete_first_run_persists_state_no_deletes(
    tmp_path: pathlib.Path,
) -> None:
    contract_file = tmp_path / "contract.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    state_file = tmp_path / "state.json"

    src = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=_POSTGRES_MAPPING,
        state_file_path=str(state_file),
    )
    workunits = list(src.get_workunits_internal())

    assert _status_removed_urns(workunits) == []
    saved = json.loads(state_file.read_text())
    entry = next(iter(saved.values()))
    assert any(
        u.startswith("urn:li:dataset:(urn:li:dataPlatform:odcs,")
        for u in entry["datasets"]
    )
    assert any(u.startswith("urn:li:assertion:") for u in entry["assertions"])


def test_soft_delete_second_run_no_changes_no_deletes(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "contract.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    state_file = tmp_path / "state.json"

    for _ in range(2):
        src = _make_source(
            tmp_path,
            path=str(contract_file),
            servers_to_platform=_POSTGRES_MAPPING,
            state_file_path=str(state_file),
        )
        workunits = list(src.get_workunits_internal())
    assert _status_removed_urns(workunits) == []


def test_soft_delete_removed_rule_deletes_assertion_not_physical(
    tmp_path: pathlib.Path,
) -> None:
    """Removing a rule between runs soft-deletes the Assertion URN only — never
    a physical dataset (ODCS owns assertions and logical datasets, not physical)."""
    contract_file = tmp_path / "contract.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    state_file = tmp_path / "state.json"

    src1 = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=_POSTGRES_MAPPING,
        state_file_path=str(state_file),
    )
    list(src1.get_workunits_internal())

    saved = json.loads(state_file.read_text())
    first_entry = next(iter(saved.values()))
    first_assertions = list(first_entry["assertions"])
    assert len(first_assertions) == 1

    body_no_rule = _VALID_CONTRACT_BODY.replace(
        "        quality:\n          - name: id_not_null\n            rule: notNull\n",
        "",
    )
    assert body_no_rule != _VALID_CONTRACT_BODY
    contract_file.write_text(body_no_rule, encoding="utf-8")

    src2 = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=_POSTGRES_MAPPING,
        state_file_path=str(state_file),
    )
    workunits = list(src2.get_workunits_internal())
    deleted = _status_removed_urns(workunits)

    assert deleted == first_assertions
    for urn in deleted:
        assert not urn.startswith("urn:li:dataset:(urn:li:dataPlatform:postgres")


def test_soft_delete_loader_filters_physical_dataset_urns(
    tmp_path: pathlib.Path,
) -> None:
    """A stale state file containing a physical Dataset URN must never produce a
    physical-dataset soft-delete (belt-and-suspenders)."""
    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps(
            {
                str(tmp_path / "missing.yaml"): {
                    "datasets": [
                        "urn:li:dataset:(urn:li:dataPlatform:odcs,c.t,PROD)",
                        # Buggy older state leaked a physical URN — must be dropped.
                        "urn:li:dataset:(urn:li:dataPlatform:postgres,a,PROD)",
                    ],
                    "assertions": ["urn:li:assertion:def"],
                }
            }
        )
    )

    src = _make_source(tmp_path, path=str(tmp_path), state_file_path=str(state_file))
    workunits = list(src.get_workunits_internal())
    deleted = _status_removed_urns(workunits)
    for urn in deleted:
        assert not urn.startswith("urn:li:dataset:(urn:li:dataPlatform:postgres")
    # The logical odcs dataset and the assertion fall out and are deleted.
    assert "urn:li:dataset:(urn:li:dataPlatform:odcs,c.t,PROD)" in deleted
    assert "urn:li:assertion:def" in deleted


def test_soft_delete_no_state_file_is_noop(tmp_path: pathlib.Path) -> None:
    contract_file = tmp_path / "contract.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")

    src = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=_POSTGRES_MAPPING,
    )
    workunits = list(src.get_workunits_internal())
    assert _status_removed_urns(workunits) == []
