"""Unit tests for the ODCS source class — path resolution, validation, soft-delete state."""

import json
import pathlib
from typing import Any, Dict, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.odcs.odcs_config import ODCSSourceConfig
from datahub.ingestion.source.odcs.odcs_source import ODCSSource
from datahub.metadata.schema_classes import StatusClass


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
# Validation modes (D5: default strict_validation=False)
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
    src = _make_source(tmp_path)  # strict_validation defaults to False per D5
    # Has the required `id` but uses the deprecated `rule:` (not `quality:`) at top level.
    raw_dict: dict = {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "x",
        "rule": [{"name": "deprecated_form"}],
    }
    ok = src._validate(raw_dict, tmp_path / "deprecated.yaml")
    assert ok  # proceed even though schema rejects the deprecated key
    # A warning is produced explaining the schema violation.
    assert len(src.report.warnings) >= 1


# ---------------------------------------------------------------------------
# D5 unknown-field detection
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
        "frobozz" in str(w.message)
        or "frobozz" in str(w.context)
        or "frobozz" in str(w.title)
        for w in src.report.warnings
    ) or any(
        "Unknown ODCS field" in str(getattr(w, "title", ""))
        for w in src.report.warnings
    )


# ---------------------------------------------------------------------------
# Soft-delete behavior (D2) — scoped to DataContract + Assertion only
# ---------------------------------------------------------------------------


def _list_workunits_status_removed(workunits: List) -> List[str]:
    """Return the entityUrns of workunits that emit Status(removed=True)."""
    out: List[str] = []
    for wu in workunits:
        # MetadataWorkUnit has metadata field which is the MCP
        try:
            mcp = wu.metadata
        except AttributeError:
            continue
        aspect = getattr(mcp, "aspect", None)
        if isinstance(aspect, StatusClass) and aspect.removed:
            out.append(mcp.entityUrn)
    return out


def test_soft_delete_first_run_persists_state_and_emits_no_deletes(
    tmp_path: pathlib.Path,
) -> None:
    contract_file = tmp_path / "contract.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    state_file = tmp_path / "state.json"

    src = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=[{"server": "prod-postgres", "platform": "postgres"}],
        state_file_path=str(state_file),
    )
    workunits = list(src.get_workunits_internal())

    # No soft-delete on first run.
    assert _list_workunits_status_removed(workunits) == []
    # State persisted with at least one DataContract and one Assertion URN.
    assert state_file.exists()
    saved = json.loads(state_file.read_text())
    assert saved
    # Find the entry by file key (resolved absolute path).
    entry = next(iter(saved.values()))
    assert any(u.startswith("urn:li:dataContract:") for u in entry["contracts"])
    assert any(u.startswith("urn:li:assertion:") for u in entry["assertions"])


def test_soft_delete_second_run_no_changes_emits_no_deletes(
    tmp_path: pathlib.Path,
) -> None:
    contract_file = tmp_path / "contract.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    state_file = tmp_path / "state.json"

    # First run.
    src1 = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=[{"server": "prod-postgres", "platform": "postgres"}],
        state_file_path=str(state_file),
    )
    list(src1.get_workunits_internal())

    # Second run with identical contract.
    src2 = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=[{"server": "prod-postgres", "platform": "postgres"}],
        state_file_path=str(state_file),
    )
    workunits = list(src2.get_workunits_internal())
    assert _list_workunits_status_removed(workunits) == []


def test_soft_delete_second_run_with_rule_removed_emits_assertion_delete(
    tmp_path: pathlib.Path,
) -> None:
    """Removing a rule between runs soft-deletes the Assertion URN ONLY,
    never the Dataset URN (D2)."""
    contract_file = tmp_path / "contract.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")
    state_file = tmp_path / "state.json"

    # First run captures one assertion.
    src1 = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=[{"server": "prod-postgres", "platform": "postgres"}],
        state_file_path=str(state_file),
    )
    list(src1.get_workunits_internal())

    saved = json.loads(state_file.read_text())
    first_entry = next(iter(saved.values()))
    first_assertions = list(first_entry["assertions"])
    first_contracts = list(first_entry["contracts"])
    assert len(first_assertions) == 1
    assert len(first_contracts) == 1

    # Rewrite contract with the rule removed (keep schema intact).
    body_no_rule = _VALID_CONTRACT_BODY.replace(
        "        quality:\n          - name: id_not_null\n            rule: notNull\n",
        "",
    )
    assert body_no_rule != _VALID_CONTRACT_BODY  # sanity
    contract_file.write_text(body_no_rule, encoding="utf-8")

    # Second run.
    src2 = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=[{"server": "prod-postgres", "platform": "postgres"}],
        state_file_path=str(state_file),
    )
    workunits = list(src2.get_workunits_internal())
    deleted_urns = _list_workunits_status_removed(workunits)

    # Exactly the dropped assertion is soft-deleted.
    assert deleted_urns == first_assertions
    # Critically: NO Dataset URN soft-delete, ever.
    for urn in deleted_urns:
        assert not urn.startswith("urn:li:dataset:")
    # And the DataContract URN is still emitted (re-asserted), not deleted.
    for urn in deleted_urns:
        assert urn not in first_contracts


def test_soft_delete_never_includes_dataset_urns_even_if_state_corrupted(
    tmp_path: pathlib.Path,
) -> None:
    """Even if a stale state file somehow contains a Dataset URN, the source
    must refuse to soft-delete it (D2 belt-and-suspenders)."""
    state_file = tmp_path / "state.json"
    fake_state = {
        str(tmp_path / "missing.yaml"): {
            "contracts": ["urn:li:dataContract:abc"],
            "assertions": ["urn:li:assertion:def"],
            # Pretend a buggy older version persisted a dataset URN here.
        }
    }
    # Inject a Dataset URN into the on-disk state to confirm the loader/scrubber
    # filters it out. We add it under a non-canonical key to simulate corruption.
    state_file.write_text(
        json.dumps(
            {
                **fake_state,
                "_corrupted_": {
                    "contracts": [
                        "urn:li:dataset:(urn:li:dataPlatform:postgres,a,PROD)"
                    ],
                    "assertions": [
                        "urn:li:dataset:(urn:li:dataPlatform:postgres,a,PROD)"
                    ],
                },
            }
        )
    )

    src = _make_source(
        tmp_path,
        path=str(tmp_path),
        state_file_path=str(state_file),
    )
    # No real files, so current_state is empty everywhere; loader filters out
    # dataset URNs entirely; soft-delete only emits the in-scope (assertion + contract) URNs
    # that fall out of scope.
    workunits = list(src.get_workunits_internal())
    deleted = _list_workunits_status_removed(workunits)
    for urn in deleted:
        assert urn.startswith("urn:li:dataContract:") or urn.startswith(
            "urn:li:assertion:"
        )
        assert not urn.startswith("urn:li:dataset:")


def test_soft_delete_with_no_state_file_is_noop(tmp_path: pathlib.Path) -> None:
    """state_file_path=None: no cross-run state, no deletes (per Phase 2A)."""
    contract_file = tmp_path / "contract.odcs.yaml"
    contract_file.write_text(_VALID_CONTRACT_BODY, encoding="utf-8")

    src = _make_source(
        tmp_path,
        path=str(contract_file),
        servers_to_platform=[{"server": "prod-postgres", "platform": "postgres"}],
        # state_file_path omitted -> None
    )
    workunits = list(src.get_workunits_internal())
    assert _list_workunits_status_removed(workunits) == []
