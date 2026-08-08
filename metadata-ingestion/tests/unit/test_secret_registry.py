"""Tests for the SecretRegistry — invariants 3 (scope halves), 5, 9, 16."""

import contextlib
import logging
from typing import Iterator, List

import pytest

from datahub.masking.constants import REDACTED_FORMAT
from datahub.masking.secret_registry import (
    _GLOBAL_GROUP,
    SecretRegistry,
    is_masking_enabled,
)


@contextlib.contextmanager
def _capture_records(logger_name: str) -> Iterator[List[logging.LogRecord]]:
    """Attach a handler for the duration of the block; remove the same
    handler on exit. Masking loggers have propagate=False, so caplog (root)
    can't see them."""
    log = logging.getLogger(logger_name)
    records: List[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Capture()
    log.addHandler(handler)
    try:
        yield records
    finally:
        log.removeHandler(handler)


class TestIsMaskingEnabled:
    def test_default_enabled(self):
        with pytest.MonkeyPatch.context() as m:
            m.delenv("DATAHUB_DISABLE_SECRET_MASKING", raising=False)
            assert is_masking_enabled()

    def test_disabled_true(self):
        with pytest.MonkeyPatch.context() as m:
            m.setenv("DATAHUB_DISABLE_SECRET_MASKING", "true")
            assert not is_masking_enabled()

    def test_disabled_one(self):
        with pytest.MonkeyPatch.context() as m:
            m.setenv("DATAHUB_DISABLE_SECRET_MASKING", "1")
            assert not is_masking_enabled()


class TestSingleton:
    def test_get_instance_returns_singleton(self):
        a = SecretRegistry.get_instance()
        b = SecretRegistry.get_instance()
        assert a is b

    def test_reset_instance_clears_singleton(self):
        a = SecretRegistry.get_instance()
        SecretRegistry.reset_instance()
        b = SecretRegistry.get_instance()
        assert a is not b


class TestRegistrationValidation:
    """Invariant 5: registration refuses marker-prefix values and short
    values on both single and batch paths, with warnings. The marker prefix
    is derived from constants.REDACTED_FORMAT, not hardcoded."""

    @property
    def _marker_prefix(self) -> str:
        return REDACTED_FORMAT.split("{", 1)[0]

    def test_single_marker_prefix_value_refused(self):
        reg = SecretRegistry()
        reg.clear()
        with _capture_records("datahub.masking.secret_registry") as recs:
            reg.register_secret("PW", self._marker_prefix + "PW***")
        assert reg.get_count() == 0
        assert any("will NOT be masked" in r.getMessage() for r in recs), (
            "marker-prefix value should warn that it will NOT be masked"
        )

    def test_batch_marker_prefix_value_refused(self):
        reg = SecretRegistry()
        reg.clear()
        with _capture_records("datahub.masking.secret_registry") as recs:
            reg.register_secrets_batch(
                {"PW": self._marker_prefix + "PW***", "OK": "valid_value_1"}
            )
        # OK accepted; marker-prefix value refused.
        assert reg.has_secret("OK")
        assert not reg.has_secret("PW")
        assert any("will NOT be masked" in r.getMessage() for r in recs)

    def test_single_short_value_refused_with_warning(self):
        reg = SecretRegistry()
        reg.clear()
        with _capture_records("datahub.masking.secret_registry") as recs:
            reg.register_secret("SHORT", "ab")
        assert reg.get_count() == 0
        assert any("minimum floor" in r.getMessage() for r in recs)

    def test_batch_short_value_refused_with_warning(self):
        reg = SecretRegistry()
        reg.clear()
        with _capture_records("datahub.masking.secret_registry") as recs:
            reg.register_secrets_batch({"SHORT": "ab", "OK": "valid_value_2"})
        assert reg.has_secret("OK")
        assert not reg.has_secret("SHORT")
        assert any("minimum floor" in r.getMessage() for r in recs)

    def test_non_string_and_empty_ignored(self):
        reg = SecretRegistry()
        reg.clear()
        reg.register_secret("N", None)  # type: ignore[arg-type]
        reg.register_secret("E", "")
        reg.register_secrets_batch({"N": None, "E": ""})  # type: ignore[arg-type,dict-item]
        assert reg.get_count() == 0

    def test_duplicate_value_uses_first_name(self):
        reg = SecretRegistry()
        reg.clear()
        reg.register_secret("A", "same_value_123")
        reg.register_secret("B", "same_value_123")
        snap_ver, snap = reg.snapshot()
        assert snap["same_value_123"] == "A"

    def test_url_encoded_variant_registered(self):
        reg = SecretRegistry()
        reg.clear()
        reg.register_secret("password", "P#!ss@word")
        _ver, snap = reg.snapshot()
        assert "P#!ss@word" in snap
        assert "P#!ss%40word" in snap


class TestPerExecutionScoping:
    """Invariant 3 (scope halves): ending one execution leaves the other's
    secrets masked and the filter installed; shutdown with no live scope is
    a safe no-op; ending by dead/unknown token does not recreate a group."""

    def test_overlapping_executions_isolated(self):
        reg = SecretRegistry()
        reg.clear()
        tok_a = reg.begin_execution()
        reg.register_secret("A", "aaa_secret_value")
        tok_b = reg.begin_execution()
        reg.register_secret("B", "bbb_secret_value")
        assert tok_a != tok_b

        reg.end_execution(tok_a)
        # B's secret is still registered; A's is gone.
        assert reg.has_secret("B")
        assert not reg.has_secret("A")
        assert reg.get_count() == 1

        reg.end_execution(tok_b)
        assert reg.get_count() == 0

    def test_shutdown_with_no_live_scope_is_safe_noop(self):
        reg = SecretRegistry()
        reg.clear()
        # No scope open; end_execution is a no-op (debug-level).
        reg.end_execution(None)
        reg.end_execution("nonexistent-token")
        # A dead/unknown id must NOT be recreated (no setdefault revival).
        assert "nonexistent-token" not in reg._groups

    def test_dead_token_not_recreated(self):
        reg = SecretRegistry()
        reg.clear()
        tok = reg.begin_execution()
        reg.register_secret("T", "token_secret_value")
        reg.end_execution(tok)
        # Ending again with the dead token does not revive the group.
        reg.end_execution(tok)
        assert tok not in reg._groups

    def test_global_dropped_when_last_execution_ends(self):
        reg = SecretRegistry()
        reg.clear()
        tok = reg.begin_execution()
        # Register a secret with no ambient scope on another "thread" by
        # clearing the contextvar first.
        import datahub.masking.secret_registry as reg_mod

        ctx_tok = reg_mod._current_exec.set(None)
        try:
            reg.register_secret("G", "global_secret_value")
            assert _GLOBAL_GROUP in reg._groups
            reg.end_execution(tok)
            # Last real execution ended -> __global__ dropped too.
            assert _GLOBAL_GROUP not in reg._groups
        finally:
            reg_mod._current_exec.reset(ctx_tok)

    def test_register_with_unknown_explicit_id_goes_to_global(self):
        reg = SecretRegistry()
        reg.clear()
        tok = reg.begin_execution()
        reg.register_secret("X", "value_xyz_123", exec_id="dead-id")
        # dead-id is not revived; the secret lands in __global__.
        assert "dead-id" not in reg._groups
        assert _GLOBAL_GROUP in reg._groups
        assert reg.has_secret("X")
        reg.end_execution(tok)

    def test_cross_thread_register_with_explicit_id(self):
        import threading

        reg = SecretRegistry()
        reg.clear()
        tok = reg.begin_execution()
        done = threading.Event()

        def worker():
            reg.register_secret("PW", "sup3rsecret_value", exec_id=tok)
            done.set()

        t = threading.Thread(target=worker)
        t.start()
        t.join(5.0)
        assert not t.is_alive()
        assert reg.has_secret("PW")
        assert "sup3rsecret_value" in reg._groups[tok]
        reg.end_execution(tok)

    def test_global_fallthrough_warns_once_per_execution(self):
        reg = SecretRegistry()
        reg.clear()
        tok = reg.begin_execution()
        import datahub.masking.secret_registry as reg_mod

        ctx_tok = reg_mod._current_exec.set(None)
        try:
            with _capture_records("datahub.masking.secret_registry") as recs:
                reg.register_secret("G1", "global_value_one")
                reg.register_secret("G2", "global_value_two")
            warnings = [
                r for r in recs if "outside any execution scope" in r.getMessage()
            ]
            assert len(warnings) == 1, (
                f"expected one fall-through warning, got {len(warnings)}"
            )
            reg.end_execution(tok)
        finally:
            reg_mod._current_exec.reset(ctx_tok)

    def test_empty_group_end_no_warning_while_other_live(self):
        # An execution that registered no secrets pops {} (falsy); it must
        # not trip the "scope may be leaking" warning while another scope is
        # live, and the group must actually be gone.
        reg = SecretRegistry()
        reg.clear()
        tok_live = reg.begin_execution()
        reg.register_secret("LIVE", "live_secret_value")
        tok_empty = reg.begin_execution()
        with _capture_records("datahub.masking.secret_registry") as recs:
            reg.end_execution(tok_empty)
        leak_warnings = [r for r in recs if "scope may be leaking" in r.getMessage()]
        assert leak_warnings == [], (
            f"empty-group end should not warn, got {leak_warnings}"
        )
        assert tok_empty not in reg._groups
        assert reg.has_secret("LIVE")
        reg.end_execution(tok_live)

    def test_noop_shutdown_does_not_bump_version(self):
        reg = SecretRegistry()
        reg.clear()
        v0 = reg.get_version()
        reg.end_execution(None)
        reg.end_execution("nonexistent-token")
        assert reg.get_version() == v0

    def test_noop_end_does_not_drop_global_with_secrets(self):
        # A completely no-op end call (unknown id, nothing removed) must
        # not drop __global__ nor bump the version, even if __global__ holds
        # secrets and no live scope exists.
        reg = SecretRegistry()
        reg.clear()
        import datahub.masking.secret_registry as reg_mod

        reg_mod._current_exec.set(None)
        reg.register_secret("G", "global_secret_value")
        assert _GLOBAL_GROUP in reg._groups
        v0 = reg.get_version()
        reg.end_execution("nonexistent-token")
        assert _GLOBAL_GROUP in reg._groups, "no-op end dropped __global__"
        assert reg.get_version() == v0, "no-op end bumped version"

    def test_unknown_exec_id_no_warning_without_live_scope(self):
        # Registering under an unknown explicit id with NO live scope must
        # not warn (the message says "while live scope(s) are active").
        reg = SecretRegistry()
        reg.clear()
        with _capture_records("datahub.masking.secret_registry") as recs:
            reg.register_secret("X", "value_xyz_123", exec_id="dead-id")
        fallthrough_warnings = [
            r for r in recs if "live scope(s) are active" in r.getMessage()
        ]
        assert fallthrough_warnings == [], (
            f"warned without live scope: {fallthrough_warnings}"
        )
        assert reg.has_secret("X")


class TestVersionAndSnapshot:
    """Invariant 9: registering N secrets triggers no pattern rebuild;
    the next mask_text call triggers exactly one snapshot+compile. The
    version bumps once per batch, and snapshot() is called only on version
    mismatch."""

    def test_version_increments_on_register(self):
        reg = SecretRegistry()
        reg.clear()
        v0 = reg.get_version()
        reg.register_secret("K", "value_one_123")
        assert reg.get_version() > v0

    def test_batch_bumps_version_once(self):
        reg = SecretRegistry()
        reg.clear()
        v0 = reg.get_version()
        reg.register_secrets_batch(
            {"A": "value_aaa_123", "B": "value_bbb_123", "C": "value_ccc_123"}
        )
        assert reg.get_version() == v0 + 1

    def test_snapshot_returns_expanded_keys(self):
        reg = SecretRegistry()
        reg.clear()
        reg.register_secret("PW", "pa:ss@wo/rd")
        ver, snap = reg.snapshot()
        assert ver == reg.get_version()
        # raw + repr (no change) + sqlalchemy-encoded + json-escaped
        assert "pa:ss@wo/rd" in snap
        assert "pa%3Ass%40wo%2Frd" in snap

    def test_snapshot_first_registration_wins_name(self):
        reg = SecretRegistry()
        reg.clear()
        tok = reg.begin_execution()
        reg.register_secret("A", "shared_value_123")
        reg.register_secret("B", "shared_value_123")
        _ver, snap = reg.snapshot()
        assert snap["shared_value_123"] == "A"
        reg.end_execution(tok)

    def test_register_then_mask_is_one_compile(self):
        """Invariant 9: N registrations followed by one mask_text trigger
        exactly one snapshot+compile. Asserted by counting snapshot() and
        re.compile calls, not wall-clock time."""
        from unittest.mock import patch

        reg = SecretRegistry()
        reg.clear()
        from datahub.masking.masking_filter import SecretMaskingFilter

        mf = SecretMaskingFilter(reg)
        snapshot_calls = [0]
        original_snapshot = reg.snapshot

        def counting_snapshot():
            snapshot_calls[0] += 1
            return original_snapshot()

        compile_calls = [0]
        original_compile = __import__("re").compile

        def counting_compile(pattern, flags=0):
            compile_calls[0] += 1
            return original_compile(pattern, flags)

        with (
            patch.object(reg, "snapshot", counting_snapshot),
            patch("re.compile", counting_compile),
        ):
            for i in range(20):
                reg.register_secret(f"K{i}", f"unique_value_{i:03d}_xx")
            # No snapshot/compile during registration.
            assert snapshot_calls[0] == 0
            assert compile_calls[0] == 0
            mf.mask_text("mask a unique_value_000_xx here")
            # Exactly one snapshot and one compile for the first mask after
            # N registrations.
            assert snapshot_calls[0] == 1
            assert compile_calls[0] == 1


class TestMaxSecrets:
    """Invariant 16: registration beyond MAX_SECRETS is refused with
    exactly one warning."""

    def test_register_beyond_max_refused_once(self):
        reg = SecretRegistry()
        reg.clear()
        with pytest.MonkeyPatch.context() as m:
            m.setattr(SecretRegistry, "MAX_SECRETS", 10)
            with _capture_records("datahub.masking.secret_registry") as recs:
                for i in range(30):
                    reg.register_secret(f"K{i}", f"value_{i:03d}_xxx")
        capacity_warnings = [r for r in recs if "at capacity" in r.getMessage()]
        assert len(capacity_warnings) == 1, (
            f"expected exactly one capacity warning, got {len(capacity_warnings)}"
        )
        assert reg.get_count() <= 10

    def test_batch_beyond_max_refused_once(self):
        reg = SecretRegistry()
        reg.clear()
        with pytest.MonkeyPatch.context() as m:
            m.setattr(SecretRegistry, "MAX_SECRETS", 10)
            with _capture_records("datahub.masking.secret_registry") as recs:
                reg.register_secrets_batch(
                    {f"K{i}": f"value_{i:03d}_xxx" for i in range(30)}
                )
        capacity_warnings = [r for r in recs if "at capacity" in r.getMessage()]
        assert len(capacity_warnings) == 1
        assert reg.get_count() <= 10

    def test_capacity_warning_can_fire_again_after_clear(self):
        reg = SecretRegistry()
        reg.clear()
        with pytest.MonkeyPatch.context() as m:
            m.setattr(SecretRegistry, "MAX_SECRETS", 10)
            with _capture_records("datahub.masking.secret_registry"):
                for i in range(30):
                    reg.register_secret(f"A{i}", f"value_{i:03d}_xxx")
        reg.clear()
        with pytest.MonkeyPatch.context() as m:
            m.setattr(SecretRegistry, "MAX_SECRETS", 10)
            with _capture_records("datahub.masking.secret_registry") as recs2:
                for i in range(30):
                    reg.register_secret(f"B{i}", f"other_{i:03d}_xxx")
        assert len([r for r in recs2 if "at capacity" in r.getMessage()]) == 1

    def test_capacity_warning_resets_per_execution(self):
        # begin_execution resets _capacity_warned, so a worker that hits
        # MAX_SECRETS warns once per execution, not once per process.
        reg = SecretRegistry()
        reg.clear()
        with pytest.MonkeyPatch.context() as m:
            m.setattr(SecretRegistry, "MAX_SECRETS", 10)
            with _capture_records("datahub.masking.secret_registry") as recs1:
                tok = reg.begin_execution()
                for i in range(30):
                    reg.register_secret(f"A{i}", f"value_{i:03d}_xxx")
            assert len([r for r in recs1 if "at capacity" in r.getMessage()]) == 1
            reg.end_execution(tok)
            with _capture_records("datahub.masking.secret_registry") as recs2:
                tok2 = reg.begin_execution()
                for i in range(30):
                    reg.register_secret(f"B{i}", f"other_{i:03d}_xxx")
            assert len([r for r in recs2 if "at capacity" in r.getMessage()]) == 1
            reg.end_execution(tok2)


class TestClear:
    def test_clear_removes_all(self):
        reg = SecretRegistry()
        reg.clear()
        reg.register_secret("A", "value_aaa_123")
        reg.register_secret("B", "value_bbb_123")
        assert reg.get_count() == 2
        reg.clear()
        assert reg.get_count() == 0

    def test_clear_increments_version(self):
        reg = SecretRegistry()
        reg.clear()
        v0 = reg.get_version()
        reg.register_secret("A", "value_aaa_123")
        reg.clear()
        assert reg.get_version() > v0


class TestGetSecretValue:
    def test_first_wins_across_scopes(self):
        reg = SecretRegistry()
        reg.clear()
        tok = reg.begin_execution()
        reg.register_secret("PW", "first_value_123")
        tok2 = reg.begin_execution()
        reg.register_secret("PW", "second_value_456")
        # first-wins across scopes
        assert reg.get_secret_value("PW") == "first_value_123"
        reg.end_execution(tok)
        reg.end_execution(tok2)

    def test_not_found_returns_none(self):
        reg = SecretRegistry()
        reg.clear()
        assert reg.get_secret_value("NOPE") is None
        assert not reg.has_secret("NOPE")
