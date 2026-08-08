"""Tests for the SecretMaskingFilter — invariants 4, 6, 7 (happy half), 8,
14, 15."""

import logging
import re
import sys
from io import StringIO
from unittest.mock import patch

import pytest

from datahub.masking.masking_filter import (
    MASKING_ERROR_MESSAGE,
    SecretMaskingFilter,
    StreamMaskingWrapper,
)
from datahub.masking.secret_registry import SecretRegistry


@pytest.fixture
def registry():
    reg = SecretRegistry()
    reg.clear()
    return reg


@pytest.fixture
def masking_filter(registry):
    return SecretMaskingFilter(registry)


class TestBasicMasking:
    def test_message_masked(self, registry, masking_filter):
        registry.register_secret("PW", "secret123")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Password is secret123",
            args=(),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert "secret123" not in record.msg
        assert "***REDACTED:PW***" in record.msg

    def test_multiple_secrets(self, registry, masking_filter):
        registry.register_secret("PASSWORD", "pass123")
        registry.register_secret("TOKEN", "tok456")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Password: pass123, Token: tok456",
            args=(),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert "pass123" not in record.msg
        assert "tok456" not in record.msg
        assert "***REDACTED:PASSWORD***" in record.msg
        assert "***REDACTED:TOKEN***" in record.msg

    def test_no_secrets_returns_unchanged(self, masking_filter):
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="normal message",
            args=(),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert record.msg == "normal message"


class TestMultiHandlerSentinelAndIdempotency:
    """Invariant 4: a record dispatched to multiple handlers is masked and
    truncated exactly once (sentinel), and mask_text is idempotent on
    already-masked text and actually transforms every registered non-marker
    value."""

    def test_record_masked_once_across_handlers(self, registry, masking_filter):
        registry.register_secret("PW", "multi_secret_value")
        cap1 = StringIO()
        cap2 = StringIO()
        h1 = logging.StreamHandler(cap1)
        h2 = logging.StreamHandler(cap2)
        for h in (h1, h2):
            h.setFormatter(logging.Formatter("%(message)s"))
            h.addFilter(masking_filter)
        log = logging.getLogger("test.multi_handler")
        log.handlers.clear()
        log.propagate = False
        log.addHandler(h1)
        log.addHandler(h2)
        log.setLevel(logging.INFO)
        # Longer than the 5000-char budget so truncation actually fires.
        long_msg = "leak multi_secret_value here" + "x" * 6000
        try:
            log.info(long_msg)
        finally:
            log.removeHandler(h1)
            log.removeHandler(h2)
        out1 = cap1.getvalue()
        out2 = cap2.getvalue()
        assert "multi_secret_value" not in out1
        assert "multi_secret_value" not in out2
        assert "***REDACTED:PW***" in out1
        assert "***REDACTED:PW***" in out2
        # Truncated exactly once per handler (sentinel), not twice.
        assert out1.count("bytes truncated") == 1, out1
        assert out2.count("bytes truncated") == 1, out2

    def test_mask_text_idempotent_and_transforms(self, registry, masking_filter):
        registry.register_secret("PW", "idempotent_secret_value")
        text = "leak idempotent_secret_value here"
        once = masking_filter.mask_text(text)
        twice = masking_filter.mask_text(once)
        assert once == twice, "mask_text is not idempotent"
        assert once != text, "mask_text did not transform the input"
        assert "idempotent_secret_value" not in once
        assert "***REDACTED:PW***" in once

    def test_mask_text_marker_shaped_value_idempotent(self, registry, masking_filter):
        registry.register_secret("ORPHAN", "***REDACTED:GONE***")
        text = "***REDACTED:GONE***"
        once = masking_filter.mask_text(text)
        twice = masking_filter.mask_text(once)
        assert once == twice
        assert "REDACTED" in once


class TestArgsNestedContainers:
    """Invariant 6: args containing nested containers are masked
    recursively (not just top-level strings)."""

    def test_args_nested_dict_masked(self, registry, masking_filter):
        registry.register_secret("PW", "args_secret_value")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="cfg: %s",
            args=({"a": {"password": "args_secret_value"}},),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert "args_secret_value" not in str(record.args)
        assert "***REDACTED:PW***" in str(record.args)

    def test_args_list_of_dicts_masked(self, registry, masking_filter):
        registry.register_secret("PW", "list_secret_value")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="items: %s",
            args=([{"password": "list_secret_value"}],),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert "list_secret_value" not in str(record.args)
        assert "***REDACTED:PW***" in str(record.args)


class TestTracebackHappyHalf:
    """Invariant 7 (happy half): exc_text is masked; exc_info retains the
    original exception class, args, and __cause__ chain."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_exc_text_masked_exc_info_retained(self):
        reg = SecretRegistry.get_instance()
        reg.clear()
        mf = SecretMaskingFilter(reg)
        reg.register_secret("PW", "traceback_secret_value")
        try:
            raise ValueError("embed traceback_secret_value in message")
        except ValueError:
            exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="error occurred",
            args=(),
            exc_info=exc_info,
        )
        mf.filter(record)
        assert record.exc_text is not None
        assert "traceback_secret_value" not in record.exc_text
        assert "***REDACTED:PW***" in record.exc_text
        assert record.exc_info is exc_info
        exc_type, exc_value, _ = record.exc_info
        assert exc_type is ValueError
        assert "traceback_secret_value" in str(exc_value.args[0])

    def test_exc_info_cause_chain_preserved(self):
        reg = SecretRegistry.get_instance()
        reg.clear()
        mf = SecretMaskingFilter(reg)
        reg.register_secret("PW", "cause_secret_value")
        try:
            try:
                raise ValueError("embed cause_secret_value")
            except ValueError:
                cause = RuntimeError("cause_secret_value too")
                raise RuntimeError("wrapper") from cause
        except RuntimeError:
            exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="error",
            args=(),
            exc_info=exc_info,
        )
        mf.filter(record)
        assert record.exc_info is exc_info
        _t, v, _tb = record.exc_info
        cause = v.__cause__
        assert cause is not None
        assert "cause_secret_value" in str(cause.args[0])


class TestStreamWrapper:
    """Invariant 8: print() of a secret through the wrapped stdout is
    masked; if masking raises inside the wrapper, the output is
    MASKING_ERROR_MESSAGE, never the raw text."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_print_secret_masked(self, registry, masking_filter):
        registry.register_secret("PW", "print_secret_value")
        out = StringIO()
        wrapper = StreamMaskingWrapper(out, masking_filter)
        wrapper.write("leak print_secret_value here")
        result = out.getvalue()
        assert "print_secret_value" not in result
        assert "***REDACTED:PW***" in result

    def test_wrapper_failure_writes_masking_error(self, registry, masking_filter):
        registry.register_secret("PW", "wrapper_secret_value")
        out = StringIO()
        wrapper = StreamMaskingWrapper(out, masking_filter)
        with patch.object(
            masking_filter, "mask_text", side_effect=RuntimeError("mask boom")
        ):
            n = wrapper.write("leak wrapper_secret_value here")
        written = out.getvalue()
        assert "wrapper_secret_value" not in written
        assert MASKING_ERROR_MESSAGE in written
        assert n == len(MASKING_ERROR_MESSAGE) + 1


class TestMaskingNamespaceBypass:
    """Invariant 14: a record from a masking-safe-namespace logger
    (datahub.masking.*) reaching a filter-carrying handler bypasses masking.
    Asserted directly against a handler that carries the filter, so a
    regression in the record.name early-return is actually caught."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_masking_namespace_record_bypasses(self, registry, masking_filter):
        registry.register_secret("PW", "namespace_secret_value")
        cap = StringIO()
        h = logging.StreamHandler(cap)
        h.setFormatter(logging.Formatter("%(message)s"))
        h.addFilter(masking_filter)
        masking_logger = logging.getLogger("datahub.masking.test_internal")
        masking_logger.handlers.clear()
        masking_logger.propagate = False
        masking_logger.addHandler(h)
        masking_logger.setLevel(logging.INFO)
        try:
            masking_logger.info("leak namespace_secret_value on purpose")
        finally:
            masking_logger.removeHandler(h)
        out = cap.getvalue()
        assert "namespace_secret_value" in out
        assert "***REDACTED:PW***" not in out

    def test_exact_namespace_logger_bypassed(self, registry, masking_filter):
        registry.register_secret("PW", "exact_ns_secret_value")
        record = logging.LogRecord(
            name="datahub.masking",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="leak exact_ns_secret_value",
            args=(),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert "exact_ns_secret_value" in record.msg

    def test_trailing_dot_does_not_match_maskingfoo(self, registry, masking_filter):
        registry.register_secret("PW", "foo_secret_value")
        record = logging.LogRecord(
            name="datahub.maskingfoo",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="leak foo_secret_value",
            args=(),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert "foo_secret_value" not in record.msg
        assert "***REDACTED:PW***" in record.msg


class TestSecretStraddlingBoundary:
    """Invariant 15: a secret straddling the max_message_size boundary is
    never severed by truncation — the two-stage pre-cut either masks it
    whole or cuts before it."""

    def test_secret_at_boundary_not_severed(self):
        reg = SecretRegistry()
        reg.clear()
        mf = SecretMaskingFilter(reg, max_message_size=100)
        secret = "boundary_secret_value_12345"  # 26 chars
        reg.register_secret("PW", secret)
        text = "x" * 95 + secret + "y" * 50
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=text,
            args=(),
            exc_info=None,
        )
        mf.filter(record)
        out = record.msg
        assert secret not in out, f"secret was severed by truncation: {out!r}"
        assert secret[:16] not in out, f"leading fragment survived: {out!r}"

    def test_secret_inside_budget_appears_as_marker(self):
        reg = SecretRegistry()
        reg.clear()
        mf = SecretMaskingFilter(reg, max_message_size=200)
        secret = "inside_secret_value_12345"
        reg.register_secret("PW", secret)
        text = "prefix " + secret + " suffix"
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=text,
            args=(),
            exc_info=None,
        )
        mf.filter(record)
        assert "***REDACTED:PW***" in record.msg

    def test_slide_regression_msg_path(self):
        # Two copies of a long secret; masking the first shrinks the text
        # below budget and slides the second copy's severed fragment inside
        # the retained window. A mandatory final cut alone leaves it; the
        # strip is what removes it.
        reg = SecretRegistry()
        reg.clear()
        budget = 200
        mf = SecretMaskingFilter(reg, max_message_size=budget)
        secret = "S" * 300
        reg.register_secret("PW", secret)
        text = secret + "x" * 10 + secret + "y" * (budget + 50)
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=text,
            args=(),
            exc_info=None,
        )
        mf.filter(record)
        assert secret[:16] not in record.msg, (
            f"severed fragment survived in msg: {record.msg!r}"
        )
        assert "truncated" in record.msg

    def test_slide_regression_exc_text_path(self):
        # Second copy of the secret straddles the exc_text pre-cut: with
        # max_message_size=200 the exc_text budget is 2*200=400, longest=300,
        # so the pre-cut is 700. The second copy (~517-817) straddles 700; the
        # pre-cut severs it, masking slides the fragment inside the window,
        # and the post-mask strip removes it.
        reg = SecretRegistry()
        reg.clear()
        mf = SecretMaskingFilter(reg, max_message_size=200)
        secret = "S" * 300
        reg.register_secret("PW", secret)
        tb = (
            "Traceback (most recent call last):\n  File ...\n    raise ValueError(\n"
            + secret
            + "p" * 150
            + secret
            + "q" * 300
            + "\n)"
        )
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="error",
            args=(),
            exc_info=None,
        )
        record.exc_text = tb
        mf.filter(record)
        assert secret[:16] not in (record.exc_text or ""), (
            f"severed fragment survived in exc_text: {record.exc_text!r}"
        )
        assert "truncated" in (record.exc_text or "")

    def test_slide_regression_exc_text_strip_is_load_bearing(self):
        # Same geometry, but with _strip_severed_tail neutralized: the
        # severed fragment of the second copy MUST survive, proving the
        # post-mask strip is what removes it (not the final cut alone).
        reg = SecretRegistry()
        reg.clear()
        mf = SecretMaskingFilter(reg, max_message_size=200)
        secret = "S" * 300
        reg.register_secret("PW", secret)
        tb = (
            "Traceback (most recent call last):\n  File ...\n    raise ValueError(\n"
            + secret
            + "p" * 150
            + secret
            + "q" * 300
            + "\n)"
        )
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="error",
            args=(),
            exc_info=None,
        )
        record.exc_text = tb
        with patch.object(
            mf, "_strip_severed_tail", side_effect=lambda kept, keys, longest: kept
        ):
            mf.filter(record)
        assert secret[:16] in (record.exc_text or ""), (
            f"fragment did not survive without the strip: {record.exc_text!r}"
        )

    def test_periodic_secret_strip_after_mask_no_leak(self):
        # A periodic secret severed by the pre-cut: stripping before masking
        # can match a coincidental self-overlap longer than the real fragment
        # and sever a complete earlier occurrence instead. Masking first
        # turns every complete occurrence into a marker, so a tail matching a
        # key prefix can only be a severed fragment.
        reg = SecretRegistry()
        reg.clear()
        budget = 200
        mf = SecretMaskingFilter(reg, max_message_size=budget)
        secret = "s3cr3t" * 50  # period 6, 300 chars
        reg.register_secret("P", secret)
        text = secret + secret
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=text,
            args=(),
            exc_info=None,
        )
        mf.filter(record)
        out = record.msg
        assert "***REDACTED:P***" in out, f"marker missing: {out!r}"
        # No 8-char substring of the secret survives (period 6 regenerates the
        # whole value from any 6+ char fragment).
        for i in range(len(secret) - 8 + 1):
            assert secret[i : i + 8] not in out, (
                f"8-char secret fragment at {i} survived: {out!r}"
            )


class TestExtrasNotMutated:
    """Extras: nested containers masked recursively on a copy (caller's dict
    unmutated)."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_extras_not_mutated_in_place(self, registry, masking_filter):
        registry.register_secret("PW", "extras_secret_value")
        caller_dict = {"a": {"password": "extras_secret_value"}, "b": "plain"}
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="msg",
            args=(),
            exc_info=None,
        )
        record.__dict__["cfg"] = caller_dict
        masking_filter.filter(record)
        assert caller_dict == {"a": {"password": "extras_secret_value"}, "b": "plain"}
        masked_cfg = record.__dict__["cfg"]
        assert "extras_secret_value" not in str(masked_cfg)
        assert "***REDACTED:PW***" in str(masked_cfg)


class TestLateRegistrationMaskedOnNextCall:
    # Invariant 2: a secret registered after the first mask_text is masked
    # on the next call (version bump triggers a rebuild).

    def test_secret_added_after_first_mask_is_masked_next_call(
        self, registry, masking_filter
    ):
        registry.register_secret("FIRST", "first_secret_value")
        out1 = masking_filter.mask_text("leak first_secret_value here")
        assert "first_secret_value" not in out1
        assert "***REDACTED:FIRST***" in out1

        registry.register_secret("LATE", "late_secret_value")
        out2 = masking_filter.mask_text("leak late_secret_value here")
        assert "late_secret_value" not in out2
        assert "***REDACTED:LATE***" in out2


class TestNamedtupleReconstruction:
    """A multi-field namedtuple in extra= is reconstructed as the same
    namedtuple type with the secret field masked (not collapsed to a tuple
    or list)."""

    def test_two_field_namedtuple_preserved(self, registry, masking_filter):
        from collections import namedtuple

        Pair = namedtuple("Pair", ["user", "secret"])
        registry.register_secret("PW", "nt_secret_value")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="msg",
            args=(),
            exc_info=None,
        )
        record.__dict__["cfg"] = Pair(user="alice", secret="nt_secret_value")
        masking_filter.filter(record)
        out = record.__dict__["cfg"]
        assert isinstance(out, Pair)
        assert out.user == "alice"
        assert "nt_secret_value" not in str(out)
        assert "***REDACTED:PW***" in str(out)


class TestNonStringMsg:
    """Non-str record.msg is masked (dict/list/tuple/set via recursive
    helper; arbitrary objects str()-ed and replaced only if changed)."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_dict_msg_masked(self, registry, masking_filter):
        registry.register_secret("PW", "dict_msg_secret_value")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg={"password": "dict_msg_secret_value"},
            args=(),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert "dict_msg_secret_value" not in str(record.msg)
        assert "***REDACTED:PW***" in str(record.msg)

    def test_object_msg_masked_only_when_secret_present(self, registry, masking_filter):
        registry.register_secret("PW", "obj_secret_value")

        class Obj:
            def __str__(self):
                return "embed obj_secret_value here"

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=Obj(),
            args=(),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert "obj_secret_value" not in str(record.msg)
        assert "***REDACTED:PW***" in str(record.msg)

    def test_object_msg_preserved_when_no_secret(self, masking_filter):
        class Obj:
            def __str__(self):
                return "no secret here"

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=Obj(),
            args=(),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert isinstance(record.msg, Obj)


class TestFormatStringArgsSkipTruncation:
    """Truncation is skipped when record.args is present (truncating a
    format string can sever a %s placeholder)."""

    def test_format_string_not_truncated_with_args(self, registry, masking_filter):
        registry.register_secret("PW", "format_secret_value")
        fmt = "x" * 6000 + " %s"
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=fmt,
            args=("format_secret_value",),
            exc_info=None,
        )
        masking_filter.filter(record)
        assert "%s" in record.msg
        assert "format_secret_value" not in str(record.args)


class TestRebuildFailureFailClosed:
    """When secrets exist but no pattern was ever built and the rebuild
    fails, mask_text returns MASKING_ERROR_MESSAGE (never the raw text).
    When a previous pattern exists, it is kept."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_first_build_failure_returns_masking_error(self, registry):
        registry.register_secret("PW", "firstbuild_secret_value")
        mf = SecretMaskingFilter(registry)
        with patch(
            "datahub.masking.masking_filter.re.compile",
            side_effect=re.error("forced compile failure"),
        ):
            out = mf.mask_text("leak firstbuild_secret_value here")
        assert out == MASKING_ERROR_MESSAGE
        assert "firstbuild_secret_value" not in out

    def test_rebuild_failure_with_previous_keeps_previous(self, registry):
        registry.register_secret("PW", "previous_secret_value")
        mf = SecretMaskingFilter(registry)
        mf.mask_text("leak previous_secret_value")
        with patch(
            "datahub.masking.masking_filter.re.compile",
            side_effect=re.error("forced compile failure"),
        ):
            out = mf.mask_text("leak previous_secret_value again")
        assert "previous_secret_value" not in out
        assert "***REDACTED:PW***" in out


class TestRegexSecurity:
    """re.escape ensures secrets with regex metacharacters are matched
    literally, not as regex."""

    def test_metacharacters_literal(self, registry, masking_filter):
        registry.register_secret("PW", "a+b*c.d?e[f]g")
        out = masking_filter.mask_text("leak a+b*c.d?e[f]g here")
        assert "a+b*c.d?e[f]g" not in out
        assert "***REDACTED:PW***" in out

    def test_alternation_literal(self, registry, masking_filter):
        registry.register_secret("PW", "test|prod")
        out = masking_filter.mask_text("leak test|prod here")
        assert "test|prod" not in out
        assert "***REDACTED:PW***" in out

    def test_longest_first_prefix_secret(self, registry, masking_filter):
        """A short secret that is a prefix of a longer one must not win first
        (longest-first ordering)."""
        registry.register_secret("SHORT", "secret")
        registry.register_secret("LONG", "secret_value_long")
        out = masking_filter.mask_text("leak secret_value_long here")
        assert "secret_value_long" not in out
        assert "***REDACTED:LONG***" in out


def _strip_truncation_suffix(s: str) -> str:
    return re.split(r"\n\.\.\. \[\d+ bytes truncated for performance\]", s, maxsplit=1)[
        0
    ]


def _maximal_alphabet_runs(s: str, alphabet: set) -> list:
    runs = []
    i = 0
    n = len(s)
    while i < n:
        if s[i] in alphabet:
            j = i
            while j < n and s[j] in alphabet:
                j += 1
            runs.append(s[i:j])
            i = j
        else:
            i += 1
    return runs


class TestTruncationDifferentialProperty:
    """The truncation path leaks nothing that plain masking does not: every
    maximal run of secret-alphabet chars in the bounded output is a substring
    of the plain mask_text output."""

    BUDGET = 200

    def _setup_case(self, reg, case_id):
        # Returns (secret_values, alphabet, period_or_None).
        if case_id == "periodic_s3cr3t":
            secret = "s3cr3t" * 50
            reg.register_secret("P", secret)
            return [secret], set(secret), 6
        if case_id == "periodic_ab":
            secret = "ab" * 150
            reg.register_secret("P", secret)
            return [secret], set(secret), 2
        if case_id == "self_overlap_aab":
            secret = ("aab" * 100)[:300]
            reg.register_secret("P", secret)
            return [secret], set(secret), 3
        if case_id == "prefix_pair":
            short = "prefixed_secret_value_12345"
            long = short + "_extended_tail_part"
            reg.register_secret("SHORT", short)
            reg.register_secret("LONG", long)
            return [short, long], set(long), None
        if case_id == "marker_adjacent":
            secret = "***" + "k" * 297
            reg.register_secret("P", secret)
            return [secret], set(secret), None
        if case_id == "random_entropy":
            import random
            import string

            rng = random.Random(12345)
            secret = "".join(
                rng.choice(string.ascii_letters + string.digits) for _ in range(300)
            )
            reg.register_secret("P", secret)
            return [secret], set(secret), None
        raise AssertionError(case_id)

    def _longest(self, secrets):
        return max(len(s) for s in secrets)

    def _build_layout(self, secrets, period, layout_id, reg):
        longest = self._longest(secrets)
        secret = secrets[0]
        if layout_id == "straddle_budget":
            # A single occurrence straddling the budget boundary.
            return "Q" * 100 + secret + "Q" * 200
        if layout_id == "straddle_budget_plus_longest":
            return "Q" * (self.BUDGET + longest - 100) + secret + "Q" * 200
        if layout_id == "adjacent_copies":
            return secret + secret + "Q" * 100
        if layout_id == "partial_repeat_tail":
            if period is None:
                pytest.skip("partial-repeat tail only applies to periodic secrets")
            return secret + secret[: len(secret) - 2 * period] + "Q" * 100
        raise AssertionError(layout_id)

    @pytest.mark.parametrize(
        "case_id",
        [
            "periodic_s3cr3t",
            "periodic_ab",
            "self_overlap_aab",
            "prefix_pair",
            "marker_adjacent",
            "random_entropy",
        ],
    )
    @pytest.mark.parametrize(
        "layout_id",
        [
            "straddle_budget",
            "straddle_budget_plus_longest",
            "adjacent_copies",
            "partial_repeat_tail",
        ],
    )
    def test_bounded_leaks_no_more_than_baseline(self, case_id, layout_id):
        reg = SecretRegistry()
        reg.clear()
        secrets, alphabet, period = self._setup_case(reg, case_id)
        text = self._build_layout(secrets, period, layout_id, reg)
        mf = SecretMaskingFilter(reg, max_message_size=self.BUDGET)
        baseline = mf.mask_text(text)
        bounded = _strip_truncation_suffix(mf._mask_bounded(text, self.BUDGET))
        # Every maximal run of secret-alphabet chars in bounded must appear
        # somewhere in baseline (substring, not run-equality: the final cut
        # may bisect a residue run; bounded leaking less than baseline is fine).
        for run in _maximal_alphabet_runs(bounded, alphabet):
            assert run in baseline, (
                f"case={case_id} layout={layout_id} run={run!r} not in baseline"
            )
