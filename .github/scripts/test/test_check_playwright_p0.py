"""Tests for the Playwright P0 serial-chain check."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import check_playwright_p0 as check  # noqa: E402

SPEC = Path("tests/x.spec.ts")


def test_p0_on_serial_describe_passes() -> None:
    source = """
test.describe('Incidents', { tag: [TestTag.Release, TestTag.P0] }, () => {
  test.describe.configure({ mode: 'serial' });
  test('create', { tag: [DomainTag.Observe] }, async () => {});
  test('update', { tag: [DomainTag.Observe] }, async () => {});
});
"""
    assert check.check_source(SPEC, source) == []


def test_p0_on_single_test_in_serial_spec_fails() -> None:
    source = """
test.describe('Incidents', () => {
  test.describe.configure({ mode: 'serial' });
  test('create', { tag: [DomainTag.Observe] }, async () => {});
  test(
    'update',
    { tag: [TestTag.P0, DomainTag.Observe] },
    async () => {},
  );
});
"""
    violations = check.check_source(SPEC, source)
    assert [(v.line, v.message) for v in violations] == [(7, check.SERIAL_MESSAGE)]


def test_p0_on_single_test_in_non_serial_spec_passes() -> None:
    source = """
test.describe('Sources', () => {
  test('create', { tag: [TestTag.P0, DomainTag.Ingestion] }, async () => {});
});
"""
    assert check.check_source(SPEC, source) == []


def test_describe_serial_and_raw_tag_are_recognized() -> None:
    source = """
test.describe.serial('Chain', () => {
  test.only('step', { tag: ['@p0', '@domain:catalog'] }, async () => {});
});
"""
    assert [v.message for v in check.check_source(SPEC, source)] == [
        check.SERIAL_MESSAGE
    ]


def test_repo_specs_pass() -> None:
    assert check.main() == 0
