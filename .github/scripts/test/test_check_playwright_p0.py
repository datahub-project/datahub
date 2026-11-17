"""Tests for the Playwright P0 and domain tag check."""

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


def test_known_domains_reads_the_enum() -> None:
    source = """
export enum DomainTag {
  Observe = '@domain:observe',
  PlatformInternal = '@domain:platform_internal',
}
"""
    assert check.known_domains(source) == {"observe", "platform_internal"}


def _listing(*specs: dict) -> dict:
    """A `playwright test --list --reporter=json` tree: file suite > describe > specs."""
    return {
        "suites": [
            {
                "title": "x.spec.ts",
                "specs": [],
                "suites": [{"title": "Suite", "specs": list(specs), "suites": []}],
            }
        ]
    }


def test_every_listed_test_needs_a_known_domain_tag() -> None:
    listing = _listing(
        {
            "title": "tagged",
            "file": "x.spec.ts",
            "line": 3,
            "tags": ["release", "domain:observe"],
        },
        {"title": "untagged", "file": "x.spec.ts", "line": 4, "tags": ["release"]},
        {"title": "typo", "file": "x.spec.ts", "line": 5, "tags": ["domain:obsrve"]},
    )
    problems = check.check_domain_tags(listing, domains={"observe", "catalog"})
    assert problems == [
        "x.spec.ts:4 Suite › untagged: no DomainTag; add one to its tag array",
        "x.spec.ts:5 Suite › typo: unknown domain tag @domain:obsrve",
    ]


def test_repo_specs_pass_static_rules() -> None:
    assert check.main([]) == 0


BORROWERS = {"proposals": ("doc-propagation/doc-propagation.spec.ts",)}


def test_seed_feature_in_own_directory_passes() -> None:
    source = "test.use({ viewport, featureName: ['search', \"search\"] });"
    assert check.check_seed_features("search/a.spec.ts", source, BORROWERS) == []


def test_undeclared_seed_borrower_fails_but_a_declared_one_passes() -> None:
    source = "test.use({ featureName: ['doc-propagation', 'proposals'] });"
    assert check.check_seed_features(
        "doc-propagation/doc-propagation.spec.ts", source, BORROWERS
    ) == []
    borrowing = "test.use({ featureName: ['lineage', 'proposals'] });"
    [(line, message)] = check.check_seed_features("lineage/a.spec.ts", borrowing, BORROWERS)
    assert line == 1
    assert "SEED_BORROWERS['proposals']" in message


def test_non_literal_feature_name_fails_and_comments_are_ignored() -> None:
    source = """
// featureName is set below; see tests/search/fixtures/data.json
const url = 'http://host//path'; test.use({ featureName: FEATURE });
"""
    assert [line for line, _ in check.check_seed_features("search/a.spec.ts", source, {})] == [3]


def test_stale_seed_borrower_entries_fail() -> None:
    sources = {"doc-propagation/doc-propagation.spec.ts": "test.use({ featureName: 'doc-propagation' });"}
    problems = check.check_seed_borrowers(
        sources, {"proposals": ("doc-propagation/doc-propagation.spec.ts", "gone/gone.spec.ts")}
    )
    assert len(problems) == 2
