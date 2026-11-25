from unittest.mock import patch

import pytest

from datahub.ingestion.source.repository_documents.okf_frontmatter import (
    MAX_FRONTMATTER_BYTES,
    is_okf_concept,
    parse_okf_frontmatter,
    read_okf_frontmatter,
)
from datahub.ingestion.source.repository_documents.report import (
    RepositoryDocumentsSourceReport,
)


@pytest.mark.parametrize(
    "yaml_text",
    [
        "type: Guide\nseed: &level0 [x]\n"
        + "".join(
            f"level{level}: &level{level} [{', '.join([f'*level{level - 1}'] * 10)}]\n"
            for level in range(1, 10)
        ),
        "type: Guide\nunused: &anchor value\n",
        "type: Guide\nvalue: *missing\n",
        "type: Guide\nvalue: " + "x" * MAX_FRONTMATTER_BYTES + "\n",
        "type: Guide\nvalue: " + "é" * (MAX_FRONTMATTER_BYTES // 2) + "\n",
    ],
    ids=["alias-bomb", "anchor", "alias", "oversize", "oversize-utf8"],
)
def test_unsafe_frontmatter_is_rejected_before_construction(yaml_text: str) -> None:
    report = RepositoryDocumentsSourceReport()
    with patch(
        "datahub.ingestion.source.repository_documents.okf_frontmatter.yaml.load"
    ) as load:
        assert (
            parse_okf_frontmatter(
                f"---\n{yaml_text}---\nBody", path="guide.md", report=report
            )
            is None
        )
        load.assert_not_called()
    assert report.okf_parse_errors == 1
    assert report.warnings


def test_limit_applies_to_frontmatter_not_body() -> None:
    report = RepositoryDocumentsSourceReport()
    raw = "---\ntype: Guide\nvalue: "
    raw += "x" * (MAX_FRONTMATTER_BYTES - len(raw) - len("\n---\n")) + "\n---\n"
    parsed = parse_okf_frontmatter(
        raw + "body" * MAX_FRONTMATTER_BYTES, path="guide.md", report=report
    )
    assert parsed is not None and parsed.raw == raw
    assert report.okf_parse_errors == 0


@pytest.mark.parametrize(
    "content, concept",
    [
        ("---\ntype: Guide\n---\nBody", True),
        ("---\ntype: '  '\n---\nBody", False),
        ("---\ntype: 3\n---\nBody", False),
        ("---\ntitle: Guide\n---\nBody", False),
    ],
)
def test_a_concept_needs_a_non_empty_string_type(content: str, concept: bool) -> None:
    report = RepositoryDocumentsSourceReport()
    parsed = parse_okf_frontmatter(content, path="guide.md", report=report)
    assert (parsed is not None) == concept
    assert is_okf_concept(read_okf_frontmatter(content).frontmatter) == concept
    # A declared but unusable type is reported; a missing one is not.
    assert report.okf_unresolved_values == int("type:" in content and not concept)


@pytest.mark.parametrize(
    "content, reason",
    [
        ("Body", "no frontmatter"),
        ("---\n- a\n---\nBody", "not a YAML mapping"),
        ("---\ntitle: Guide\n---\nBody", "type"),
        ("---\nvalue: *missing\n---\nBody", "aliases"),
    ],
)
def test_read_okf_frontmatter_says_why_without_reporting(
    content: str, reason: str
) -> None:
    result = read_okf_frontmatter(content)
    assert result.frontmatter is None
    assert result.rejection is not None and reason in result.rejection
