from unittest.mock import patch

import pytest

from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.ingestion.source.github_documents.okf_frontmatter import (
    MAX_FRONTMATTER_BYTES,
    parse_okf_frontmatter,
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
    report = GitHubDocumentsSourceReport()
    with patch(
        "datahub.ingestion.source.github_documents.okf_frontmatter.yaml.load"
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
    report = GitHubDocumentsSourceReport()
    raw = "---\ntype: Guide\nvalue: "
    raw += "x" * (MAX_FRONTMATTER_BYTES - len(raw) - len("\n---\n")) + "\n---\n"
    parsed = parse_okf_frontmatter(
        raw + "body" * MAX_FRONTMATTER_BYTES, path="guide.md", report=report
    )
    assert parsed is not None and parsed.raw == raw
    assert report.okf_parse_errors == 0
