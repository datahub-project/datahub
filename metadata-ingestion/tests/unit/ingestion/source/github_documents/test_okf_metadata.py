from datetime import datetime, timezone
from typing import Dict

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.ingestion.source.github_documents.okf_frontmatter import (
    parse_okf_frontmatter,
)
from datahub.ingestion.source.github_documents.okf_mapping import map_okf
from datahub.metadata.schema_classes import (
    OkfBundleClass,
    OkfPropertiesClass,
    OkfRevisionClass,
)


def _raw_frontmatter_values(properties: OkfPropertiesClass) -> Dict[str, object]:
    assert properties.raw is not None
    parsed = parse_okf_frontmatter(
        properties.raw,
        path="context/guides/mau.md",
        report=GitHubDocumentsSourceReport(),
    )
    assert parsed is not None
    return parsed.values


def _properties(fields: str, report: GitHubDocumentsSourceReport) -> OkfPropertiesClass:
    content = f"---\ntype: Guide\n{fields}---\nBody"
    parsed = parse_okf_frontmatter(content, path="context/guides/mau.md", report=report)
    assert parsed is not None
    mapped = map_okf(
        parsed,
        document_urn="urn:li:document:example",
        bundle=OkfBundleClass(
            location="https://github.com/example-org/analytics",
            rootPath="context",
            okfVersion="0.2",
        ),
        file_path="context/guides/mau.md",
        revision=OkfRevisionClass(ref="main"),
        attribution_source=None,
        ctx=PipelineContext(run_id="test"),
        report=report,
    )
    assert mapped is not None
    # Trust and lifecycle fields must not create native association patches.
    assert mapped.patches == []
    return next(a for a in mapped.aspects if isinstance(a, OkfPropertiesClass))


def test_lifecycle_trust_and_declared_id_are_typed_without_losing_frontmatter() -> None:
    report = GitHubDocumentsSourceReport()
    fields = (
        "id: producer:mau\n"
        'okfVersion: "99"\n'
        'okf_version: "98"\n'
        "status: experimental\n"
        "stale_after: 2026-09-23T02:00:00+02:00\n"
        "generated: {by: reference_agent/model, at: 2026-06-20T22:53:05Z}\n"
        "verified: {by: human:alice, at: 2026-06-25T09:00:00Z}\n"
    )
    properties = _properties(fields, report)
    assert properties.conceptId == "guides/mau"
    assert properties.declaredId == "producer:mau"
    assert properties.bundle is not None
    assert properties.bundle.okfVersion == "0.2"
    assert properties.status == "experimental"
    assert properties.staleAfter == int(
        datetime(2026, 9, 23, tzinfo=timezone.utc).timestamp() * 1000
    )
    assert properties.generated is not None
    assert properties.generated.by == "reference_agent/model"
    assert properties.generated.at == int(
        datetime(2026, 6, 20, 22, 53, 5, tzinfo=timezone.utc).timestamp() * 1000
    )
    assert properties.verified is not None and len(properties.verified) == 1
    assert properties.verified[0].by == "human:alice"
    assert properties.verified[0].at == int(
        datetime(2026, 6, 25, 9, tzinfo=timezone.utc).timestamp() * 1000
    )
    values = _raw_frontmatter_values(properties)
    assert values["status"] == "experimental"
    assert values["stale_after"] == "2026-09-23T02:00:00+02:00"
    generated = values["generated"]
    assert isinstance(generated, dict)
    assert generated["by"] == "reference_agent/model"
    assert isinstance(values["verified"], dict)
    assert values["okfVersion"] == "99"
    assert values["okf_version"] == "98"
    assert properties.raw == f"---\ntype: Guide\n{fields}---\n"
    assert report.okf_invalid_values == 0


def test_verified_list_and_stamps_without_times() -> None:
    report = GitHubDocumentsSourceReport()
    properties = _properties(
        "generated: {by: human:alice}\n"
        "verified:\n"
        "  - {by: human:alice}\n"
        "  - {by: process:nightly, at: 2026-06-25T09:00:00Z}\n",
        report,
    )
    assert properties.generated is not None
    assert properties.generated.at is None
    assert properties.verified is not None
    assert [stamp.by for stamp in properties.verified] == [
        "human:alice",
        "process:nightly",
    ]
    assert properties.verified[0].at is None
    assert properties.verified[1].at is not None
    assert report.okf_invalid_values == 0


def test_verified_without_valid_entries_is_omitted() -> None:
    report = GitHubDocumentsSourceReport()
    properties = _properties("verified: [{at: 2026-06-25T09:00:00Z}]\n", report)
    assert properties.verified is None
    assert report.okf_invalid_values == 1
    assert report.warnings


@pytest.mark.parametrize("value", ["not-a-date", "2026-09-23", "2026-09-23T00:00:00"])
def test_invalid_or_offsetless_stale_after_warns_and_preserves_value(
    value: str,
) -> None:
    report = GitHubDocumentsSourceReport()
    properties = _properties(f"stale_after: {value}\n", report)
    assert properties.staleAfter is None
    assert _raw_frontmatter_values(properties)["stale_after"] == value
    assert report.okf_invalid_values == 1
    assert report.warnings


def test_invalid_stamp_fields_warn_without_discarding_the_concept() -> None:
    report = GitHubDocumentsSourceReport()
    properties = _properties(
        "generated: {by: human:alice, at: invalid}\n"
        "verified: [{at: 2026-06-25T09:00:00Z}, {by: process:nightly}]\n",
        report,
    )
    assert properties.generated is not None
    assert properties.generated.by == "human:alice"
    assert properties.generated.at is None
    assert properties.verified is not None
    assert [stamp.by for stamp in properties.verified] == ["process:nightly"]
    verified = _raw_frontmatter_values(properties)["verified"]
    assert isinstance(verified, list) and len(verified) == 2
    assert report.okf_invalid_values == 2


def test_optional_lifecycle_and_trust_fields_remain_unset() -> None:
    properties = _properties("", GitHubDocumentsSourceReport())
    assert properties.status is None
    assert properties.staleAfter is None
    assert properties.generated is None
    assert properties.verified is None
    assert properties.declaredId is None


def test_non_string_status_is_preserved_and_reported() -> None:
    report = GitHubDocumentsSourceReport()
    properties = _properties("status: 42\n", report)
    assert properties.status is None
    assert _raw_frontmatter_values(properties)["status"] == 42
    assert report.okf_invalid_values == 1
    assert report.warnings
