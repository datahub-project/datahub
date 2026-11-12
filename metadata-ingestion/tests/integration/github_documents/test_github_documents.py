import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from unittest.mock import patch

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.github_documents.github_api import GitHubFileInfo
from datahub.ingestion.source.github_documents.github_documents_source import (
    GitHubDocumentsSource,
)
from datahub.testing import mce_helpers


@time_machine.travel(datetime(2026, 1, 1, tzinfo=timezone.utc), tick=False)
@pytest.mark.integration
def test_github_documents_ingest(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    resources = Path(__file__).parent
    fixtures = resources / "fixtures"
    files: List[GitHubFileInfo] = []
    for path in sorted(fixtures.rglob("*.md")):
        contents = path.read_bytes()
        blob = f"blob {len(contents)}\0".encode() + contents
        files.append(
            GitHubFileInfo(
                path=path.relative_to(fixtures).as_posix(),
                size=len(contents),
                sha=hashlib.sha1(blob).hexdigest(),
            )
        )

    def fetch_file(repository: str, file_path: str, branch: str) -> str:
        assert repository == "example-org/analytics"
        assert branch == "a" * 40
        return (fixtures / file_path).read_text()

    output = tmp_path / "github_documents_mces.json"
    with patch(
        "datahub.ingestion.source.github_documents.github_documents_source.GitHubApiClient",
        autospec=True,
    ) as client_class:
        client = client_class.return_value
        client.list_matching_files.return_value = (files, False)
        client.index_file_paths = {
            file.path for file in files if file.path.endswith("index.md")
        }
        client.get_latest_commit_sha.return_value = "a" * 40
        client.fetch_file_content.side_effect = fetch_file
        pipeline = Pipeline.create(
            {
                "run_id": "github-documents-golden",
                "pipeline_name": "github-documents-golden",
                "source": {
                    "type": "github-documents",
                    "config": {
                        "repository": "example-org/analytics",
                        "github_token": "fixture-token",
                        "enable_okf": True,
                    },
                },
                "sink": {"type": "file", "config": {"filename": str(output)}},
            },
            report_to=None,
        )
        assert pipeline.ctx.graph is None
        pipeline.run()
        # The malformed fixture intentionally produces one non-fatal warning.
        pipeline.raise_from_status(raise_warnings=False)

    assert isinstance(pipeline.source, GitHubDocumentsSource)
    report = pipeline.source.get_report()
    assert report.files_processed == 6
    assert report.okf_files_processed == 1
    assert report.okf_parse_errors == 1
    assert report.okf_unresolved_values == 0
    assert report.warnings
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=resources / "github_documents_mces_golden.json",
        fail_on_output_mces=True,
    )
