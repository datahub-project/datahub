import base64
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import pytest
import requests_mock
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.gitlab_documents.gitlab_documents_source import (
    GitLabDocumentsSource,
)
from datahub.testing import mce_helpers

BASE_URL = "https://gitlab.example.com"
PROJECT = "example-group/data/analytics"
COMMIT_SHA = "a" * 40
# Same repository content as the github-documents golden test, so the two
# sources are checked against identical input.
FIXTURES = Path(__file__).parent.parent / "github_documents" / "fixtures"


@time_machine.travel(datetime(2026, 1, 1, tzinfo=timezone.utc), tick=False)
@pytest.mark.integration
def test_gitlab_documents_ingest(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    api = f"{BASE_URL}/api/v4/projects/{quote(PROJECT, safe='')}"
    tree = []
    output = tmp_path / "gitlab_documents_mces.json"
    with requests_mock.Mocker() as m:
        m.get(f"{api}/repository/branches/main", json={"commit": {"id": COMMIT_SHA}})
        for path in sorted(FIXTURES.rglob("*.md")):
            relative = path.relative_to(FIXTURES).as_posix()
            contents = path.read_bytes()
            blob = f"blob {len(contents)}\0".encode() + contents
            tree.append(
                {
                    "id": hashlib.sha1(blob).hexdigest(),
                    "type": "blob",
                    "path": relative,
                }
            )
            m.get(
                f"{api}/repository/files/{quote(relative, safe='')}",
                json={
                    "size": len(contents),
                    "encoding": "base64",
                    "content": base64.b64encode(contents).decode(),
                },
            )
        m.get(f"{api}/repository/tree", json=tree)

        pipeline = Pipeline.create(
            {
                "run_id": "gitlab-documents-golden",
                "pipeline_name": "gitlab-documents-golden",
                "source": {
                    "type": "gitlab-documents",
                    "config": {
                        "base_url": BASE_URL,
                        "repository": PROJECT,
                        "gitlab_token": "fixture-token",
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

    assert isinstance(pipeline.source, GitLabDocumentsSource)
    report = pipeline.source.get_report()
    assert report.files_processed == 6
    assert report.okf_files_processed == 1
    assert report.okf_parse_errors == 1
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=Path(__file__).parent / "gitlab_documents_mces_golden.json",
        fail_on_output_mces=True,
    )
