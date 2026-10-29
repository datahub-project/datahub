#!/usr/bin/env python3
"""Unit tests for record_inherited_frontend_deployment."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Mapping
from urllib.request import Request

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import record_inherited_frontend_deployment as inherited  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[3]
PAGES_URL = "https://abc123.datahub-project-web-react.pages.dev"
OLDER_URL = "https://older.datahub-project-web-react.pages.dev"


def _deployment(
    deployment_id: int,
    sha: str,
    *,
    environment: str = inherited.SOURCE_ENVIRONMENTS[1],
    transient: bool = False,
    created_at: str = "2026-10-01T00:00:00Z",
) -> dict[str, Any]:
    return {
        "id": deployment_id,
        "sha": sha,
        "environment": environment,
        "transient_environment": transient,
        "created_at": created_at,
    }


def _success(url: str) -> dict[str, Any]:
    return {"state": "success", "environment_url": url}


class FakeClient:
    def __init__(self) -> None:
        self.deployments: list[dict[str, Any]] = []
        self.statuses: dict[int, list[dict[str, Any]]] = {}
        self.created: list[dict[str, Any]] = []
        self.status_posts: list[tuple[int, dict[str, Any]]] = []
        self._next_id = 100

    def list_deployments(
        self, repo: str, *, environment: str, sha: str | None
    ) -> list[dict[str, Any]]:
        assert repo == "acme/datahub"
        matched = [item for item in self.deployments if item.get("environment") == environment]
        if sha is not None:
            matched = [item for item in matched if item["sha"].lower() == sha.lower()]
        return list(matched)

    def list_statuses(self, repo: str, deployment_id: int) -> list[dict[str, Any]]:
        return list(self.statuses.get(deployment_id, []))

    def create_deployment(self, repo: str, body: Mapping[str, Any]) -> dict[str, Any]:
        self.created.append(dict(body))
        deployment_id = self._next_id
        self._next_id += 1
        deployment = _deployment(
            deployment_id,
            str(body["ref"]),
            environment=str(body["environment"]),
            transient=bool(body["transient_environment"]),
            created_at="2099-01-01T00:00:00Z",
        )
        self.deployments.insert(0, deployment)
        return deployment

    def create_status(
        self, repo: str, deployment_id: int, body: Mapping[str, Any]
    ) -> dict[str, Any]:
        posted = dict(body)
        self.status_posts.append((deployment_id, posted))
        self.statuses.setdefault(deployment_id, []).insert(0, posted)
        return posted


def _record(client: FakeClient, sha: str = "abc123") -> inherited.RecordResult:
    return inherited.record_inherited_deployment(
        client,
        repo="acme/datahub",
        sha=sha,
        environment=inherited.RECORD_ENVIRONMENT,
        source_environments=inherited.SOURCE_ENVIRONMENTS,
        log_url="https://github.com/acme/datahub/actions/runs/7",
    )


def test_reuses_newest_successful_pages_url() -> None:
    client = FakeClient()
    client.deployments = [
        _deployment(
            3,
            "prpr",
            environment=inherited.RECORD_ENVIRONMENT,
            transient=True,
            created_at="2026-10-03T00:00:00Z",
        ),
        _deployment(2, "bbbb", created_at="2026-10-02T00:00:00Z"),
        _deployment(1, "aaaa", created_at="2026-10-01T00:00:00Z"),
    ]
    client.statuses = {
        3: [_success("https://pull-request.example")],
        2: [_success(PAGES_URL)],
        1: [_success(OLDER_URL)],
    }

    result = _record(client, "cccc")

    assert result.action == "created"
    assert result.environment_url == PAGES_URL
    assert result.source_deployment_id == 2
    assert client.created == [
        {
            "ref": "cccc",
            "environment": inherited.RECORD_ENVIRONMENT,
            "description": "Cloudflare Pages",
            "auto_merge": False,
            "required_contexts": [],
            "transient_environment": False,
            "production_environment": False,
            "payload": json.dumps({"inherited": True, "source_deployment_id": 2}),
        }
    ]
    assert client.status_posts == [
        (
            result.deployment_id,
            {
                "state": "success",
                "environment": inherited.RECORD_ENVIRONMENT,
                "environment_url": PAGES_URL,
                "description": "Cloudflare Pages",
                "auto_inactive": False,
                "log_url": "https://github.com/acme/datahub/actions/runs/7",
            },
        )
    ]


def test_skips_inactive_newer_deployment() -> None:
    client = FakeClient()
    client.deployments = [_deployment(3, "bbbb"), _deployment(2, "aaaa")]
    client.statuses = {
        3: [{"state": "inactive", "environment_url": "https://gone.example"}],
        2: [_success(OLDER_URL)],
    }

    result = _record(client, "cccc")

    assert result.environment_url == OLDER_URL
    assert result.source_deployment_id == 2


def test_does_not_reuse_current_sha_or_non_https_url() -> None:
    client = FakeClient()
    client.deployments = [
        _deployment(5, "cccc", environment=inherited.RECORD_ENVIRONMENT),
        _deployment(4, "bbbb"),
        _deployment(2, "aaaa"),
    ]
    client.statuses = {
        5: [{"state": "failure"}],
        4: [_success("http://insecure.example")],
        2: [_success(PAGES_URL)],
    }

    result = _record(client, "CCCC")

    assert result.action == "status_on_existing"
    assert result.deployment_id == 5
    assert result.environment_url == PAGES_URL
    assert result.source_deployment_id == 2
    assert client.created == []


def test_already_recorded_sha_is_left_alone() -> None:
    client = FakeClient()
    client.deployments = [
        _deployment(9, "cccc", environment=inherited.RECORD_ENVIRONMENT),
        _deployment(2, "aaaa"),
    ]
    client.statuses = {9: [_success(PAGES_URL)], 2: [_success(OLDER_URL)]}

    result = _record(client, "cccc")

    assert result.action == "already_recorded"
    assert result.deployment_id == 9
    assert result.environment_url == PAGES_URL
    assert client.created == []
    assert client.status_posts == []


def test_missing_source_deployment_raises() -> None:
    client = FakeClient()
    client.deployments = [_deployment(3, "bbbb")]
    client.statuses = {3: [{"state": "failure"}]}

    with pytest.raises(inherited.NoPublishedDeploymentError):
        _record(client, "cccc")
    assert client.created == []


def test_success_url_ignores_older_success_once_latest_is_inactive() -> None:
    statuses = [
        {"state": "inactive", "environment_url": ""},
        _success(PAGES_URL),
    ]
    assert inherited.success_environment_url(statuses) is None


def test_environment_name_matches_frontend_preview_workflow() -> None:
    workflow = (REPO_ROOT / ".github/workflows/react-cloudflare-pages.yml").read_text()
    assert f'ENVIRONMENT="{inherited.RECORD_ENVIRONMENT}"' in workflow
    for environment in inherited.SOURCE_ENVIRONMENTS:
        assert environment in workflow


def test_inherited_record_stays_inside_frontend_preview() -> None:
    workflow = (REPO_ROOT / ".github/workflows/react-cloudflare-pages.yml").read_text()
    assert "record_inherited_frontend_deployment.py" in workflow
    assert "github.ref == 'refs/heads/master'" in workflow
    assert "frontend-change == 'false'" in workflow
    assert "needs.setup.outputs.frontend_change == 'true'" in workflow
    assert not (REPO_ROOT / ".github/workflows/record-inherited-frontend-deployment.yml").exists()
    listed = (REPO_ROOT / ".github/workflows/post-workflow-actions.yml").read_text()
    assert "Record inherited frontend deployment" not in listed


def test_list_deployments_encodes_environment_name(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    class _Response:
        def read(self) -> bytes:
            return b"[]"

        def __enter__(self) -> _Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    def fake_urlopen(request: Request, timeout: int = 0) -> _Response:
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        assert timeout == 30
        return _Response()

    monkeypatch.setattr(inherited.urllib.request, "urlopen", fake_urlopen)
    client = inherited.UrllibDeploymentClient("token", "https://api.github.com")
    assert client.list_deployments(
        "acme/datahub",         environment=inherited.RECORD_ENVIRONMENT, sha=None
    ) == []
    assert captured["method"] == "GET"
    assert "environment=datahub-project-web-react+%28Preview%29" in captured["url"]
    assert "per_page=100" in captured["url"]
