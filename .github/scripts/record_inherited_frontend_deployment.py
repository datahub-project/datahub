#!/usr/bin/env python3
"""Record a production frontend deployment for a master commit that did not rebuild.

Meticulous compares a pull request with a test run on the merge-base commit. That
run is created only when the commit has a ``datahub-project-web-react (Production)``
GitHub Deployment. Frontend Preview records one after it publishes to Cloudflare
Pages, and it publishes only when the frontend changed. A backend-only master
commit therefore has no deployment, and a PR branched from it is skipped with
"no base test run was found".

The frontend bundle is unchanged, so the new deployment reuses the last successful
Pages URL. ``auto_inactive`` stays false: marking the previous production
deployment inactive would drop the base run for the commit that actually built it.

The environment name must match the string recorded in
``.github/workflows/react-cloudflare-pages.yml``.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence

# Keep in sync with ENVIRONMENT in react-cloudflare-pages.yml.
PRODUCTION_ENVIRONMENT = "datahub-project-web-react (Production)"
DEPLOYMENT_DESCRIPTION = "Cloudflare Pages"

_MAX_DEPLOYMENTS = 100
_TRANSIENT_STATUS = {429, 500, 502, 503, 504}
_RETRY_DELAYS_SECONDS = (1, 2, 4)


class GitHubAPIError(RuntimeError):
    """GitHub REST call failed."""


class NoPublishedDeploymentError(RuntimeError):
    """No earlier successful production deployment exists to reuse."""


class DeploymentClient(Protocol):
    def list_deployments(
        self, repo: str, *, environment: str, sha: str | None
    ) -> list[dict[str, Any]]: ...

    def list_statuses(self, repo: str, deployment_id: int) -> list[dict[str, Any]]: ...

    def create_deployment(self, repo: str, body: Mapping[str, Any]) -> dict[str, Any]: ...

    def create_status(
        self, repo: str, deployment_id: int, body: Mapping[str, Any]
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class RecordResult:
    action: str
    deployment_id: int
    environment_url: str
    source_deployment_id: int | None = None


def success_environment_url(statuses: Sequence[Mapping[str, Any]]) -> str | None:
    """URL from the latest status, when that status is a successful https deploy.

    ``statuses`` is newest first, matching the GitHub deployments API.
    An inactive or failed latest status hides any earlier success on that deployment.
    """
    if not statuses:
        return None
    if statuses[0].get("state") != "success":
        return None
    for status in statuses:
        if status.get("state") != "success":
            continue
        url = str(status.get("environment_url") or "").strip()
        if url.startswith("https://"):
            return url
    return None


def _deployment_id(deployment: Mapping[str, Any]) -> int:
    raw = deployment.get("id")
    if not isinstance(raw, int):
        raise GitHubAPIError(f"Deployment is missing an integer id: {deployment!r}")
    return raw


def _sha(deployment: Mapping[str, Any]) -> str:
    return str(deployment.get("sha") or "").lower()


def record_inherited_deployment(
    client: DeploymentClient,
    *,
    repo: str,
    sha: str,
    environment: str,
    log_url: str | None,
) -> RecordResult:
    """Create a production deployment for ``sha`` pointing at the last Pages URL.

    A successful deployment already on ``sha`` is left alone. A deployment that
    exists but never reached success gets a success status instead of a second
    deployment, so a rerun after a partial failure does not pile up records.
    """
    sha = sha.lower()
    existing = client.list_deployments(repo, environment=environment, sha=sha)
    for deployment in existing:
        url = success_environment_url(client.list_statuses(repo, _deployment_id(deployment)))
        if url:
            return RecordResult(
                action="already_recorded",
                deployment_id=_deployment_id(deployment),
                environment_url=url,
            )

    existing_id = _deployment_id(existing[0]) if existing else None

    source_id: int | None = None
    source_url: str | None = None
    for deployment in client.list_deployments(repo, environment=environment, sha=None):
        if _sha(deployment) == sha:
            continue
        deployment_id = _deployment_id(deployment)
        url = success_environment_url(client.list_statuses(repo, deployment_id))
        if url:
            source_id = deployment_id
            source_url = url
            break

    if source_id is None or source_url is None:
        raise NoPublishedDeploymentError(
            f"No successful {environment!r} deployment with an https URL to reuse for {sha}"
        )

    if existing_id is None:
        created = client.create_deployment(
            repo,
            {
                "ref": sha,
                "environment": environment,
                "description": DEPLOYMENT_DESCRIPTION,
                "auto_merge": False,
                "required_contexts": [],
                "transient_environment": False,
                "production_environment": True,
                # String, matching the create-deployment schema. Marks records that
                # reuse a previous Pages URL so they can be told apart from a build.
                "payload": json.dumps(
                    {"inherited": True, "source_deployment_id": source_id}
                ),
            },
        )
        deployment_id = _deployment_id(created)
        action = "created"
    else:
        deployment_id = existing_id
        action = "status_on_existing"

    status_body: dict[str, Any] = {
        "state": "success",
        "environment": environment,
        "environment_url": source_url,
        "description": DEPLOYMENT_DESCRIPTION,
        # Default true would mark every earlier production deployment inactive.
        "auto_inactive": False,
    }
    if log_url:
        status_body["log_url"] = log_url
    client.create_status(repo, deployment_id, status_body)
    return RecordResult(
        action=action,
        deployment_id=deployment_id,
        environment_url=source_url,
        source_deployment_id=source_id,
    )


class UrllibDeploymentClient:
    """GitHub REST client using the stdlib so the workflow does not pip-install."""

    def __init__(self, token: str, api_url: str) -> None:
        self._token = token
        self._api_url = api_url.rstrip("/")

    def list_deployments(
        self, repo: str, *, environment: str, sha: str | None
    ) -> list[dict[str, Any]]:
        query: dict[str, str] = {
            "environment": environment,
            "per_page": str(_MAX_DEPLOYMENTS),
        }
        if sha is not None:
            query["sha"] = sha
        payload = self._request("GET", f"/repos/{repo}/deployments", query=query)
        if not isinstance(payload, list):
            raise GitHubAPIError(f"Expected a deployment list, got {type(payload).__name__}")
        return payload

    def list_statuses(self, repo: str, deployment_id: int) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            f"/repos/{repo}/deployments/{deployment_id}/statuses",
            query={"per_page": "20"},
        )
        if not isinstance(payload, list):
            raise GitHubAPIError(f"Expected a status list, got {type(payload).__name__}")
        return payload

    def create_deployment(self, repo: str, body: Mapping[str, Any]) -> dict[str, Any]:
        payload = self._request("POST", f"/repos/{repo}/deployments", body=body)
        if not isinstance(payload, dict):
            raise GitHubAPIError(f"Expected a deployment object, got {type(payload).__name__}")
        return payload

    def create_status(
        self, repo: str, deployment_id: int, body: Mapping[str, Any]
    ) -> dict[str, Any]:
        payload = self._request(
            "POST", f"/repos/{repo}/deployments/{deployment_id}/statuses", body=body
        )
        if not isinstance(payload, dict):
            raise GitHubAPIError(f"Expected a status object, got {type(payload).__name__}")
        return payload

    def _request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> Any:
        url = self._api_url + path
        if query:
            url = f"{url}?{urllib.parse.urlencode(query)}"
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "datahub-inherited-frontend-deployment",
                **({"Content-Type": "application/json"} if data is not None else {}),
            },
        )
        last_error: GitHubAPIError | None = None
        for attempt, delay in enumerate((0, *_RETRY_DELAYS_SECONDS)):
            if delay:
                time.sleep(delay)
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    raw = response.read()
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", errors="replace")[:500]
                last_error = GitHubAPIError(f"{method} {path} failed: {error.code} {detail}")
                if error.code in _TRANSIENT_STATUS and attempt < len(_RETRY_DELAYS_SECONDS):
                    print(f"{last_error} (attempt {attempt + 1}); retrying", file=sys.stderr)
                    continue
                raise last_error from error
            except urllib.error.URLError as error:
                last_error = GitHubAPIError(f"{method} {path} failed: {error.reason}")
                if attempt < len(_RETRY_DELAYS_SECONDS):
                    print(f"{last_error} (attempt {attempt + 1}); retrying", file=sys.stderr)
                    continue
                raise last_error from error
            if not raw:
                return None
            return json.loads(raw)
        assert last_error is not None
        raise last_error


def _log_url_from_env() -> str | None:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com").rstrip("/")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if not repo or not run_id:
        return None
    return f"{server}/{repo}/actions/runs/{run_id}"


def main() -> int:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    sha = os.environ.get("GITHUB_SHA", "")
    if not token or not repo or not sha:
        print(
            "::error::GITHUB_TOKEN, GITHUB_REPOSITORY, and GITHUB_SHA are required",
            file=sys.stderr,
        )
        return 1
    environment = os.environ.get("DEPLOYMENT_ENVIRONMENT", PRODUCTION_ENVIRONMENT)
    client = UrllibDeploymentClient(
        token, os.environ.get("GITHUB_API_URL", "https://api.github.com")
    )
    try:
        result = record_inherited_deployment(
            client,
            repo=repo,
            sha=sha,
            environment=environment,
            log_url=_log_url_from_env(),
        )
    except (GitHubAPIError, NoPublishedDeploymentError) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1

    if result.action == "already_recorded":
        print(
            f"SHA {sha} already has {environment} deployment "
            f"{result.deployment_id} -> {result.environment_url}"
        )
    else:
        print(
            f"Recorded {environment} deployment {result.deployment_id} "
            f"({result.action}) -> {result.environment_url} "
            f"inherited from {result.source_deployment_id}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
