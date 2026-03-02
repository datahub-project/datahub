"""GitHub Actions REST API client with rate-limit retry handling."""

import functools
import sys
import time
from collections.abc import Callable
from typing import Any

import requests


def handle_rate_limit(
    max_wait_seconds: int = 60, max_retries: int = 3
) -> Callable[[Callable[..., requests.Response]], Callable[..., requests.Response]]:
    """Decorator that retries on GitHub API rate limit (403 with rate limit headers).

    Checks the ``x-ratelimit-remaining`` and ``retry-after`` response headers.
    If a rate limit is hit, waits for the lesser of the reset wait time or
    *max_wait_seconds*, then retries.  Raises ``RuntimeError`` after
    *max_retries* consecutive rate-limited responses.
    """

    def decorator(
        func: Callable[..., requests.Response],
    ) -> Callable[..., requests.Response]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> requests.Response:
            last_resp: requests.Response | None = None
            for attempt in range(1, max_retries + 1):
                last_resp = func(*args, **kwargs)

                is_rate_limited = last_resp.headers.get("retry-after") or last_resp.headers.get("x-ratelimit-remaining")
                if last_resp.status_code in [403, 429] and is_rate_limited:
                    retry_after = last_resp.headers.get("retry-after")
                    reset_timestamp = last_resp.headers.get("x-ratelimit-reset")

                    if retry_after is not None:
                        wait = int(retry_after)
                    elif reset_timestamp is not None:
                        wait = max(0, int(reset_timestamp) - int(time.time()))
                    else:
                        wait = max_wait_seconds

                    wait = min(wait, max_wait_seconds)

                    if attempt < max_retries:
                        print(
                            f"Rate limited (attempt {attempt}/{max_retries}). "
                            f"Waiting {wait}s before retry.",
                            file=sys.stderr,
                        )
                        time.sleep(wait)
                        continue

                    raise RuntimeError(
                        f"GitHub API rate limit hit {max_retries} consecutive times. "
                        f"Last response: {last_resp.status_code} {last_resp.text}"
                    )

                return last_resp

            # All retries exhausted without returning — last_resp is guaranteed set
            assert last_resp is not None
            return last_resp

        return wrapper

    return decorator


class GitHubAPIClient:
    """Thin wrapper around requests.Session for GitHub REST API calls."""

    BASE_URL = "https://api.github.com"
    _RUN_ATTEMPT_PATH = "/repos/{repo}/actions/runs/{run_id}/attempts/{attempt}"
    _RUN_JOBS_PATH = "/repos/{repo}/actions/runs/{run_id}/jobs"

    def __init__(self, token: str, base_url: str = BASE_URL) -> None:
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )

    def _run_attempt_url(self, repo: str, run_id: str, attempt: int) -> str:
        return self.base_url + self._RUN_ATTEMPT_PATH.format(
            repo=repo, run_id=run_id, attempt=attempt
        )

    def _run_jobs_url(self, repo: str, run_id: str) -> str:
        return self.base_url + self._RUN_JOBS_PATH.format(repo=repo, run_id=run_id)

    def get_run_attempt(self, repo: str, run_id: str, attempt: int) -> dict[str, Any]:
        """Fetch a specific workflow run attempt."""
        return self.get(self._run_attempt_url(repo, run_id, attempt))

    def get_run_jobs(self, repo: str, run_id: str) -> list[dict[str, Any]]:
        """Fetch all jobs for a workflow run (latest version of each job across attempts)."""
        return self.get_paginated(
            self._run_jobs_url(repo, run_id), "jobs", params={"filter": "latest"}
        )

    @handle_rate_limit()
    def _request(
        self, url: str, params: dict[str, str] | None = None
    ) -> requests.Response:
        """Make a GET request with rate-limit retry handling."""
        return self.session.get(url, params=params)

    def get(self, url: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        """Make a GET request. Raises on non-2xx responses."""
        resp = self._request(url, params=params)
        if not resp.ok:
            message = f"GitHub API error: {resp.status_code} {resp.reason}\n{resp.text}"
            print(
                message,
                file=sys.stderr,
            )
            raise RuntimeError(message)
        return resp.json()

    def get_paginated(
        self,
        url: str,
        list_key: str,
        params: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all pages of a paginated endpoint.

        Returns the concatenated list from `list_key` across all pages.
        """
        params = dict(params or {})
        params["per_page"] = "100"
        page = 1
        all_items: list[dict[str, Any]] = []

        while True:
            params["page"] = str(page)
            data = self.get(url, params=params)
            items = data.get(list_key, [])
            all_items.extend(items)
            if len(items) < 100:
                break
            page += 1

        return all_items
