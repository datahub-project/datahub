"""Dataclasses representing GitHub Actions workflow metrics."""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any


def parse_dt(value: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp from the GitHub API (handles trailing Z)."""
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def duration_seconds(start: datetime | None, end: datetime | None) -> int | None:
    """Compute duration in whole seconds. Returns None if either timestamp is None."""
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds()))


@dataclass
class StepMetrics:
    number: int | None
    name: str | None
    conclusion: str | None
    started_at: str | None
    completed_at: str | None
    duration_seconds: int | None

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "StepMetrics":
        started = parse_dt(data.get("started_at"))
        completed = parse_dt(data.get("completed_at"))
        return cls(
            number=data.get("number"),
            name=data.get("name"),
            conclusion=data.get("conclusion"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            duration_seconds=duration_seconds(started, completed),
        )


@dataclass
class JobMetrics:
    id: int | None
    name: str
    full_name: str
    matrix_values: list[str]
    conclusion: str | None
    started_at: str | None
    completed_at: str | None
    duration_seconds: int | None
    runner_name: str | None
    run_attempt: int
    steps: list[StepMetrics]

    @staticmethod
    def _parse_matrix(full_name: str) -> tuple[str, list[str]]:
        """Parse matrix values from a job's full name.

        If `full_name` matches "base_name (val1, val2, ...)", returns
        (base_name, [val1, val2, ...]). Otherwise returns (full_name, []).
        """
        match = re.match(r"^(.+?)\s*\((.+)\)$", full_name)
        if match:
            base_name = match.group(1).strip()
            matrix_values = [v.strip() for v in match.group(2).split(",")]
            return base_name, matrix_values
        return full_name, []

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "JobMetrics":
        full_name = data.get("name", "")
        name, matrix_values = cls._parse_matrix(full_name)
        started = parse_dt(data.get("started_at"))
        completed = parse_dt(data.get("completed_at"))
        return cls(
            id=data.get("id"),
            name=name,
            full_name=full_name,
            matrix_values=matrix_values,
            conclusion=data.get("conclusion"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            duration_seconds=duration_seconds(started, completed),
            runner_name=data.get("runner_name"),
            run_attempt=data.get("run_attempt", 1),
            steps=[StepMetrics.from_api(s) for s in data.get("steps", [])],
        )


@dataclass
class AttemptInfo:
    number: int
    type: str


@dataclass
class WorkflowMetrics:
    id: int | None
    name: str | None
    run_id: int
    trigger_event: str | None
    actor: str | None
    triggering_actor: str | None
    pull_request_number: int | None
    pull_request_url: str | None
    head_branch: str | None
    head_sha: str | None
    conclusion: str | None
    started_at: str | None
    completed_at: str | None
    duration_seconds: int | None
    attempt: AttemptInfo

    @classmethod
    def from_api(
        cls,
        data: dict[str, Any],
        run_id: int,
        attempt: int,
        rerun_type: str,
    ) -> "WorkflowMetrics":
        run_started = parse_dt(data.get("run_started_at"))
        run_completed = parse_dt(data.get("updated_at"))
        # pull_requests is populated only when the triggering event is pull_request
        pr = (data.get("pull_requests") or [None])[0]
        actor_obj: dict[str, Any] = data.get("actor") or {}
        triggering_actor_obj: dict[str, Any] = data.get("triggering_actor") or {}
        return cls(
            id=data.get("workflow_id"),
            name=data.get("name"),
            run_id=run_id,
            trigger_event=data.get("event"),
            actor=actor_obj.get("login"),
            triggering_actor=triggering_actor_obj.get("login"),
            pull_request_number=pr.get("number") if pr else None,
            pull_request_url=pr.get("url") if pr else None,
            head_branch=data.get("head_branch"),
            head_sha=data.get("head_sha"),
            conclusion=data.get("conclusion"),
            started_at=data.get("run_started_at"),
            completed_at=data.get("updated_at"),
            duration_seconds=duration_seconds(run_started, run_completed),
            attempt=AttemptInfo(number=attempt, type=rerun_type),
        )


@dataclass
class Metrics:
    collected_at: str
    repository: str
    workflow: WorkflowMetrics
    jobs: list[JobMetrics]
