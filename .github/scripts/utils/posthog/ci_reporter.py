"""Send GitHub Actions workflow metrics to PostHog."""

from typing import Any

import posthog

from utils.datetime_utils import parse_dt

POSTHOG_HOST = "https://us.i.posthog.com"


class PostHogCIReporter:
    def __init__(self, api_key: str) -> None:
        posthog.api_key = api_key
        posthog.host = POSTHOG_HOST

    def send(self, data: dict[str, Any]) -> None:
        """
        Capture step, job, and workflow events then flush.
        Note: The posthog.capture queues and returns. Will need to monitor logs
        for any errors.
        """
        repo = data["repository"]
        workflow_props = self._workflow_props(data)

        for job in data.get("jobs", []):
            job_props = self._job_props(job)
            for step in job.get("steps", []):
                posthog.capture(
                    distinct_id=repo,
                    event="ci_step_completed",
                    properties={
                        **workflow_props,
                        **job_props,
                        **self._step_props(step),
                    },
                    timestamp=parse_dt(step.get("completed_at")),
                )
            posthog.capture(
                distinct_id=repo,
                event="ci_job_completed",
                properties={**workflow_props, **job_props},
                timestamp=parse_dt(job.get("completed_at")),
            )

        posthog.capture(
            distinct_id=repo,
            event="ci_workflow_run_completed",
            properties=workflow_props,
            timestamp=parse_dt(data["workflow"].get("completed_at")),
        )

        # flush() blocks until the background delivery thread has sent all queued events —
        # necessary for short-lived scripts that would otherwise exit before delivery completes.
        posthog.flush()

    def _workflow_props(self, data: dict[str, Any]) -> dict[str, Any]:
        w = data["workflow"]
        attempt = w.get("attempt", {})
        return {
            "repository": data["repository"],
            "run_id": w.get("run_id"),
            "workflow_id": w.get("id"),
            "workflow_name": w.get("name"),
            "trigger_event": w.get("trigger_event"),
            "actor": w.get("actor"),
            "triggering_actor": w.get("triggering_actor"),
            "pull_request_number": w.get("pull_request_number"),
            "pull_request_url": w.get("pull_request_url"),
            "head_branch": w.get("head_branch"),
            "head_sha": w.get("head_sha"),
            "conclusion": w.get("conclusion"),
            "duration_seconds": w.get("duration_seconds"),
            "attempt_number": attempt.get("number"),
            "attempt_type": attempt.get("type"),
            "job_count": len(data.get("jobs", [])),
        }

    def _job_props(self, job: dict[str, Any]) -> dict[str, Any]:
        return {
            "job_id": job.get("id"),
            "job_name": job.get("name"),
            "job_full_name": job.get("full_name"),
            "job_matrix_values": job.get("matrix_values"),
            "job_conclusion": job.get("conclusion"),
            "job_duration_seconds": job.get("duration_seconds"),
            "runner_name": job.get("runner_name"),
            "step_count": len(job.get("steps", [])),
        }

    def _step_props(self, step: dict[str, Any]) -> dict[str, Any]:
        return {
            "step_number": step.get("number"),
            "step_name": step.get("name"),
            "step_conclusion": step.get("conclusion"),
            "step_duration_seconds": step.get("duration_seconds"),
        }
