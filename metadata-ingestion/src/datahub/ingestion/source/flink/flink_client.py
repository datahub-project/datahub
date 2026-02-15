import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from datahub.ingestion.source.flink.flink_config import FlinkSourceConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FlinkPlanNode:
    """A node in the Flink execution plan graph."""

    id: str
    description: str
    operator: str
    parallelism: int
    inputs: List[Dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class FlinkJobSummary:
    """Summary from /v1/jobs/overview."""

    jid: str
    name: str
    state: str
    start_time: int
    end_time: int
    duration: int
    last_modification: int


@dataclass(frozen=True)
class FlinkJobDetail:
    """Detail from /v1/jobs/:jobid."""

    jid: str
    name: str
    state: str
    start_time: int
    end_time: int
    duration: int
    job_type: Optional[str]
    max_parallelism: Optional[int]
    plan_nodes: List[FlinkPlanNode]


@dataclass(frozen=True)
class FlinkCheckpointConfig:
    """Checkpoint config from /v1/jobs/:jobid/checkpoints/config."""

    mode: Optional[str]
    interval: Optional[int]
    timeout: Optional[int]
    externalized_checkpoint_config: Optional[Dict[str, Any]]
    state_backend: Optional[str]
    checkpoint_storage: Optional[str]


@dataclass(frozen=True)
class FlinkClusterConfig:
    """Cluster config from /v1/config."""

    flink_version: str
    timezone: Optional[str] = None


def _is_retryable(exception: BaseException) -> bool:
    """Retry on 5xx and connection errors. Do NOT retry 401/403."""
    if isinstance(exception, requests.exceptions.HTTPError):
        status = exception.response.status_code if exception.response else 0
        return status >= 500
    if isinstance(
        exception,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
    ):
        return True
    return False


class FlinkRestClient:
    """HTTP client for the Flink JobManager REST API v1."""

    def __init__(self, config: FlinkSourceConfig) -> None:
        self.config = config
        self.base_url = config.rest_endpoint
        self.session = requests.Session()
        self.session.verify = config.verify_ssl

        if config.token:
            self.session.headers["Authorization"] = (
                f"Bearer {config.token.get_secret_value()}"
            )
        elif config.username and config.password:
            self.session.auth = HTTPBasicAuth(
                config.username, config.password.get_secret_value()
            )

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get(self, path: str) -> dict:
        """Issue a GET request with retry logic."""
        url = urljoin(f"{self.base_url}/", path.lstrip("/"))
        response = self.session.get(url, timeout=self.config.request_timeout_seconds)
        response.raise_for_status()
        return response.json()

    def get_cluster_config(self) -> FlinkClusterConfig:
        """GET /v1/config — cluster-level configuration."""
        data = self._get("/v1/config")
        return FlinkClusterConfig(
            flink_version=data.get("flink-version", "unknown"),
            timezone=data.get("timezone"),
        )

    def get_jobs_overview(self) -> List[FlinkJobSummary]:
        """GET /v1/jobs/overview — summary of all jobs."""
        data = self._get("/v1/jobs/overview")
        return [
            FlinkJobSummary(
                jid=job["jid"],
                name=job["name"],
                state=job["state"],
                start_time=job["start-time"],
                end_time=job["end-time"],
                duration=job["duration"],
                last_modification=job.get("last-modification", job["start-time"]),
            )
            for job in data.get("jobs", [])
        ]

    def get_job_details(self, job_id: str) -> FlinkJobDetail:
        """GET /v1/jobs/:jobid — full job details with embedded plan."""
        data = self._get(f"/v1/jobs/{job_id}")
        plan_nodes = []
        plan = data.get("plan", {})
        for node in plan.get("nodes", []):
            plan_nodes.append(
                FlinkPlanNode(
                    id=str(node["id"]),
                    description=node.get("description", ""),
                    operator=node.get("operator", ""),
                    parallelism=node.get("parallelism", 1),
                    inputs=node.get("inputs", []),
                )
            )
        return FlinkJobDetail(
            jid=data["jid"],
            name=data["name"],
            state=data["state"],
            start_time=data["start-time"],
            end_time=data["end-time"],
            duration=data["duration"],
            job_type=data.get("job-type"),
            max_parallelism=data.get("maxParallelism"),
            plan_nodes=plan_nodes,
        )

    def get_checkpoint_config(self, job_id: str) -> Optional[FlinkCheckpointConfig]:
        """GET /v1/jobs/:jobid/checkpoints/config — checkpoint settings."""
        try:
            data = self._get(f"/v1/jobs/{job_id}/checkpoints/config")
            return FlinkCheckpointConfig(
                mode=data.get("mode"),
                interval=data.get("interval"),
                timeout=data.get("timeout"),
                externalized_checkpoint_config=data.get("externalization"),
                state_backend=data.get("state_backend"),
                checkpoint_storage=data.get("checkpoint_storage"),
            )
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                logger.debug("No checkpoint config for job %s (may be batch)", job_id)
                return None
            raise

    def test_connectivity(self) -> None:
        """Verify REST API reachability and authentication. Raises on failure."""
        self._get("/v1/config")
