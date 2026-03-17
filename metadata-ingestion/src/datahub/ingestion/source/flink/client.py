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

from datahub.ingestion.source.flink.config import FlinkConnectionConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FlinkPlanNode:
    id: str
    description: str
    operator: str
    parallelism: int
    inputs: List[Dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class FlinkJobSummary:
    jid: str
    name: str
    state: str
    start_time: int
    end_time: int
    duration: int
    last_modification: int


@dataclass(frozen=True)
class FlinkJobDetail:
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
    mode: Optional[str]
    interval: Optional[int]
    timeout: Optional[int]
    state_backend: Optional[str]
    checkpoint_storage: Optional[str]
    externalized_enabled: Optional[bool] = None
    externalized_delete_on_cancellation: Optional[bool] = None


@dataclass(frozen=True)
class FlinkClusterConfig:
    flink_version: str
    timezone: Optional[str] = None


def _is_retryable(exception: BaseException) -> bool:
    """Retry on 5xx and connection errors. Fail fast on 4xx (auth/not found)."""
    if isinstance(exception, requests.exceptions.HTTPError):
        status = exception.response.status_code if exception.response else 0
        return status >= 500
    if isinstance(
        exception,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
    ):
        return True
    return False


def create_authenticated_session(config: FlinkConnectionConfig) -> requests.Session:
    session = requests.Session()
    session.verify = config.verify_ssl
    if config.token:
        session.headers["Authorization"] = f"Bearer {config.token.get_secret_value()}"
    elif config.username and config.password:
        session.auth = HTTPBasicAuth(
            config.username, config.password.get_secret_value()
        )
    return session


class FlinkRestClient:
    """HTTP client for the Flink JobManager REST API v1."""

    def __init__(self, config: FlinkConnectionConfig) -> None:
        self.base_url = config.rest_api_url
        self.timeout = config.timeout_seconds
        self.session = create_authenticated_session(config)

        self._get = retry(  # type: ignore[method-assign]  # tenacity wraps method with compatible callable
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(max(1, config.max_retries)),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            reraise=True,
        )(self._get)

    def _get(self, path: str) -> Dict[str, Any]:
        url = urljoin(f"{self.base_url}/", path.lstrip("/"))
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_cluster_config(self) -> FlinkClusterConfig:
        data = self._get("/v1/config")
        return FlinkClusterConfig(
            flink_version=data.get("flink-version", "unknown"),
            timezone=data.get("timezone-name"),
        )

    def get_jobs_overview(self) -> List[FlinkJobSummary]:
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
        data = self._get(f"/v1/jobs/{job_id}")
        plan_nodes = [
            FlinkPlanNode(
                id=str(node["id"]),
                description=node.get("description", ""),
                operator=node.get("operator", ""),
                parallelism=node.get("parallelism", 1),
                inputs=node.get("inputs", []),
            )
            for node in data.get("plan", {}).get("nodes", [])
        ]
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
        try:
            data = self._get(f"/v1/jobs/{job_id}/checkpoints/config")
            ext = data.get("externalization") or {}
            return FlinkCheckpointConfig(
                mode=data.get("mode"),
                interval=data.get("interval"),
                timeout=data.get("timeout"),
                state_backend=data.get("state_backend"),
                checkpoint_storage=data.get("checkpoint_storage"),
                externalized_enabled=ext.get("enabled"),
                externalized_delete_on_cancellation=ext.get("delete_on_cancellation"),
            )
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                logger.debug("No checkpoint config for job %s", job_id)
                return None
            raise

    def test_connectivity(self) -> None:
        self._get("/v1/config")

    def close(self) -> None:
        self.session.close()
