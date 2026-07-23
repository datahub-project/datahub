from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel


def _client(config: Any) -> Any:
    # Lazy import: client.py pulls in requests/tenacity, which ship only with the
    # `flink` extra. Reusing the connector's own client factory keeps auth/SSL/retry
    # behaviour identical to a run.
    from datahub.ingestion.source.flink.client import get_flink_client

    return get_flink_client(config)


def _jobs(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return sorted({job.name for job in client.get_jobs_overview()})


# Flink is a flat job namespace filtered by the connector's own job_name_pattern (its
# ingestion also filters by include_job_states, which the probe does not reproduce
# since job state is not knowable structurally). Each job is emitted as a plain
# DataFlow with no subtype (see entities.py) and no shared-subtype member names the
# concept — so the probe uses the plain, honest kind label "Flink Job".
FLINK_PROBE = ClientProbe(
    client_factory=_client,
    close=lambda client: client.close(),
    levels=[ProbeLevel("Flink Job", "job_name_pattern", _jobs)],
)

FLINK_PROBE_HIERARCHY: List[ProbeNodeKind] = FLINK_PROBE.hierarchy()


def list_flink_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    return FLINK_PROBE.list_children(config, parent_path, limit)
