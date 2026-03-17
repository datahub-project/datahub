from typing import FrozenSet, List

# All valid Flink job states (from Flink REST API /v1/jobs/overview)
VALID_FLINK_JOB_STATES: FrozenSet[str] = frozenset(
    {
        "INITIALIZING",
        "CREATED",
        "RUNNING",
        "FAILING",
        "FAILED",
        "CANCELLING",
        "CANCELED",
        "FINISHED",
        "RESTARTING",
        "SUSPENDED",
        "RECONCILING",
    }
)

DEFAULT_INCLUDE_JOB_STATES: List[str] = ["RUNNING", "FINISHED", "FAILED", "CANCELED"]
