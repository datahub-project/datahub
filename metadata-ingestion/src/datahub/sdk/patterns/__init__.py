"""Reusable platform coordination patterns built on the DataHub SDK.

Typical usage::

    from datahub.sdk.patterns.job_queue import JobQueue, Claim, SearchDiscovery

    queue = JobQueue(
        discovery=SearchDiscovery(graph=graph, entity_type="dataHubAction", filters={"state": ["ACTIVE"]}),
        claim=Claim(graph=graph, aspect_name="dataHubActionStatus", ...),
        owner_id=instance_id,
    )

    for job in queue.poll():
        try:
            process(job.urn)
        finally:
            job.release()
"""

from datahub.sdk.patterns._shared.conditional_writer import (
    CASResult,
    ConditionalWriter,
    VersionedAspect,
)
from datahub.sdk.patterns.job_queue.claim import Claim
from datahub.sdk.patterns.job_queue.discovery import (
    Discovery,
    MCLDiscovery,
    SearchDiscovery,
    WorkItem,
)
from datahub.sdk.patterns.job_queue.job_queue import Job, JobQueue
from datahub.sdk.patterns.job_queue.sweeper import Sweeper, SweepResult

__all__ = [
    "CASResult",
    "Claim",
    "ConditionalWriter",
    "Discovery",
    "Job",
    "JobQueue",
    "MCLDiscovery",
    "SearchDiscovery",
    "SweepResult",
    "Sweeper",
    "VersionedAspect",
    "WorkItem",
]
