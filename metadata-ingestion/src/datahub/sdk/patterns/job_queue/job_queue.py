"""Layer 3: Caller-facing JobQueue API.

Composes :class:`~datahub.sdk.patterns.job_queue.discovery.Discovery` and
:class:`~datahub.sdk.patterns.job_queue.claim.Claim` into a single API that discovers
work candidates, atomically claims them, and hands back :class:`Job`
handles.
"""

from __future__ import annotations

import logging
from typing import List

from datahub.sdk.patterns.job_queue.claim import Claim
from datahub.sdk.patterns.job_queue.discovery import Discovery

logger = logging.getLogger(__name__)


class Job:
    """Handle to a claimed work item.

    The caller processes the work associated with :attr:`urn` and then
    calls :meth:`release` to relinquish the claim.
    """

    def __init__(self, urn: str, _queue: JobQueue) -> None:
        self.urn = urn
        self._queue = _queue

    def release(self) -> bool:
        """Release this job's claim.

        Returns:
            ``True`` if the release succeeded, ``False`` if the version
            had changed (e.g. someone else force-released it).
        """
        return self._queue._claim.release(self.urn, self._queue._owner_id)

    def __repr__(self) -> str:
        return f"Job(urn={self.urn!r})"


class JobQueue:
    """Discover work, atomically claim it, and hand back Job handles.

    Args:
        discovery: Strategy for finding work candidates.
        claim: Claim instance configured for the target entity/aspect.
        owner_id: Unique identifier for this worker instance.
    """

    def __init__(
        self,
        discovery: Discovery,
        claim: Claim,
        owner_id: str,
    ) -> None:
        self._discovery = discovery
        self._claim = claim
        self._owner_id = owner_id

    def poll(self) -> List[Job]:
        """Discover work candidates, attempt to claim each one.

        Returns only successfully claimed jobs.  Discovery hints (if any)
        are forwarded to :meth:`Claim.try_claim` to reduce round trips.
        """
        work_items = self._discovery.poll()
        claimed: List[Job] = []
        for item in work_items:
            if self._claim.try_claim(
                item.urn, self._owner_id, claim_hint=item.claim_hint
            ):
                claimed.append(Job(urn=item.urn, _queue=self))
        logger.debug(
            "Polled %d candidates, claimed %d",
            len(work_items),
            len(claimed),
        )
        return claimed

    @property
    def held_jobs(self) -> List[Job]:
        """All jobs currently claimed by this instance."""
        return [Job(urn=urn, _queue=self) for urn in self._claim.held_urns]

    def release_all(self) -> None:
        """Release all held claims. Call on shutdown."""
        for urn in list(self._claim.held_urns):
            self._claim.release(urn, self._owner_id)
