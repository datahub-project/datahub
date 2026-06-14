from __future__ import annotations

from unittest.mock import MagicMock

from datahub.sdk.patterns._shared.conditional_writer import VersionedAspect
from datahub.sdk.patterns.job_queue.discovery import WorkItem
from datahub.sdk.patterns.job_queue.job_queue import Job, JobQueue


def _make_discovery(items: list[WorkItem]) -> MagicMock:
    mock = MagicMock()
    mock.poll.return_value = items
    return mock


def _make_claim(success_urns: set[str] | None = None) -> MagicMock:
    """Create a mock Claim that succeeds for URNs in *success_urns*.

    If *success_urns* is None, all try_claim calls succeed.
    """
    mock = MagicMock()

    if success_urns is None:
        mock.try_claim.return_value = True
    else:
        mock.try_claim.side_effect = lambda urn, _, **kw: urn in success_urns

    mock.held_urns = set()
    mock.release.return_value = True
    return mock


# ---------------------------------------------------------------------------
# JobQueue.poll
# ---------------------------------------------------------------------------


class TestJobQueuePoll:
    def test_returns_only_claimed_jobs(self) -> None:
        items = [
            WorkItem(urn="urn:a"),
            WorkItem(urn="urn:b"),
            WorkItem(urn="urn:c"),
        ]
        discovery = _make_discovery(items)
        claim = _make_claim(success_urns={"urn:a", "urn:c"})

        queue = JobQueue(discovery=discovery, claim=claim, owner_id="worker-1")
        jobs = queue.poll()

        assert len(jobs) == 2
        assert {j.urn for j in jobs} == {"urn:a", "urn:c"}
        assert claim.try_claim.call_count == 3

    def test_returns_empty_when_no_candidates(self) -> None:
        discovery = _make_discovery([])
        claim = _make_claim()

        queue = JobQueue(discovery=discovery, claim=claim, owner_id="worker-1")
        jobs = queue.poll()

        assert jobs == []
        assert claim.try_claim.call_count == 0

    def test_returns_empty_when_all_claims_fail(self) -> None:
        items = [WorkItem(urn="urn:a"), WorkItem(urn="urn:b")]
        discovery = _make_discovery(items)
        claim = _make_claim(success_urns=set())

        queue = JobQueue(discovery=discovery, claim=claim, owner_id="worker-1")
        jobs = queue.poll()

        assert jobs == []

    def test_passes_owner_id_and_hint_to_try_claim(self) -> None:
        hint = VersionedAspect(version="3", aspect=MagicMock())
        items = [WorkItem(urn="urn:a", claim_hint=hint)]
        discovery = _make_discovery(items)
        claim = _make_claim()

        queue = JobQueue(discovery=discovery, claim=claim, owner_id="my-instance-42")
        queue.poll()

        claim.try_claim.assert_called_once_with(
            "urn:a", "my-instance-42", claim_hint=hint
        )

    def test_passes_none_hint_for_search_discovery(self) -> None:
        items = [WorkItem(urn="urn:a")]
        discovery = _make_discovery(items)
        claim = _make_claim()

        queue = JobQueue(discovery=discovery, claim=claim, owner_id="worker-1")
        queue.poll()

        claim.try_claim.assert_called_once_with("urn:a", "worker-1", claim_hint=None)

    def test_calls_discovery_poll_once(self) -> None:
        items = [WorkItem(urn="urn:a")]
        discovery = _make_discovery(items)
        claim = _make_claim()

        queue = JobQueue(discovery=discovery, claim=claim, owner_id="worker-1")
        queue.poll()

        discovery.poll.assert_called_once()


# ---------------------------------------------------------------------------
# Job.release
# ---------------------------------------------------------------------------


class TestJobRelease:
    def test_delegates_to_claim(self) -> None:
        claim = _make_claim()
        queue = JobQueue(
            discovery=_make_discovery([]),
            claim=claim,
            owner_id="worker-1",
        )

        job = Job(urn="urn:li:dataset:test", _queue=queue)
        result = job.release()

        assert result is True
        claim.release.assert_called_once_with("urn:li:dataset:test", "worker-1")

    def test_release_returns_false_on_failure(self) -> None:
        claim = _make_claim()
        claim.release.return_value = False
        queue = JobQueue(
            discovery=_make_discovery([]),
            claim=claim,
            owner_id="worker-1",
        )

        job = Job(urn="urn:li:dataset:test", _queue=queue)
        result = job.release()

        assert result is False


# ---------------------------------------------------------------------------
# JobQueue.held_jobs
# ---------------------------------------------------------------------------


class TestJobQueueHeldJobs:
    def test_reflects_claim_held_urns(self) -> None:
        claim = _make_claim()
        claim.held_urns = {"urn:a", "urn:b"}
        queue = JobQueue(
            discovery=_make_discovery([]),
            claim=claim,
            owner_id="worker-1",
        )

        held = queue.held_jobs
        assert len(held) == 2
        assert {j.urn for j in held} == {"urn:a", "urn:b"}

    def test_empty_when_nothing_held(self) -> None:
        claim = _make_claim()
        claim.held_urns = set()
        queue = JobQueue(
            discovery=_make_discovery([]),
            claim=claim,
            owner_id="worker-1",
        )

        assert queue.held_jobs == []

    def test_held_jobs_are_releasable(self) -> None:
        claim = _make_claim()
        claim.held_urns = {"urn:a"}
        queue = JobQueue(
            discovery=_make_discovery([]),
            claim=claim,
            owner_id="worker-1",
        )

        held = queue.held_jobs
        assert len(held) == 1
        held[0].release()
        claim.release.assert_called_once_with("urn:a", "worker-1")


# ---------------------------------------------------------------------------
# JobQueue.release_all
# ---------------------------------------------------------------------------


class TestJobQueueReleaseAll:
    def test_releases_all_held_claims(self) -> None:
        claim = _make_claim()
        claim.held_urns = {"urn:a", "urn:b"}
        queue = JobQueue(
            discovery=_make_discovery([]),
            claim=claim,
            owner_id="worker-1",
        )

        queue.release_all()
        assert claim.release.call_count == 2

    def test_passes_owner_id_to_each_release(self) -> None:
        claim = _make_claim()
        claim.held_urns = {"urn:a"}
        queue = JobQueue(
            discovery=_make_discovery([]),
            claim=claim,
            owner_id="instance-99",
        )

        queue.release_all()
        claim.release.assert_called_once_with("urn:a", "instance-99")

    def test_release_all_with_no_held_urns(self) -> None:
        claim = _make_claim()
        claim.held_urns = set()
        queue = JobQueue(
            discovery=_make_discovery([]),
            claim=claim,
            owner_id="worker-1",
        )

        queue.release_all()
        assert claim.release.call_count == 0


# ---------------------------------------------------------------------------
# Job.__repr__
# ---------------------------------------------------------------------------


class TestJobRepr:
    def test_repr(self) -> None:
        queue = JobQueue(
            discovery=_make_discovery([]),
            claim=_make_claim(),
            owner_id="worker-1",
        )
        job = Job(urn="urn:li:dataset:test", _queue=queue)
        assert repr(job) == "Job(urn='urn:li:dataset:test')"
