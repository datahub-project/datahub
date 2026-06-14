from __future__ import annotations

from typing import Any, Callable, Optional
from unittest.mock import MagicMock, patch

from datahub._codegen.aspect import _Aspect
from datahub.sdk.patterns._shared.conditional_writer import CASResult, VersionedAspect
from datahub.sdk.patterns.job_queue.discovery import (
    Discovery,
    SearchDiscovery,
    WorkItem,
)
from datahub.sdk.patterns.job_queue.sweeper import Sweeper, SweepResult


def _make_graph() -> MagicMock:
    return MagicMock()


def _make_aspect(**fields: object) -> MagicMock:
    aspect = MagicMock(spec=_Aspect)
    aspect._inner_dict = dict(fields)
    for k, v in fields.items():
        setattr(aspect, k, v)
    return aspect


def _make_sweeper(
    is_stale: Optional[Callable[[Any], bool]] = None,
    make_swept: Optional[Callable[[Any], Any]] = None,
    discovery: Optional[Discovery] = None,
) -> Sweeper[Any]:
    return Sweeper(
        graph=_make_graph(),
        aspect_name="testAspect",
        aspect_class=_Aspect,
        is_stale=is_stale or (lambda _: True),
        make_swept=make_swept or (lambda current: MagicMock(spec=_Aspect)),
        discovery=discovery or MagicMock(poll=MagicMock(return_value=[])),
    )


# ---------------------------------------------------------------------------
# Sweeper.sweep
# ---------------------------------------------------------------------------


class TestSweeperSweep:
    def test_sweeps_stale_claims(self) -> None:
        """Discovery returns 2 URNs, both stale → both swept."""
        discovery = MagicMock()
        discovery.poll.return_value = [
            WorkItem(urn="urn:li:dataset:a"),
            WorkItem(urn="urn:li:dataset:b"),
        ]
        sweeper = _make_sweeper(discovery=discovery)

        with (
            patch.object(
                sweeper._writer,
                "read_versioned_aspect",
                return_value=VersionedAspect(version="5", aspect=_make_aspect()),
            ),
            patch.object(
                sweeper._writer,
                "write_if_version",
                return_value=CASResult(success=True),
            ),
        ):
            result = sweeper.sweep()

        assert result.swept == ["urn:li:dataset:a", "urn:li:dataset:b"]
        assert result.failed == []
        assert result.skipped == []

    def test_skips_non_stale(self) -> None:
        """Discovery returns 2 URNs, is_stale returns False for one."""
        discovery = MagicMock()
        discovery.poll.return_value = [
            WorkItem(urn="urn:li:dataset:stale"),
            WorkItem(urn="urn:li:dataset:fresh"),
        ]

        stale_aspect = _make_aspect(status="RUNNING")
        fresh_aspect = _make_aspect(status="RUNNING")

        call_count = 0

        def _is_stale(aspect: object) -> bool:
            nonlocal call_count
            call_count += 1
            return call_count == 1  # First call stale, second not

        sweeper = _make_sweeper(is_stale=_is_stale, discovery=discovery)

        with (
            patch.object(
                sweeper._writer,
                "read_versioned_aspect",
                side_effect=[
                    VersionedAspect(version="5", aspect=stale_aspect),
                    VersionedAspect(version="6", aspect=fresh_aspect),
                ],
            ),
            patch.object(
                sweeper._writer,
                "write_if_version",
                return_value=CASResult(success=True),
            ),
        ):
            result = sweeper.sweep()

        assert result.swept == ["urn:li:dataset:stale"]
        assert result.skipped == ["urn:li:dataset:fresh"]
        assert result.failed == []

    def test_handles_cas_conflict(self) -> None:
        """CAS write fails → counted as failed, not exception."""
        discovery = MagicMock()
        discovery.poll.return_value = [WorkItem(urn="urn:li:dataset:a")]
        sweeper = _make_sweeper(discovery=discovery)

        with (
            patch.object(
                sweeper._writer,
                "read_versioned_aspect",
                return_value=VersionedAspect(version="5", aspect=_make_aspect()),
            ),
            patch.object(
                sweeper._writer,
                "write_if_version",
                return_value=CASResult(success=False, reason="conflict"),
            ),
        ):
            result = sweeper.sweep()

        assert result.swept == []
        assert result.failed == ["urn:li:dataset:a"]
        assert result.skipped == []

    def test_handles_missing_aspect(self) -> None:
        """read_versioned_aspect returns aspect=None → skipped."""
        discovery = MagicMock()
        discovery.poll.return_value = [WorkItem(urn="urn:li:dataset:a")]
        sweeper = _make_sweeper(discovery=discovery)

        with patch.object(
            sweeper._writer,
            "read_versioned_aspect",
            return_value=VersionedAspect(version="-1", aspect=None),
        ):
            result = sweeper.sweep()

        assert result.swept == []
        assert result.failed == []
        assert result.skipped == ["urn:li:dataset:a"]

    def test_handles_exception_per_urn(self) -> None:
        """One URN throws exception during read, other succeeds."""
        discovery = MagicMock()
        discovery.poll.return_value = [
            WorkItem(urn="urn:li:dataset:broken"),
            WorkItem(urn="urn:li:dataset:ok"),
        ]
        sweeper = _make_sweeper(discovery=discovery)

        with (
            patch.object(
                sweeper._writer,
                "read_versioned_aspect",
                side_effect=[
                    RuntimeError("connection lost"),
                    VersionedAspect(version="5", aspect=_make_aspect()),
                ],
            ),
            patch.object(
                sweeper._writer,
                "write_if_version",
                return_value=CASResult(success=True),
            ),
        ):
            result = sweeper.sweep()

        assert result.swept == ["urn:li:dataset:ok"]
        assert result.failed == ["urn:li:dataset:broken"]
        assert result.skipped == []

    def test_empty_discovery(self) -> None:
        """No candidates → empty SweepResult."""
        discovery = MagicMock()
        discovery.poll.return_value = []
        sweeper = _make_sweeper(discovery=discovery)

        result = sweeper.sweep()

        assert result == SweepResult(swept=[], failed=[], skipped=[])

    def test_uses_track_new_version_false(self) -> None:
        """Verify CAS write called with track_new_version=False."""
        discovery = MagicMock()
        discovery.poll.return_value = [WorkItem(urn="urn:li:dataset:a")]
        sweeper = _make_sweeper(discovery=discovery)

        with (
            patch.object(
                sweeper._writer,
                "read_versioned_aspect",
                return_value=VersionedAspect(version="5", aspect=_make_aspect()),
            ),
            patch.object(
                sweeper._writer,
                "write_if_version",
                return_value=CASResult(success=True),
            ) as mock_write,
        ):
            sweeper.sweep()

        mock_write.assert_called_once()
        assert mock_write.call_args[1]["track_new_version"] is False


# ---------------------------------------------------------------------------
# Sweeper.from_fields
# ---------------------------------------------------------------------------


class TestSweeperFromFields:
    def _make_from_fields(self, **overrides: object) -> Sweeper:
        defaults = dict(
            graph=_make_graph(),
            aspect_name="testAspect",
            aspect_class=MagicMock(),
            entity_type="workflowTask",
            owner_field="workerId",
            state_field="status",
            claimed_state="RUNNING",
            swept_state="TIMED_OUT",
            timestamp_field="startTimeMs",
            timeout_ms=3600_000,
        )
        defaults.update(overrides)
        return Sweeper.from_fields(**defaults)  # type: ignore[arg-type]

    def test_is_stale_checks_timestamp(self) -> None:
        """Aspect with old timestamp → stale; recent timestamp → not stale."""
        sweeper = self._make_from_fields(timeout_ms=1000)

        old_aspect = _make_aspect(status="RUNNING", startTimeMs=0)
        assert sweeper._is_stale(old_aspect) is True

        import time

        recent_ts = int(time.time() * 1000)
        recent_aspect = _make_aspect(status="RUNNING", startTimeMs=recent_ts)
        assert sweeper._is_stale(recent_aspect) is False

    def test_is_stale_checks_state(self) -> None:
        """Aspect in released state → not stale even if timestamp is old."""
        sweeper = self._make_from_fields(timeout_ms=1000)

        released_aspect = _make_aspect(status="COMPLETED", startTimeMs=0)
        assert sweeper._is_stale(released_aspect) is False

    def test_is_stale_no_timestamp_is_stale(self) -> None:
        """Aspect with None timestamp → stale."""
        sweeper = self._make_from_fields(timeout_ms=1000)

        aspect = _make_aspect(status="RUNNING", startTimeMs=None)
        # getattr returns None for the timestamp → treated as stale
        assert sweeper._is_stale(aspect) is True

    def test_make_swept_sets_fields(self) -> None:
        """Verify swept aspect has correct state and timestamp, preserves other fields."""
        aspect_class = MagicMock()
        instance = MagicMock()
        aspect_class.return_value = instance

        sweeper = self._make_from_fields(aspect_class=aspect_class)

        current = _make_aspect(
            status="RUNNING", workerId="w1", startTimeMs=100, extra="keep"
        )
        result = sweeper._make_swept(current)

        assert result is instance
        call_kwargs = aspect_class.call_args[1]
        assert call_kwargs["status"] == "TIMED_OUT"
        assert isinstance(call_kwargs["startTimeMs"], int)
        assert call_kwargs["startTimeMs"] > 100
        assert call_kwargs["extra"] == "keep"
        assert call_kwargs["workerId"] == "w1"

    def test_make_swept_extra_fields(self) -> None:
        """Extra swept fields are set."""
        aspect_class = MagicMock()
        aspect_class.return_value = MagicMock()

        sweeper = self._make_from_fields(
            aspect_class=aspect_class,
            extra_swept_fields={"reason": "timeout", "retries": 0},
        )

        current = _make_aspect(status="RUNNING", startTimeMs=100)
        sweeper._make_swept(current)

        call_kwargs = aspect_class.call_args[1]
        assert call_kwargs["reason"] == "timeout"
        assert call_kwargs["retries"] == 0

    def test_creates_search_discovery(self) -> None:
        """Verify the SearchDiscovery is created with correct filters."""
        sweeper = self._make_from_fields()

        assert isinstance(sweeper._discovery, SearchDiscovery)
        assert sweeper._discovery._filters == {"status": ["RUNNING"]}
        assert sweeper._discovery._entity_type == "workflowTask"

    def test_returns_sweeper_instance(self) -> None:
        """Verify return type."""
        sweeper = self._make_from_fields()
        assert isinstance(sweeper, Sweeper)
