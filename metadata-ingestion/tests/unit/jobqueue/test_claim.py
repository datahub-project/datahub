from __future__ import annotations

from unittest.mock import MagicMock, patch

from datahub.sdk.patterns._shared.conditional_writer import CASResult, VersionedAspect
from datahub.sdk.patterns.job_queue.claim import Claim, _HeldClaim


def _make_graph() -> MagicMock:
    return MagicMock()


def _make_aspect_class() -> MagicMock:
    cls = MagicMock()
    cls.ASPECT_NAME = "testAspect"
    return cls


def _make_claim(
    make_claimed: MagicMock | None = None,
    make_released: MagicMock | None = None,
) -> Claim:
    return Claim(
        graph=_make_graph(),
        aspect_name="testAspect",
        aspect_class=_make_aspect_class(),
        make_claimed=make_claimed or MagicMock(return_value=MagicMock()),
        make_released=make_released or MagicMock(return_value=MagicMock()),
        is_claimed=lambda _: False,
    )


# ---------------------------------------------------------------------------
# Claim.try_claim
# ---------------------------------------------------------------------------


class TestClaimTryClaim:
    def test_success_tracks_version_in_held_urns(self) -> None:
        make_claimed = MagicMock(return_value=MagicMock())
        claim = _make_claim(make_claimed=make_claimed)

        with (
            patch.object(
                claim._writer,
                "read_versioned_aspect",
                return_value=VersionedAspect(version="-1", aspect=None),
            ),
            patch.object(
                claim._writer,
                "write_if_version",
                return_value=CASResult(success=True, new_version="1"),
            ),
        ):
            result = claim.try_claim("urn:li:dataset:test", "worker-1")

        assert result is True
        assert "urn:li:dataset:test" in claim.held_urns
        # make_claimed now receives (owner_id, current_aspect)
        make_claimed.assert_called()
        args = make_claimed.call_args_list[0][0]
        assert args[0] == "worker-1"

    def test_conflict_does_not_track(self) -> None:
        make_claimed = MagicMock(return_value=MagicMock())
        claim = _make_claim(make_claimed=make_claimed)

        with (
            patch.object(
                claim._writer,
                "read_versioned_aspect",
                return_value=VersionedAspect(version="-1", aspect=None),
            ),
            patch.object(
                claim._writer,
                "write_if_version",
                return_value=CASResult(success=False, reason="conflict"),
            ),
        ):
            result = claim.try_claim("urn:li:dataset:test", "worker-1")

        assert result is False
        assert "urn:li:dataset:test" not in claim.held_urns

    def test_multiple_urns_tracked_independently(self) -> None:
        claim = _make_claim()

        with (
            patch.object(
                claim._writer,
                "read_versioned_aspect",
                return_value=VersionedAspect(version="-1", aspect=None),
            ),
            patch.object(
                claim._writer,
                "write_if_version",
                return_value=CASResult(success=True, new_version="1"),
            ),
        ):
            claim.try_claim("urn:li:dataset:a", "worker-1")
            claim.try_claim("urn:li:dataset:b", "worker-1")

        assert claim.held_urns == {"urn:li:dataset:a", "urn:li:dataset:b"}

    def test_passes_read_version_to_write(self) -> None:
        """Verifies the version from read_versioned_aspect flows into write_if_version."""
        claim = _make_claim()

        with (
            patch.object(
                claim._writer,
                "read_versioned_aspect",
                return_value=VersionedAspect(version="42", aspect=None),
            ),
            patch.object(
                claim._writer,
                "write_if_version",
                return_value=CASResult(success=True, new_version="43"),
            ) as mock_write,
        ):
            claim.try_claim("urn:li:dataset:test", "worker-1")

        mock_write.assert_called_once()
        call_args = mock_write.call_args[0]
        assert call_args[0] == "urn:li:dataset:test"
        assert call_args[1] == "testAspect"
        assert call_args[3] == "42"  # expected_version

    def test_claim_hint_skips_read(self) -> None:
        """When a valid claim_hint is provided, read_versioned_aspect is not called."""
        from datahub.metadata.schema_classes import DatasetPropertiesClass

        claim = Claim(
            graph=_make_graph(),
            aspect_name="datasetProperties",
            aspect_class=DatasetPropertiesClass,
            make_claimed=MagicMock(return_value=MagicMock()),
            make_released=MagicMock(return_value=MagicMock()),
            is_claimed=lambda _: False,
        )

        hint_aspect = DatasetPropertiesClass()
        hint = VersionedAspect(version="7", aspect=hint_aspect)

        with (
            patch.object(claim._writer, "read_versioned_aspect") as mock_read,
            patch.object(
                claim._writer,
                "write_if_version",
                return_value=CASResult(success=True, new_version="8"),
            ),
        ):
            result = claim.try_claim("urn:li:dataset:test", "worker-1", claim_hint=hint)

        assert result is True
        mock_read.assert_not_called()

    def test_wrong_type_hint_falls_back_to_read(self) -> None:
        """When the hint aspect type doesn't match, falls back to read."""
        from datahub.metadata.schema_classes import (
            DatasetPropertiesClass,
            OwnershipClass,
        )

        claim = Claim(
            graph=_make_graph(),
            aspect_name="ownership",
            aspect_class=OwnershipClass,
            make_claimed=MagicMock(return_value=MagicMock()),
            make_released=MagicMock(return_value=MagicMock()),
            is_claimed=lambda _: False,
        )

        # Hint has a DatasetPropertiesClass but claim expects OwnershipClass
        hint = VersionedAspect(version="7", aspect=DatasetPropertiesClass())

        with (
            patch.object(
                claim._writer,
                "read_versioned_aspect",
                return_value=VersionedAspect(version="7", aspect=None),
            ) as mock_read,
            patch.object(
                claim._writer,
                "write_if_version",
                return_value=CASResult(success=True, new_version="8"),
            ),
        ):
            result = claim.try_claim("urn:li:dataset:test", "worker-1", claim_hint=hint)

        assert result is True
        mock_read.assert_called_once()

    def test_none_hint_aspect_falls_back_to_read(self) -> None:
        """When claim_hint.aspect is None, falls back to read."""
        claim = _make_claim()
        hint = VersionedAspect(version="7", aspect=None)

        with (
            patch.object(
                claim._writer,
                "read_versioned_aspect",
                return_value=VersionedAspect(version="7", aspect=None),
            ) as mock_read,
            patch.object(
                claim._writer,
                "write_if_version",
                return_value=CASResult(success=True, new_version="8"),
            ),
        ):
            claim.try_claim("urn:li:dataset:test", "worker-1", claim_hint=hint)

        mock_read.assert_called_once()


# ---------------------------------------------------------------------------
# Claim.release
# ---------------------------------------------------------------------------


class TestClaimRelease:
    def test_release_with_cached_aspect(self) -> None:
        """Release uses the cached aspect and passes track_new_version=False."""
        make_released = MagicMock(return_value=MagicMock())
        claim = _make_claim(make_released=make_released)

        cached_aspect = MagicMock()
        claim._held["urn:li:dataset:test"] = _HeldClaim(
            version="5", aspect=cached_aspect
        )

        with patch.object(
            claim._writer,
            "write_if_version",
            return_value=CASResult(success=True),
        ) as mock_write:
            result = claim.release("urn:li:dataset:test", "worker-1")

        assert result is True
        assert "urn:li:dataset:test" not in claim.held_urns
        # make_released receives the cached aspect
        make_released.assert_called_once_with("worker-1", cached_aspect)
        # write_if_version is called with track_new_version=False
        mock_write.assert_called_once()
        assert mock_write.call_args[0][3] == "5"  # expected_version
        assert mock_write.call_args[1]["track_new_version"] is False

    def test_release_no_read_calls(self) -> None:
        """Release should not call read_aspect or read_versioned_aspect."""
        claim = _make_claim()
        claim._held["urn:li:dataset:test"] = _HeldClaim(version="5", aspect=MagicMock())

        with (
            patch.object(claim._writer, "read_aspect") as mock_read_aspect,
            patch.object(claim._writer, "read_versioned_aspect") as mock_read_versioned,
            patch.object(
                claim._writer,
                "write_if_version",
                return_value=CASResult(success=True),
            ),
        ):
            claim.release("urn:li:dataset:test", "worker-1")

        mock_read_aspect.assert_not_called()
        mock_read_versioned.assert_not_called()

    def test_release_returns_false_for_unknown_urn(self) -> None:
        claim = _make_claim()

        result = claim.release("urn:li:dataset:unknown", "worker-1")

        assert result is False

    def test_release_conflict_returns_false(self) -> None:
        claim = _make_claim()
        claim._held["urn:li:dataset:test"] = _HeldClaim(version="5", aspect=MagicMock())

        with patch.object(
            claim._writer,
            "write_if_version",
            return_value=CASResult(success=False, reason="conflict"),
        ):
            result = claim.release("urn:li:dataset:test", "worker-1")

        assert result is False
        # URN should be removed from held even on conflict (pop semantics).
        assert "urn:li:dataset:test" not in claim.held_urns

    def test_release_removes_urn_before_write(self) -> None:
        """Ensures the URN is popped before writing (not after), so a failed
        release doesn't leave stale state."""
        claim = _make_claim()
        claim._held["urn:li:dataset:test"] = _HeldClaim(version="5", aspect=MagicMock())

        with patch.object(
            claim._writer,
            "write_if_version",
            side_effect=RuntimeError("unexpected"),
        ):
            try:
                claim.release("urn:li:dataset:test", "worker-1")
            except RuntimeError:
                pass

        # URN should have been popped even though the write threw.
        assert "urn:li:dataset:test" not in claim.held_urns


# ---------------------------------------------------------------------------
# Claim.held_urns
# ---------------------------------------------------------------------------


class TestClaimHeldUrns:
    def test_empty_initially(self) -> None:
        claim = _make_claim()
        assert claim.held_urns == set()

    def test_reflects_held_state(self) -> None:
        claim = _make_claim()
        claim._held["urn:li:dataset:a"] = _HeldClaim(version="1", aspect=None)
        claim._held["urn:li:dataset:b"] = _HeldClaim(version="2", aspect=None)
        assert claim.held_urns == {"urn:li:dataset:a", "urn:li:dataset:b"}

    def test_returns_copy_not_reference(self) -> None:
        """Modifications to the returned set do not affect internal state."""
        claim = _make_claim()
        claim._held["urn:li:dataset:a"] = _HeldClaim(version="1", aspect=None)
        held = claim.held_urns
        held.add("urn:li:dataset:injected")
        assert "urn:li:dataset:injected" not in claim.held_urns


# ---------------------------------------------------------------------------
# Claim.from_fields
# ---------------------------------------------------------------------------


class TestClaimFromFields:
    def test_generates_correct_claimed_aspect(self) -> None:
        graph = _make_graph()
        aspect_class = MagicMock()
        aspect_class.ASPECT_NAME = "testAspect"
        instance = MagicMock()
        aspect_class.return_value = instance

        claim = Claim.from_fields(
            graph=graph,
            aspect_name="testAspect",
            aspect_class=aspect_class,
            owner_field="ownerId",
            state_field="status",
            claimed_state="RUNNING",
            released_state="SUCCESS",
            timestamp_field="startTimeMs",
            extra_claimed_fields={"priority": "HIGH"},
        )

        result = claim._make_claimed("worker-42", None)
        assert result is instance
        call_kwargs = aspect_class.call_args[1]
        assert call_kwargs["ownerId"] == "worker-42"
        assert call_kwargs["status"] == "RUNNING"
        assert "startTimeMs" in call_kwargs
        assert isinstance(call_kwargs["startTimeMs"], int)
        assert call_kwargs["priority"] == "HIGH"

    def test_generates_correct_released_aspect(self) -> None:
        graph = _make_graph()
        aspect_class = MagicMock()
        aspect_class.ASPECT_NAME = "testAspect"
        instance = MagicMock()
        aspect_class.return_value = instance

        claim = Claim.from_fields(
            graph=graph,
            aspect_name="testAspect",
            aspect_class=aspect_class,
            owner_field="ownerId",
            state_field="status",
            claimed_state="RUNNING",
            released_state="SUCCESS",
            extra_released_fields={"reason": "completed"},
        )

        result = claim._make_released("worker-42", None)
        assert result is instance
        call_kwargs = aspect_class.call_args[1]
        assert call_kwargs["ownerId"] == "worker-42"
        assert call_kwargs["status"] == "SUCCESS"
        assert call_kwargs["reason"] == "completed"

    def test_without_timestamp_field(self) -> None:
        graph = _make_graph()
        aspect_class = MagicMock()
        aspect_class.ASPECT_NAME = "testAspect"

        claim = Claim.from_fields(
            graph=graph,
            aspect_name="testAspect",
            aspect_class=aspect_class,
            owner_field="ownerId",
            state_field="status",
            claimed_state="RUNNING",
            released_state="SUCCESS",
        )

        claim._make_claimed("worker-1", None)
        call_kwargs = aspect_class.call_args[1]
        assert "startTimeMs" not in call_kwargs
        assert set(call_kwargs.keys()) == {"ownerId", "status"}

    def test_without_extra_fields(self) -> None:
        graph = _make_graph()
        aspect_class = MagicMock()
        aspect_class.ASPECT_NAME = "testAspect"

        claim = Claim.from_fields(
            graph=graph,
            aspect_name="testAspect",
            aspect_class=aspect_class,
            owner_field="ownerId",
            state_field="status",
            claimed_state="RUNNING",
            released_state="SUCCESS",
            timestamp_field="ts",
        )

        claim._make_claimed("w1", None)
        claimed_kwargs = aspect_class.call_args[1]
        assert set(claimed_kwargs.keys()) == {"ownerId", "status", "ts"}

        claim._make_released("w1", None)
        released_kwargs = aspect_class.call_args[1]
        assert set(released_kwargs.keys()) == {"ownerId", "status", "ts"}

    def test_extra_fields_on_both_claimed_and_released(self) -> None:
        graph = _make_graph()
        aspect_class = MagicMock()
        aspect_class.ASPECT_NAME = "testAspect"

        claim = Claim.from_fields(
            graph=graph,
            aspect_name="testAspect",
            aspect_class=aspect_class,
            owner_field="ownerId",
            state_field="status",
            claimed_state="RUNNING",
            released_state="SUCCESS",
            extra_claimed_fields={"priority": "HIGH", "retries": 0},
            extra_released_fields={"endReason": "done"},
        )

        claim._make_claimed("w1", None)
        claimed_kwargs = aspect_class.call_args[1]
        assert claimed_kwargs["priority"] == "HIGH"
        assert claimed_kwargs["retries"] == 0

        claim._make_released("w1", None)
        released_kwargs = aspect_class.call_args[1]
        assert released_kwargs["endReason"] == "done"
        assert "priority" not in released_kwargs

    def test_returns_claim_instance(self) -> None:
        claim = Claim.from_fields(
            graph=_make_graph(),
            aspect_name="testAspect",
            aspect_class=MagicMock(),
            owner_field="ownerId",
            state_field="status",
            claimed_state="RUNNING",
            released_state="SUCCESS",
        )
        assert isinstance(claim, Claim)
