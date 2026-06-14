"""Layer 2a: Atomic claim and release of a single entity.

A ``Claim`` reads the current aspect version, then CAS-writes a "claimed"
or "released" value.  It internally tracks held versions so that releases
use the exact version obtained at claim time.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Set, Type

from datahub._codegen.aspect import _Aspect
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.patterns._shared.conditional_writer import (
    ConditionalWriter,
    VersionedAspect,
)

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    """Return current time in milliseconds since the epoch."""
    return int(time.time() * 1000)


@dataclass
class _HeldClaim:
    """Internal bookkeeping for a held claim."""

    version: str
    aspect: Optional[_Aspect]


class Claim:
    """Atomic claim and release of a single entity via CAS.

    Args:
        graph: DataHub graph client.
        aspect_name: The aspect used for claiming (e.g. ``"dataHubExecutionRequestResult"``).
        aspect_class: The aspect's Python class.
        make_claimed: Callable that takes ``(owner_id, current_aspect)`` and
            returns the aspect value representing "claimed by owner".  The
            *current_aspect* is the existing aspect value (``None`` if absent),
            allowing the callable to preserve fields it does not own.
        make_released: Callable that takes ``(owner_id, current_aspect)`` and
            returns the aspect value representing "released by owner".  The
            *current_aspect* is the existing aspect value (``None`` if absent).
        is_claimed: Callable that inspects an existing aspect and returns ``True``
            if it is already in "claimed" state.  Used to prevent sequential
            overwrites — without this, a second caller could read the latest
            version and CAS-write over a legitimate claim.
    """

    def __init__(
        self,
        graph: DataHubGraph,
        aspect_name: str,
        aspect_class: Type[_Aspect],
        make_claimed: Callable[[str, Optional[_Aspect]], _Aspect],
        make_released: Callable[[str, Optional[_Aspect]], _Aspect],
        is_claimed: Callable[[_Aspect], bool],
        jitter_max_ms: float = 0.0,
    ) -> None:
        self._writer = ConditionalWriter(graph)
        self._aspect_name = aspect_name
        self._aspect_class = aspect_class
        self._make_claimed = make_claimed
        self._make_released = make_released
        self._is_claimed = is_claimed
        self._jitter_max_ms = jitter_max_ms
        self._held: Dict[str, _HeldClaim] = {}  # urn -> held claim info

    def try_claim(
        self,
        urn: str,
        owner_id: str,
        claim_hint: Optional[VersionedAspect] = None,
    ) -> bool:
        """Attempt to claim an entity.

        If *claim_hint* is provided and its aspect type matches the claim
        aspect class, the initial read is skipped (saving one round trip).

        Reads the current aspect value and version.  If the aspect already
        indicates a claim by another owner, returns ``False`` immediately.
        Otherwise CAS-writes the claimed value.

        Tracks the new version internally on success (used by :meth:`release`).

        Returns:
            ``True`` if the claim succeeded, ``False`` if already claimed or
            lost to another claimant during CAS.
        """
        # Draw jitter once, sleep before each HTTP step to maintain spacing
        # throughout the multi-step claim rather than only at the start.
        if self._jitter_max_ms > 0:
            delay_sec = random.uniform(0, self._jitter_max_ms) / 1000
            logger.debug("Jitter delay %.1fms per step for %s", delay_sec * 1000, urn)
        else:
            delay_sec = 0.0

        # Try to use the hint to skip the read round trip.
        if (
            claim_hint is not None
            and claim_hint.aspect is not None
            and isinstance(claim_hint.aspect, self._aspect_class)
        ):
            version = claim_hint.version
            current: Optional[_Aspect] = claim_hint.aspect
        else:
            if delay_sec > 0:
                time.sleep(delay_sec)
            va = self._writer.read_versioned_aspect(
                urn, self._aspect_name, self._aspect_class
            )
            version = va.version
            current = va.aspect

        # Check if already claimed.
        if current is not None and self._is_claimed(current):
            logger.debug("Entity %s is already claimed, skipping CAS", urn)
            return False

        if delay_sec > 0:
            time.sleep(delay_sec)
        result = self._writer.write_if_version(
            urn,
            self._aspect_name,
            self._make_claimed(owner_id, current),
            version,
        )
        if result.success:
            self._held[urn] = _HeldClaim(
                version=result.new_version,  # type: ignore[arg-type]
                aspect=self._make_claimed(owner_id, current),
            )
            logger.debug("Claimed %s (version %s)", urn, result.new_version)
        else:
            logger.debug("Failed to claim %s: %s", urn, result.reason)
        return result.success

    def release(self, urn: str, owner_id: str) -> bool:
        """Release a previously claimed entity.

        Uses the tracked version and cached aspect from :meth:`try_claim`
        to CAS-write the released value without additional reads.  If the
        version has changed (e.g. someone else force-released it), this
        returns ``False``.

        Returns:
            ``True`` if the release succeeded.
        """
        held = self._held.pop(urn, None)
        if held is None:
            logger.warning("release() called for %s which is not held", urn)
            return False
        result = self._writer.write_if_version(
            urn,
            self._aspect_name,
            self._make_released(owner_id, held.aspect),
            held.version,
            track_new_version=False,
        )
        if result.success:
            logger.debug("Released %s", urn)
        else:
            logger.debug("Failed to release %s: %s", urn, result.reason)
        return result.success

    @property
    def held_urns(self) -> Set[str]:
        """URNs currently claimed by this instance."""
        return set(self._held.keys())

    @classmethod
    def from_fields(
        cls,
        graph: DataHubGraph,
        aspect_name: str,
        aspect_class: Type[_Aspect],
        owner_field: str,
        state_field: str,
        claimed_state: str,
        released_state: str,
        timestamp_field: Optional[str] = None,
        extra_claimed_fields: Optional[Dict[str, object]] = None,
        extra_released_fields: Optional[Dict[str, object]] = None,
        jitter_max_ms: float = 0.0,
    ) -> Claim:
        """Build a ``Claim`` from field names instead of lambdas.

        Generates ``make_claimed`` / ``make_released`` / ``is_claimed``
        automatically.  When a *current* aspect is available, all existing
        fields are preserved and only the claim-managed fields are
        overwritten.  When *current* is ``None`` (aspect does not exist
        yet), a new aspect is constructed from scratch.

        Use the lambda constructor for aspects with nested structures or
        complex initialization logic.
        """

        def _make_claimed(owner_id: str, current: Optional[_Aspect]) -> _Aspect:
            fields: Dict[str, object] = (
                dict(current._inner_dict) if current is not None else {}
            )
            fields[owner_field] = owner_id
            fields[state_field] = claimed_state
            if timestamp_field:
                fields[timestamp_field] = _now_ms()
            if extra_claimed_fields:
                fields.update(extra_claimed_fields)
            return aspect_class(**fields)  # type: ignore[call-arg]

        def _make_released(owner_id: str, current: Optional[_Aspect]) -> _Aspect:
            fields: Dict[str, object] = (
                dict(current._inner_dict) if current is not None else {}
            )
            fields[owner_field] = owner_id
            fields[state_field] = released_state
            if timestamp_field:
                fields[timestamp_field] = _now_ms()
            if extra_released_fields:
                fields.update(extra_released_fields)
            return aspect_class(**fields)  # type: ignore[call-arg]

        def _is_claimed(aspect: _Aspect) -> bool:
            return (
                getattr(aspect, state_field, None) == claimed_state
                and getattr(aspect, owner_field, None) is not None
            )

        return cls(
            graph=graph,
            aspect_name=aspect_name,
            aspect_class=aspect_class,
            make_claimed=_make_claimed,
            make_released=_make_released,
            is_claimed=_is_claimed,
            jitter_max_ms=jitter_max_ms,
        )
