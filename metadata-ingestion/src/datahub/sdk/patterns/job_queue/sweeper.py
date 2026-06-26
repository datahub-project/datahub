"""Layer 2c: Periodic cleanup of stale claims.

A ``Sweeper`` discovers entities that appear to be stale (via a
:class:`~datahub.sdk.patterns.job_queue.discovery.Discovery` instance),
reads each one to confirm staleness, then force-releases via CAS write.

Designed to run in a **dedicated master process**.  The caller is
responsible for ensuring only one Sweeper instance runs at a time —
distributed leader election is not part of this SDK.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field as dataclass_field
from typing import Callable, Dict, Generic, List, Optional, Type, TypeVar

from datahub._codegen.aspect import _Aspect
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.patterns._shared.conditional_writer import ConditionalWriter
from datahub.sdk.patterns.job_queue.discovery import Discovery, SearchDiscovery

logger = logging.getLogger(__name__)

_A = TypeVar("_A", bound=_Aspect)


def _now_ms() -> int:
    """Return current time in milliseconds since the epoch."""
    return int(time.time() * 1000)


@dataclass
class SweepResult:
    """Result of a single sweep cycle."""

    swept: List[str] = dataclass_field(default_factory=list)
    """URNs successfully force-released."""

    failed: List[str] = dataclass_field(default_factory=list)
    """URNs where CAS write failed (version changed) or an error occurred."""

    skipped: List[str] = dataclass_field(default_factory=list)
    """URNs that weren't actually stale after reading the current aspect."""


class Sweeper(Generic[_A]):
    """Periodic cleanup of stale claims.

    Generic over the aspect type so that ``is_stale`` and ``make_swept``
    callables can use typed aspect attributes without ``type: ignore``.

    Designed to run in a dedicated master process.  The caller is responsible
    for ensuring only one Sweeper instance runs at a time — distributed
    leader election is not part of this SDK.

    Args:
        graph: DataHub graph client.
        aspect_name: The claim aspect to sweep (same one used by Claim).
        aspect_class: The aspect's Python class.
        is_stale: Predicate that receives the current aspect and returns
            ``True`` if the claim should be force-released.
        make_swept: Callable that receives the current aspect and returns
            the new aspect value to write (e.g., status="TIMED_OUT").
        discovery: Strategy for finding sweep candidates (entities that
            might be stale).  Typically a :class:`SearchDiscovery` with
            filters matching the claimed state.
    """

    def __init__(
        self,
        graph: DataHubGraph,
        aspect_name: str,
        aspect_class: Type[_A],
        is_stale: Callable[[_A], bool],
        make_swept: Callable[[_A], _A],
        discovery: Discovery,
    ) -> None:
        self._writer = ConditionalWriter(graph)
        self._aspect_name = aspect_name
        self._aspect_class = aspect_class
        self._is_stale = is_stale
        self._make_swept = make_swept
        self._discovery = discovery

    def sweep(self) -> SweepResult:
        """Run one sweep cycle.

        1. Poll discovery for candidates (entities in claimed state).
        2. For each candidate, read the current versioned aspect.
        3. Check ``is_stale`` — skip if not stale.
        4. CAS-write the swept aspect value.
        5. Return results.

        CAS conflicts are expected and harmless — they mean the entity
        was released or re-claimed between the read and write.
        """
        swept: List[str] = []
        failed: List[str] = []
        skipped: List[str] = []

        for item in self._discovery.poll():
            urn = item.urn
            try:
                va = self._writer.read_versioned_aspect(
                    urn, self._aspect_name, self._aspect_class
                )

                if va.aspect is None or not isinstance(va.aspect, self._aspect_class):
                    skipped.append(urn)
                    continue

                aspect: _A = va.aspect

                if not self._is_stale(aspect):
                    skipped.append(urn)
                    continue

                result = self._writer.write_if_version(
                    urn,
                    self._aspect_name,
                    self._make_swept(aspect),
                    va.version,
                    track_new_version=False,
                )

                if result.success:
                    swept.append(urn)
                else:
                    failed.append(urn)

            except Exception:
                logger.warning("Sweep failed for %s", urn, exc_info=True)
                failed.append(urn)

        return SweepResult(swept=swept, failed=failed, skipped=skipped)

    @classmethod
    def from_fields(
        cls,
        graph: DataHubGraph,
        aspect_name: str,
        aspect_class: Type[_A],
        entity_type: str,
        owner_field: str,
        state_field: str,
        claimed_state: str,
        swept_state: str,
        timestamp_field: str,
        timeout_ms: int,
        extra_swept_fields: Optional[Dict[str, object]] = None,
        max_results: int = 100,
    ) -> Sweeper:
        """Build a ``Sweeper`` from field names.

        Creates a :class:`SearchDiscovery` that finds entities in
        *claimed_state*, and generates ``is_stale`` / ``make_swept``
        from field names and timeout.

        Args:
            entity_type: Entity type for search (e.g. ``"workflowTask"``).
            owner_field: Field containing the claim owner ID.
            state_field: Field indicating claim state (e.g. ``"status"``).
            claimed_state: Value of *state_field* when claimed (e.g. ``"RUNNING"``).
            swept_state: Value to set on force-release (e.g. ``"TIMED_OUT"``).
            timestamp_field: Field containing claim timestamp (epoch ms).
            timeout_ms: Claims older than this are considered stale.
            extra_swept_fields: Additional fields to set on sweep.
            max_results: Max candidates per sweep cycle.
        """

        def _is_stale(aspect: _A) -> bool:
            state = getattr(aspect, state_field, None)
            if state != claimed_state:
                return False
            ts = getattr(aspect, timestamp_field, None)
            if ts is None:
                return True  # No timestamp → assume stale
            return (_now_ms() - int(ts)) > timeout_ms

        def _make_swept(current: _A) -> _A:
            fields: Dict[str, object] = (
                dict(current._inner_dict) if current is not None else {}
            )
            fields[state_field] = swept_state
            if timestamp_field:
                fields[timestamp_field] = _now_ms()
            if extra_swept_fields:
                fields.update(extra_swept_fields)
            return aspect_class(**fields)  # type: ignore[call-arg]

        discovery = SearchDiscovery(
            graph=graph,
            entity_type=entity_type,
            filters={state_field: [claimed_state]},
            max_results=max_results,
        )

        return cls(
            graph=graph,
            aspect_name=aspect_name,
            aspect_class=aspect_class,
            is_stale=_is_stale,
            make_swept=_make_swept,
            discovery=discovery,
        )
