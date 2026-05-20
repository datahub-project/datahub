from __future__ import annotations

import logging
import re
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from queue import Queue
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from .docker_compose import DockerComposeClient

log = logging.getLogger(__name__)


class SweepState(Enum):
    STARTED = "started"
    BATCH_URNS = "batch_urns"
    BATCH = "batch"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass
class SweepEvent:
    state: SweepState
    source: Literal["datahub-upgrade", "datahub-gms"]
    timestamp: datetime
    message: str
    batch_count: int | None = None
    total_migrated: int | None = None
    batch_urns: list[str] | None = None


# (pattern, state, captures_batch_count_in_group1, captures_total_migrated_in_group1)
_UPGRADE_PATTERNS: list[tuple[re.Pattern, SweepState, bool, bool]] = [
    (
        re.compile(r"Starting migration sweep across \d+ aspect"),
        SweepState.STARTED,
        False,
        False,
    ),
    (
        re.compile(r"Processing batch URNs: (.+)"),
        SweepState.BATCH_URNS,
        False,
        False,
    ),
    (
        re.compile(r"Sweep complete\. Total migrated: (\d+)"),
        SweepState.COMPLETED,
        False,
        True,
    ),
    (
        re.compile(r"Previous result was SUCCEEDED; skipping"),
        SweepState.SKIPPED,
        False,
        False,
    ),
    (
        re.compile(r"Failed upgrade step|Exception.*MigrateAspects"),
        SweepState.FAILED,
        False,
        False,
    ),
]

_GMS_PATTERNS: list[tuple[re.Pattern, SweepState, bool, bool]] = [
    (re.compile(r"Migrated (\d+) aspect\(s\) in batch"), SweepState.BATCH, True, False),
    (re.compile(r"Saving state\. Cursor:"), SweepState.BATCH, False, False),
]


class Phase1State(Enum):
    """Events emitted by ``BuildIndicesIncrementalStep`` during ``SystemUpdateBlocking``."""

    NO_REINDEX_NEEDED = "no_reindex_needed"
    SKIP_ALREADY_DONE = "skip_already_done"
    RESUMING_POLLING = "resuming_polling"
    ALIAS_SWAPPED = "alias_swapped"
    REINDEX_FAILED = "reindex_failed"
    # Settings/mappings-only update path — ESIndexBuilder took the in-place
    # mapping update branch instead of reindex+swap. Fires when
    # ReindexConfig.isPureMappingsAddition is true (NEW only adds mapped
    # fields, modifies none). Used by TC-103.
    MAPPINGS_UPDATE_IN_PLACE = "mappings_update_in_place"
    # ESIndexBuilder.validateAndSwapAlias detected current_count != next_count
    # and refused the swap. Production logs:
    #   "Doc count mismatch for alias swap X -> Y: current=N, next=M"
    # Used by TC-112 fault injection.
    DOC_COUNT_MISMATCH = "doc_count_mismatch"


@dataclass
class Phase1Event:
    state: Phase1State
    timestamp: datetime
    message: str
    index_name: str | None = None
    next_index_name: str | None = None


# Patterns ordered by specificity. Each tuple: (regex, state, captures-index, captures-next).
# "Empty source swap" precedes the generic "Alias swapped" so the more specific
# pattern matches first.
_PHASE1_PATTERNS: list[tuple[re.Pattern, Phase1State, bool, bool]] = [
    (
        re.compile(r"No indices require incremental reindex"),
        Phase1State.NO_REINDEX_NEEDED,
        False,
        False,
    ),
    (
        re.compile(
            r"Index (\S+) already (?:COMPLETED|DUAL_WRITE_DISABLED) in previous run, skipping"
        ),
        Phase1State.SKIP_ALREADY_DONE,
        True,
        False,
    ),
    (
        re.compile(r"Resuming polling for index (\S+) -> (\S+)"),
        Phase1State.RESUMING_POLLING,
        True,
        True,
    ),
    (
        re.compile(
            r"Index (\S+) had 0 docs, next index created as empty, alias swapped"
        ),
        Phase1State.ALIAS_SWAPPED,
        True,
        False,
    ),
    (
        re.compile(r"Alias swapped: (\S+) -> (\S+)"),
        Phase1State.ALIAS_SWAPPED,
        True,
        True,
    ),
    (
        re.compile(r"Alias swap failed for (\S+) -> (\S+)"),
        Phase1State.REINDEX_FAILED,
        True,
        True,
    ),
    (
        re.compile(r"Updating index (\S+) mappings in place"),
        Phase1State.MAPPINGS_UPDATE_IN_PLACE,
        True,
        False,
    ),
    (
        re.compile(r"Doc count mismatch for alias swap (\S+) -> (\S+):"),
        Phase1State.DOC_COUNT_MISMATCH,
        True,
        True,
    ),
]


def _parse_phase1_line(line: str) -> Phase1Event | None:
    """Parse a single line of upgrade-job stdout into a Phase1Event, or None.

    Patterns are tried in order; the first match wins. Independent of
    ``_parse_line`` so the existing SweepEvent flow is untouched.
    """
    for pattern, state, has_idx, has_next in _PHASE1_PATTERNS:
        m = pattern.search(line)
        if not m:
            continue
        idx = m.group(1) if has_idx and m.lastindex else None
        nxt = m.group(2) if has_next and m.lastindex and m.lastindex >= 2 else None
        return Phase1Event(
            state=state,
            timestamp=datetime.utcnow(),
            message=line.strip(),
            index_name=idx,
            next_index_name=nxt,
        )
    return None


def _parse_line(service: str, line: str) -> SweepEvent | None:
    patterns = _UPGRADE_PATTERNS if service == "datahub-upgrade" else _GMS_PATTERNS
    for pattern, state, has_batch, has_total in patterns:
        m = pattern.search(line)
        if m:
            batch_count: int | None = None
            total_migrated: int | None = None
            batch_urns: list[str] | None = None
            if state == SweepState.BATCH_URNS and m.lastindex:
                # URNs contain commas (e.g. urn:li:dashboard:(test,name)),
                # so split on ",urn:" boundary rather than bare ",".
                raw = m.group(1).strip()
                parts = re.split(r",(?=urn:)", raw)
                batch_urns = [u.strip() for u in parts if u.strip()]
            if has_batch and m.lastindex:
                try:
                    batch_count = int(m.group(1))
                except (IndexError, ValueError):
                    pass
            if has_total and m.lastindex:
                try:
                    total_migrated = int(m.group(1))
                except (IndexError, ValueError):
                    pass
            return SweepEvent(
                state=state,
                source=service,  # type: ignore[arg-type]
                timestamp=datetime.utcnow(),
                message=line.strip(),
                batch_count=batch_count,
                total_migrated=total_migrated,
                batch_urns=batch_urns,
            )
    return None


class LogMonitor:
    def __init__(
        self,
        docker: "DockerComposeClient",
        queue: "Queue[SweepEvent]",
        since: datetime,
        gms_service: str = "datahub-gms-debug",
        nonblocking_queue: "Queue[NonBlockingEvent] | None" = None,
    ) -> None:
        self._docker = docker
        self._queue = queue
        self._since = since
        self._gms_service = gms_service
        self._stop = threading.Event()
        self._nonblocking_queue = nonblocking_queue

    def start(self) -> None:
        # GMS BATCH events come from container logs.
        t = threading.Thread(
            target=self._tail_docker,
            args=(self._gms_service,),
            daemon=True,
            name=f"log-monitor-{self._gms_service}",
        )
        t.start()

    def attach_upgrade_popen(self, popen: "subprocess.Popen[str]") -> None:
        """Read upgrade job stdout directly; avoids docker compose logs for one-off containers."""
        t = threading.Thread(
            target=self._tail_popen,
            args=(popen,),
            daemon=True,
            name="log-monitor-upgrade-popen",
        )
        t.start()

    def stop(self) -> None:
        self._stop.set()

    # Upgrade container: show sweep execution milestones from MigrateAspectsStep.
    _UPGRADE_PRINT_KEYWORDS = (
        "migrate-aspects",
        "MigrateAspect",
        "Processing batch URNs",
        "Sweep complete",
        "Starting migration sweep",
        "Migrated",
        "Saving state",
    )

    # GMS container: show writes that arrived from the concurrent test writers.
    # EntityServiceImpl logs "Producing MCL for ingested aspect <name>, urn <urn>"
    # for every successful write. Filter to IO-pool URNs so scheduler noise
    # (dataHubExecutionRequest, etc.) is excluded.
    _GMS_PRINT_KEYWORDS = ("zdu-io-pool", "zdu-tc-")

    def _tail_docker(self, service: str) -> None:
        for line in self._docker.tail_service_logs(service, self._since):
            if self._stop.is_set():
                return
            stripped = line.rstrip()
            if stripped and any(k in stripped for k in self._GMS_PRINT_KEYWORDS):
                log.info("[gms] %s", stripped)
            event = _parse_line(service, line)
            if event:
                log.debug("SweepEvent[%s]: %s", service, event.state)
                self._queue.put(event)

    def _tail_popen(self, popen: "subprocess.Popen[str]") -> None:
        assert popen.stdout is not None
        for line in popen.stdout:
            if self._stop.is_set():
                return
            stripped = line.rstrip()
            event = _parse_line("datahub-upgrade", line)
            if event:
                if event.state == SweepState.BATCH_URNS and event.batch_urns:
                    # Log each sweep URN on its own line so it can be matched
                    # against the corresponding [gms] write line.
                    for urn in event.batch_urns:
                        log.info("[upgrade] sweep URN: %s", urn)
                elif stripped and any(
                    k in stripped for k in self._UPGRADE_PRINT_KEYWORDS
                ):
                    log.info("[upgrade] %s", stripped)
                log.debug("SweepEvent[upgrade-popen]: %s", event.state)
                self._queue.put(event)
            elif stripped and any(k in stripped for k in self._UPGRADE_PRINT_KEYWORDS):
                log.info("[upgrade] %s", stripped)
            if self._nonblocking_queue is not None:
                nb_event = _parse_nonblocking_line(line)
                if nb_event is not None:
                    self._nonblocking_queue.put(nb_event)


@dataclass
class DualWriteEvent:
    """Emitted by MAE's ``UpdateIndicesUpgradeStrategy`` when an old index
    starts receiving dual writes for rollback safety.
    """

    index_name: str
    entity_name: str
    timestamp_ms: int
    timestamp: datetime
    message: str


_DUAL_WRITE_PATTERN = re.compile(
    r"Recorded dual-write start time for index '(\S+)' \(entity '(\S+)'\): (\d+)"
)


def _parse_dual_write_line(line: str) -> DualWriteEvent | None:
    """Parse a MAE log line into a DualWriteEvent, or None.

    Independent of ``_parse_line`` and ``_parse_phase1_line`` so the
    existing flows are untouched.
    """
    m = _DUAL_WRITE_PATTERN.search(line)
    if not m:
        return None
    return DualWriteEvent(
        index_name=m.group(1),
        entity_name=m.group(2),
        timestamp_ms=int(m.group(3)),
        timestamp=datetime.utcnow(),
        message=line.strip(),
    )


class NonBlockingState(Enum):
    """Events emitted by ``IncrementalReindexCatchUpStep`` during ``SystemUpdateNonBlocking``."""

    DUAL_WRITE_DISABLED = "dual_write_disabled"
    CATCH_UP_WINDOW = "catch_up_window"


@dataclass
class NonBlockingEvent:
    state: NonBlockingState
    index_name: str
    timestamp: datetime
    message: str
    window: tuple[int, int] | None = None


_DUAL_WRITE_DISABLED_RE = re.compile(
    r"Marked index (?P<idx>\S+) as DUAL_WRITE_DISABLED"
)
_CATCH_UP_WINDOW_RE = re.compile(
    r"Catch-up for entity index (?P<idx>\S+): window \[(?P<t0>\d+),\s*(?P<t1>\d+)\]"
)


def _parse_nonblocking_line(line: str) -> NonBlockingEvent | None:
    if not line:
        return None
    m = _DUAL_WRITE_DISABLED_RE.search(line)
    if m:
        return NonBlockingEvent(
            state=NonBlockingState.DUAL_WRITE_DISABLED,
            index_name=m.group("idx"),
            timestamp=datetime.utcnow(),
            message=line,
        )
    m = _CATCH_UP_WINDOW_RE.search(line)
    if m:
        try:
            t0 = int(m.group("t0"))
            t1 = int(m.group("t1"))
        except ValueError:
            return None
        return NonBlockingEvent(
            state=NonBlockingState.CATCH_UP_WINDOW,
            index_name=m.group("idx"),
            timestamp=datetime.utcnow(),
            message=line,
            window=(t0, t1),
        )
    return None
