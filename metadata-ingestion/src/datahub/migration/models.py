"""Data structures for the entity migration framework.

These are deliberately free of any CLI/``click`` dependency so the migration
engine can be driven programmatically as well as from the ``datahub migrate`` CLI.
"""

import dataclasses
from dataclasses import dataclass
from typing import IO, Callable, List, Optional, Tuple

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DataPlatformInstanceClass
from datahub.utilities.str_enum import StrEnum


class ConflictStrategy(StrEnum):
    """How to handle aspect writes when the migration target already exists.

    Aligns with DataHub's TransformerSemantics terminology:
    - OVERWRITE: Write aspects from source, replacing target values.
    - PATCH: Merge with existing target values, only add new data.
    - PROMPT: Ask the user interactively for each conflict.
    - PRESERVE: Leave the existing target completely untouched (no merge, no
      overwrite). Incoming references are still repointed to the target and the
      source is still deleted — i.e. the existing target is *adopted* in place of
      the source.
    """

    OVERWRITE = "overwrite"
    PATCH = "patch"
    PROMPT = "prompt"
    PRESERVE = "preserve"


@dataclass
class MergeResult:
    """Outcome of merging a source entity into an existing target."""

    merged: int
    skipped: int
    merged_aspects: List[str] = dataclasses.field(default_factory=list)
    skipped_aspects: List[str] = dataclasses.field(default_factory=list)


@dataclass
class MigrationPair:
    """A single source → target URN migration.

    ``data_platform_instance`` carries the *target* platform-instance aspect to
    stamp on the migrated entity. It must be supplied by the caller: a platform
    instance is string-prepended into an entity's name/id, so it cannot be
    recovered from the target URN alone. When ``None`` no instance aspect is
    emitted.

    ``aspect_mutator`` is an optional hook applied to each cloned MCP before it is
    emitted (used e.g. by container migration to reseat ``containerProperties``),
    keeping the engine itself agnostic to entity-specific quirks.
    """

    source_urn: str
    target_urn: str
    data_platform_instance: Optional[DataPlatformInstanceClass] = None
    aspect_mutator: Optional[Callable[[MetadataChangeProposalWrapper], None]] = None


@dataclass
class MigrationOptions:
    """Knobs controlling how a batch of pairs is migrated."""

    run_id: str
    dry_run: bool = False
    hard: bool = False
    keep: bool = False
    on_conflict: Optional[ConflictStrategy] = None
    skip_on_error: bool = False
    checkpoint_file: Optional[str] = None
    report_file: Optional[str] = None


class MigrationReport:
    def __init__(self, run_id: str, dry_run: bool, keep: bool) -> None:
        self.run_id = run_id
        self.dry_run = dry_run
        self.keep = keep
        self.num_events = 0
        self.num_entities_migrated: int = 0
        self.num_entities_created: int = 0
        self.num_aspects_created: int = 0
        self.num_entities_affected: int = 0
        self.num_aspects_affected: int = 0
        self._last_created_urn: Optional[str] = None
        self._last_affected_urn: Optional[str] = None
        self._report_fh: Optional[IO[str]] = None
        self._current_source: str = ""
        self._current_target: str = ""
        self.conflicts_skipped: int = 0
        self.aspects_merged: int = 0
        self.pairs_checkpoint_skipped: int = 0
        self.entities_errored: List[Tuple[str, str]] = []

    def set_current_pair(self, source_urn: str, target_urn: str) -> None:
        self._current_source = source_urn
        self._current_target = target_urn

    def on_entity_migrated(self, urn: str, aspect: str) -> None:
        self.num_events += 1
        self.num_entities_migrated += 1
        self._write_report(
            "migrated", self._current_source, self._current_target, aspect
        )

    def on_entity_create(self, urn: str, aspect: str) -> None:
        self.num_events += 1
        self.num_aspects_created += 1
        if urn != self._last_created_urn:
            self.num_entities_created += 1
            self._last_created_urn = urn
        self._write_report("create", self._current_source, self._current_target, aspect)

    def on_aspect_merged(self, aspect: str) -> None:
        self._write_report("merge", self._current_source, self._current_target, aspect)

    def on_aspect_skipped(self, aspect: str) -> None:
        self._write_report("skip", self._current_source, self._current_target, aspect)

    def on_entity_affected(self, urn: str, aspect: str) -> None:
        self.num_events += 1
        self.num_aspects_affected += 1
        if urn != self._last_affected_urn:
            self.num_entities_affected += 1
            self._last_affected_urn = urn
        # For affected lines the referrer URN (the entity being repointed) is
        # the interesting subject; the pair's target is what it was repointed to.
        self._write_report("affected", urn, self._current_target, aspect)

    def _write_report(self, action: str, col1: str, col2: str, aspect: str) -> None:
        if self._report_fh is not None:
            self._report_fh.write(f"{action}\t{col1}\t{col2}\t{aspect}\n")

    def open_report_file(self, path: str) -> None:
        self._report_fh = open(path, "a")

    def close_report_file(self) -> None:
        if self._report_fh is not None:
            self._report_fh.close()
            self._report_fh = None

    def _get_prefix(self) -> str:
        return "[Dry Run] " if self.dry_run else ""

    def __repr__(self) -> str:
        p = self._get_prefix()
        lines = [
            f"{p}Migration Report:",
            "--------------",
            f"{p}Migration Run Id: {self.run_id}",
            f"{p}Num entities created = {self.num_entities_created}",
            f"{p}Num entities affected = {self.num_entities_affected}",
            f"{p}Num entities {'kept' if self.keep else 'migrated'} = {self.num_entities_migrated}",
        ]
        if self.aspects_merged > 0:
            lines.append(f"{p}Aspects merged = {self.aspects_merged}")
        if self.pairs_checkpoint_skipped > 0:
            lines.append(
                f"{p}Pairs checkpoint-skipped = {self.pairs_checkpoint_skipped}"
            )
        if self.conflicts_skipped > 0:
            lines.append(f"{p}Conflicts skipped = {self.conflicts_skipped}")
        if self.entities_errored:
            lines.append(f"{p}Entities errored = {len(self.entities_errored)}")
            for urn, err in self.entities_errored:
                lines.append(f"{p}  {urn}: {err}")
        return "\n".join(lines)
