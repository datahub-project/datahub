"""Data structures for the entity migration framework.

These are deliberately free of any CLI/``click`` dependency so the migration
engine can be driven programmatically as well as from the ``datahub migrate`` CLI.
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

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


class MigrationReport:
    def __init__(self, run_id: str, dry_run: bool, keep: bool) -> None:
        self.run_id = run_id
        self.dry_run = dry_run
        self.keep = keep
        self.num_events = 0
        self.entities_migrated: Dict[Tuple[str, str], int] = {}
        self.entities_created: Dict[Tuple[str, str], int] = {}
        self.entities_affected: Dict[Tuple[str, str], int] = {}
        self.conflicts_skipped: int = 0
        self.aspects_merged: int = 0
        self.pairs_checkpoint_skipped: int = 0
        self.entities_errored: List[Tuple[str, str]] = []

    def on_entity_migrated(self, urn: str, aspect: str) -> None:
        self.num_events += 1
        if (urn, aspect) not in self.entities_migrated:
            self.entities_migrated[(urn, aspect)] = 1

    def on_entity_create(self, urn: str, aspect: str) -> None:
        self.num_events += 1
        if (urn, aspect) not in self.entities_created:
            self.entities_created[(urn, aspect)] = 1

    def on_entity_affected(self, urn: str, aspect: str) -> None:
        self.num_events += 1
        if (urn, aspect) not in self.entities_affected:
            self.entities_affected[(urn, aspect)] = 1
        else:
            self.entities_affected[(urn, aspect)] += 1

    def _get_prefix(self) -> str:
        return "[Dry Run] " if self.dry_run else ""

    def __repr__(self) -> str:
        p = self._get_prefix()
        lines = [
            f"{p}Migration Report:",
            "--------------",
            f"{p}Migration Run Id: {self.run_id}",
            f"{p}Num entities created = {len(set(x[0] for x in self.entities_created))}",
            f"{p}Num entities affected = {len(set(x[0] for x in self.entities_affected))}",
            f"{p}Num entities {'kept' if self.keep else 'migrated'} = {len(set(x[0] for x in self.entities_migrated))}",
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
        lines.append(f"{p}Details:")
        lines.append(
            f"{p}New Entities Created: {set(x[0] for x in self.entities_created) or 'None'}"
        )
        lines.append(
            f"{p}External Entities Affected: {set(x[0] for x in self.entities_affected) or 'None'}"
        )
        lines.append(
            f"{p}Old Entities {'Kept' if self.keep else 'Migrated'} = {set(x[0] for x in self.entities_migrated) or 'None'}"
        )
        return "\n".join(lines)
