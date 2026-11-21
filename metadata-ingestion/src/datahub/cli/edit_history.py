"""Local undo/redo history for `datahub edit`.

Each `datahub edit` command that changes something appends one entry holding
the operations it applied and the operations that revert them. History is kept
per DataHub server, so `datahub edit undo` against one server never replays an
edit made on another.
"""

import dataclasses
import json
import os
import time
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from datahub.cli.edit_operations import EditCliError, EditOp

HISTORY_FILE_ENV = "DATAHUB_EDIT_HISTORY_FILE"
DEFAULT_HISTORY_FILE = Path.home() / ".datahub" / "edit_history.json"
HISTORY_VERSION = 1
# Older entries are dropped so the file stays small.
MAX_ENTRIES_PER_SERVER = 100


@dataclasses.dataclass
class HistoryEntry:
    command: str
    forward: List[EditOp]
    inverse: List[EditOp]
    at: float = dataclasses.field(default_factory=time.time)

    def to_dict(self) -> Dict[str, object]:
        return {
            "command": self.command,
            "forward": [op.to_dict() for op in self.forward],
            "inverse": [op.to_dict() for op in self.inverse],
            "at": self.at,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "HistoryEntry":
        return cls(
            command=data["command"],
            forward=[EditOp.from_dict(op) for op in data["forward"]],
            inverse=[EditOp.from_dict(op) for op in data["inverse"]],
            at=data["at"],
        )


@dataclasses.dataclass
class ServerHistory:
    done: List[HistoryEntry] = dataclasses.field(default_factory=list)
    undone: List[HistoryEntry] = dataclasses.field(default_factory=list)


def history_file() -> Path:
    override = os.environ.get(HISTORY_FILE_ENV)
    return Path(override) if override else DEFAULT_HISTORY_FILE


class EditHistory:
    """The history file, loaded into memory. Call :meth:`save` to persist."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._servers: Dict[str, ServerHistory] = {}

    @classmethod
    def load(cls, path: Optional[Path] = None) -> "EditHistory":
        history = cls(path or history_file())
        if not history.path.exists():
            return history
        try:
            data = json.loads(history.path.read_text())
        except json.JSONDecodeError as e:
            raise EditCliError(
                f"Edit history file is not valid JSON: {history.path}: {e}",
                error_type="history_corrupt",
                suggestion=f"Move {history.path} aside to start a new history.",
            ) from e
        if data.get("version") != HISTORY_VERSION:
            raise EditCliError(
                f"Unsupported edit history version {data.get('version')!r} "
                f"in {history.path}",
                error_type="history_corrupt",
            )
        for server, stacks in data.get("servers", {}).items():
            history._servers[server] = ServerHistory(
                done=[HistoryEntry.from_dict(e) for e in stacks.get("done", [])],
                undone=[HistoryEntry.from_dict(e) for e in stacks.get("undone", [])],
            )
        return history

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "version": HISTORY_VERSION,
            "servers": {
                server: {
                    "done": [e.to_dict() for e in stacks.done],
                    "undone": [e.to_dict() for e in stacks.undone],
                }
                for server, stacks in self._servers.items()
            },
        }
        # Write to a temp file and rename so a crash never leaves half a file.
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(tmp, self.path)

    def _stacks(self, server: str) -> ServerHistory:
        return self._servers.setdefault(server.rstrip("/"), ServerHistory())

    def record(self, server: str, entry: HistoryEntry) -> None:
        stacks = self._stacks(server)
        stacks.done.append(entry)
        del stacks.done[:-MAX_ENTRIES_PER_SERVER]
        stacks.undone.clear()

    def entries(self, server: str) -> Iterator[HistoryEntry]:
        return iter(self._stacks(server).done)

    def peek_undo(self, server: str) -> HistoryEntry:
        stacks = self._stacks(server)
        if not stacks.done:
            raise EditCliError(
                f"Nothing to undo for {server}", error_type="nothing_to_undo"
            )
        return stacks.done[-1]

    def peek_redo(self, server: str) -> HistoryEntry:
        stacks = self._stacks(server)
        if not stacks.undone:
            raise EditCliError(
                f"Nothing to redo for {server}", error_type="nothing_to_redo"
            )
        return stacks.undone[-1]

    def mark_undone(self, server: str) -> None:
        stacks = self._stacks(server)
        stacks.undone.append(stacks.done.pop())

    def mark_redone(self, server: str) -> None:
        stacks = self._stacks(server)
        stacks.done.append(stacks.undone.pop())
