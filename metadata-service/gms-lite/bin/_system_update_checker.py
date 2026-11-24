"""Only skip bootstrap when H2 and the recorded search index set still exist."""

import json
from pathlib import Path

MARKER_FILE = "system-update-done"


class SystemUpdateChecker:
    @staticmethod
    def owned_indices(prefix: str, indices: list[str]) -> list[str]:
        return sorted(
            name
            for name in indices
            if name.startswith(prefix + "_") and name != prefix + "_datahub_usage_event"
        )

    def is_complete(
        self, data_dir: Path, instance: str, prefix: str, existing_indices: list[str]
    ) -> bool:
        directory = data_dir / instance
        if not (directory / "gms.mv.db").is_file():
            return False
        try:
            expected: object = json.loads((directory / MARKER_FILE).read_text())
        except (OSError, ValueError):
            return False
        return (
            isinstance(expected, list)
            and bool(expected)
            and all(isinstance(name, str) for name in expected)
            and expected == self.owned_indices(prefix, expected)
            and set(expected).issubset(existing_indices)
        )

    def mark_complete(
        self, data_dir: Path, instance: str, prefix: str, indices: list[str]
    ) -> None:
        expected = self.owned_indices(prefix, indices)
        if not expected:
            raise RuntimeError(f"no search indices after bootstrap for {instance}")
        # An old text marker triggers one upgrade run to establish the expected set.
        marker = data_dir / instance / MARKER_FILE
        temporary = marker.with_suffix(".tmp")
        temporary.write_text(json.dumps(expected) + "\n")
        temporary.replace(marker)
