"""Only skip a completed bootstrap when its H2 database still exists."""

from pathlib import Path

MARKER_FILE = "system-update-done"


class SystemUpdateChecker:
    def is_complete(self, data_dir: Path, instance: str) -> bool:
        directory = data_dir / instance
        return (directory / "gms.mv.db").is_file() and (
            directory / MARKER_FILE
        ).is_file()

    def mark_complete(self, data_dir: Path, instance: str) -> None:
        (data_dir / instance / MARKER_FILE).write_text(
            "SystemUpdate and RestoreIndices completed\n"
        )
