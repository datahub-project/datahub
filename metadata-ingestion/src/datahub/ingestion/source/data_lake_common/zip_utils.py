import logging
import pathlib
import zipfile
from typing import IO, Iterable, Optional

from pydantic import BaseModel

from datahub.ingestion.api.source import SourceReport

logger: logging.Logger = logging.getLogger(__name__)

# Guard against zip bombs: a small compressed archive can expand to many GB and
# OOM the worker. Refuse to extract any single entry whose *uncompressed* size
# exceeds this many bytes.
DEFAULT_MAX_ZIP_ENTRY_SIZE: int = 512 * 1024 * 1024  # 512 MiB


class ZipEntry(BaseModel):
    """A single file extracted from a zip archive."""

    data: bytes
    # Inner file extension including the leading dot, e.g. ".csv".
    suffix: str


def read_first_supported_zip_entry(
    zip_file: IO[bytes],
    context: str,
    report: SourceReport,
    supported_suffixes: Iterable[str],
    max_entry_size: int = DEFAULT_MAX_ZIP_ENTRY_SIZE,
) -> Optional[ZipEntry]:
    """Read the first entry with a supported suffix from an open zip archive.

    ``supported_suffixes`` are suffixes *with* a leading dot (e.g. ``.csv``).

    Returns the extracted entry, or ``None`` when the archive cannot be opened,
    contains no supported entry, or the chosen entry's uncompressed size exceeds
    ``max_entry_size`` (zip-bomb guard). A warning is reported in each of those
    cases. When more than one supported entry is present, only the first is used
    and a warning is logged.

    The declared ``ZipInfo.file_size`` is trusted for the size check; ``zipfile``
    itself raises ``BadZipFile`` if the decompressed stream overshoots the
    declared size, so a lying header cannot silently bypass the cap.

    The caller owns ``zip_file`` and is responsible for closing it.
    """
    suffixes = frozenset(supported_suffixes)
    try:
        zf = zipfile.ZipFile(zip_file)
    except zipfile.BadZipFile as e:
        report.report_warning(
            title="Unreadable zip archive",
            message="Could not open zip archive for metadata extraction",
            context=f"{context}: {e}",
        )
        return None

    with zf:
        supported = [
            info
            for info in zf.infolist()
            if pathlib.Path(info.filename).suffix in suffixes
        ]

        if not supported:
            report.report_warning(
                title="Zip archive has no supported files",
                message="Zip archive contains no files with a supported extension",
                context=f"{context}: expected one of {sorted(suffixes)}, "
                f"found {zf.namelist()}",
            )
            return None

        if len(supported) > 1:
            logger.warning(
                f"Zip archive {context} contains {len(supported)} files with a "
                f"supported extension; using only the first: {supported[0].filename}"
            )

        entry = supported[0]
        if entry.file_size > max_entry_size:
            report.report_warning(
                title="Zip entry too large",
                message="Skipping zip entry whose uncompressed size exceeds the "
                "configured limit; increase max_zip_entry_size to allow it",
                context=f"{context}: entry {entry.filename} is {entry.file_size} "
                f"bytes uncompressed, limit is {max_entry_size} bytes",
            )
            return None

        inner_suffix = pathlib.Path(entry.filename).suffix
        data = zf.read(entry.filename)

    return ZipEntry(data=data, suffix=inner_suffix)
