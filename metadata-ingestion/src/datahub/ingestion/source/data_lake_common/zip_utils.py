import dataclasses
import lzma
import pathlib
import zipfile
import zlib
from typing import IO, FrozenSet, Iterable, Optional, Protocol

from typing_extensions import LiteralString


class ZipWarningReporter(Protocol):
    # message/title are LiteralString to match SourceReport.warning, which groups
    # identical warnings by their literal text.
    def warning(
        self,
        message: LiteralString,
        context: Optional[str] = ...,
        title: Optional[LiteralString] = ...,
    ) -> None: ...


# Guard against zip bombs: a small compressed archive can expand to many GB and
# OOM the worker. Refuse to extract any single entry whose *uncompressed* size
# exceeds this many bytes.
DEFAULT_MAX_ZIP_ENTRY_SIZE: int = 512 * 1024 * 1024  # 512 MiB

# Compression methods whose decompressor honors a per-read output cap
# (``decompress(data, max_length)``), so ``ZipExtFile.read(n)`` bounds the
# decompressed bytes for us. Any other method could inflate past the cap before
# it is applied, so we refuse to extract it rather than risk OOMing the worker.
_CAPPABLE_COMPRESS_TYPES: FrozenSet[int] = frozenset(
    {
        zipfile.ZIP_STORED,
        zipfile.ZIP_DEFLATED,
        zipfile.ZIP_BZIP2,
        zipfile.ZIP_LZMA,
    }
)


# Shown in the "no supported files" warning; large archives list only these.
_MAX_LISTED_MEMBERS = 20


def is_zip_path(path: str) -> bool:
    return path.lower().endswith(".zip")


def _is_macos_metadata(filename: str) -> bool:
    # macOS Finder adds __MACOSX/._<name> resource-fork copies of every file.
    return filename.startswith("__MACOSX/") or pathlib.PurePosixPath(
        filename
    ).name.startswith("._")


@dataclasses.dataclass(frozen=True)
class ZipEntry:
    data: bytes
    # Lower-cased inner file extension including the leading dot, e.g. ".csv".
    suffix: str


def read_first_supported_zip_entry(
    zip_file: IO[bytes],
    context: str,
    report: ZipWarningReporter,
    file_types: Iterable[str],
    max_entry_size: int = DEFAULT_MAX_ZIP_ENTRY_SIZE,
) -> Optional[ZipEntry]:
    """Read the first member whose extension is one of ``file_types`` (e.g. ``csv``).

    Returns None, after reporting why, when the archive can't be read, has no
    usable member, or the member is too large, encrypted or corrupt. The caller
    owns ``zip_file``.
    """
    suffixes = frozenset(f".{t.lower()}" for t in file_types)
    try:
        zf = zipfile.ZipFile(zip_file)
    except zipfile.BadZipFile as e:
        report.warning(
            title="Unreadable zip archive",
            message="Could not open zip archive for metadata extraction",
            context=f"{context}: {e}",
        )
        return None

    with zf:
        supported = [
            info
            for info in zf.infolist()
            if not info.is_dir()
            and not _is_macos_metadata(info.filename)
            and pathlib.PurePosixPath(info.filename).suffix.lower() in suffixes
        ]

        if not supported:
            names = zf.namelist()
            listed = ", ".join(names[:_MAX_LISTED_MEMBERS])
            if len(names) > _MAX_LISTED_MEMBERS:
                listed += f" and {len(names) - _MAX_LISTED_MEMBERS} more"
            report.warning(
                title="Zip archive has no supported files",
                message="Zip archive contains no files with a supported extension",
                context=f"{context}: expected one of {sorted(suffixes)}, found {listed}",
            )
            return None

        entry = supported[0]
        if len(supported) > 1:
            report.warning(
                title="Zip archive has multiple supported files",
                message="Only the first supported file in the zip archive was read",
                context=f"{context}: read {entry.filename}, ignored {len(supported) - 1} other(s)",
            )

        if entry.file_size > max_entry_size:
            _report_too_large(report, context, entry, max_entry_size)
            return None

        if entry.flag_bits & 0x1:
            report.warning(
                title="Encrypted zip entry",
                message="Skipping password-protected zip entry",
                context=f"{context}: entry {entry.filename}",
            )
            return None

        if entry.compress_type not in _CAPPABLE_COMPRESS_TYPES:
            report.warning(
                title="Zip entry uses an uncappable compression method",
                message="Skipping zip entry whose compression method cannot be "
                "size-bounded during decompression",
                context=f"{context}: entry {entry.filename} uses compress_type "
                f"{entry.compress_type}",
            )
            return None

        # Open by ZipInfo, not name, so a duplicate member name can't swap in a
        # different entry. Reading one byte past the cap catches a header that
        # under-declares its size; read(n) bounds decompressed output for every
        # method allowed above.
        try:
            with zf.open(entry) as member:
                data = member.read(max_entry_size + 1)
        except (zipfile.BadZipFile, EOFError, OSError, zlib.error, lzma.LZMAError) as e:
            # Reading to EOF checks the CRC-32, so tampered or truncated content
            # fails here too. bzip2 reports bad data as a plain OSError; any other
            # OSError comes from the storage read and must not be mistaken for a
            # corrupt entry.
            if isinstance(e, OSError) and (
                entry.compress_type != zipfile.ZIP_BZIP2
                or isinstance(e, (ConnectionError, TimeoutError))
            ):
                raise
            report.warning(
                title="Corrupt zip entry",
                message="Skipping zip entry that failed decompression or CRC "
                "validation",
                context=f"{context}: entry {entry.filename}: {e}",
            )
            return None
        if len(data) > max_entry_size:
            _report_too_large(report, context, entry, max_entry_size)
            return None

    return ZipEntry(
        data=data, suffix=pathlib.PurePosixPath(entry.filename).suffix.lower()
    )


def _report_too_large(
    report: ZipWarningReporter, context: str, entry: zipfile.ZipInfo, limit: int
) -> None:
    report.warning(
        title="Zip entry too large",
        message="Skipping zip entry whose uncompressed size exceeds the "
        "configured limit; increase max_zip_entry_size to allow it",
        context=f"{context}: entry {entry.filename} exceeds the {limit} byte limit",
    )
