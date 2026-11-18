import io
import lzma
import zipfile
import zlib

import pytest

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.data_lake_common.zip_utils import (
    read_first_supported_zip_entry,
)

SUPPORTED = ["csv", "json", "parquet", "avro", "tsv"]


def _make_zip(entries: dict) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    buf.seek(0)
    return buf


def test_reads_single_supported_entry():
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"data.csv": b"a,b\n1,2\n"}),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is not None
    assert result.data == b"a,b\n1,2\n"
    assert result.suffix == ".csv"
    assert not report.warnings


def test_single_supported_among_unsupported_does_not_warn():
    # A README next to a single data file must not trigger the multi-entry warning.
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"README.txt": b"docs", "data.csv": b"a,b\n1,2\n"}),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is not None
    assert result.suffix == ".csv"
    assert not report.warnings


def test_multiple_supported_entries_reads_first_and_reports_it():
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"README.txt": b"docs", "a.csv": b"x\n1\n", "b.csv": b"y\n2\n"}),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is not None
    assert result.data == b"x\n1\n"
    assert len(report.warnings) == 1


def test_macos_metadata_members_are_ignored():
    # Finder-created archives list __MACOSX/._data.csv before the real file.
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip(
            {
                "__MACOSX/._data.csv": b"\x00\x05\x16\x07",
                "._data.csv": b"\x00\x05\x16\x07",
                "data.csv": b"a,b\n1,2\n",
            }
        ),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is not None
    assert result.data == b"a,b\n1,2\n"
    assert not report.warnings


def test_member_suffix_is_matched_case_insensitively():
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"DATA.CSV": b"a,b\n1,2\n"}),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is not None
    assert result.suffix == ".csv"


def test_member_outside_file_types_is_not_read():
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"data.json": b"{}", "data.csv": b"a\n1\n"}),
        context="archive.zip",
        report=report,
        file_types=["csv"],
    )
    assert result is not None
    assert result.suffix == ".csv"


def test_encrypted_member_is_skipped():
    buf = _make_zip({"data.csv": b"a,b\n1,2\n"})
    raw = bytearray(buf.getvalue())
    # Set the "encrypted" general-purpose flag in the local and central headers.
    raw[6] |= 0x1
    central = raw.rfind(b"PK\x01\x02")
    raw[central + 8] |= 0x1
    report = SourceReport()
    result = read_first_supported_zip_entry(
        io.BytesIO(bytes(raw)),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is None
    assert report.warnings


def test_under_declared_member_size_is_still_capped():
    payload = b"a,b\n" + b"1,2\n" * 4096
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("data.csv", payload)
    raw = bytearray(buf.getvalue())
    # Rewrite the declared uncompressed size to 10 bytes in both headers so the
    # header pre-check passes and only the bounded read can catch it.
    raw[22:26] = (10).to_bytes(4, "little")
    central = raw.rfind(b"PK\x01\x02")
    raw[central + 24 : central + 28] = (10).to_bytes(4, "little")
    report = SourceReport()
    result = read_first_supported_zip_entry(
        io.BytesIO(bytes(raw)),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
        max_entry_size=1024,
    )
    assert result is None
    assert report.warnings


def test_no_supported_entry_reports_warning():
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"README.txt": b"nothing here"}),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is None
    assert report.warnings


def test_bad_zip_reports_warning():
    report = SourceReport()
    result = read_first_supported_zip_entry(
        io.BytesIO(b"this is not a zip"),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is None
    assert report.warnings


def test_zip_bomb_entry_over_limit_is_skipped():
    # Compresses to a few KB but declares 2 MiB uncompressed — the zip-bomb shape.
    payload = b"0" * (2 * 1024 * 1024)
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"big.csv": payload}),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
        max_entry_size=1024,
    )
    assert result is None
    assert report.warnings


def test_entry_within_limit_is_read():
    payload = b"col\n" + b"1\n" * 100
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"ok.csv": payload}),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
        max_entry_size=10 * 1024 * 1024,
    )
    assert result is not None
    assert result.data == payload


def test_duplicate_member_names_reads_the_first_selected_entry():
    # zipfile.read(name) returns the *last* member with a given name; reading by
    # ZipInfo must return the first, so the size guard and first-entry policy
    # apply to the same member.
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv", b"first\n")
        zf.writestr("data.csv", b"second-different\n")
    buf.seek(0)
    report = SourceReport()
    result = read_first_supported_zip_entry(
        buf, context="archive.zip", report=report, file_types=SUPPORTED
    )
    assert result is not None
    assert result.data == b"first\n"


def test_uncappable_compression_method_is_skipped(monkeypatch):
    # A member whose compression method the read cannot size-bound must be
    # skipped with a warning rather than decompressed. Narrow the allowlist so a
    # normal STORED/DEFLATED entry exercises that branch without crafting an
    # exotic archive.
    from datahub.ingestion.source.data_lake_common import zip_utils

    monkeypatch.setattr(zip_utils, "_CAPPABLE_COMPRESS_TYPES", frozenset())
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"data.csv": b"a\n1\n"}),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is None
    assert report.warnings


def test_crc_mismatch_is_skipped_not_raised():
    # A crafted/corrupt archive whose stored payload no longer matches its CRC-32
    # must be reported and skipped, not crash the source. Build a STORED entry
    # (so file_size is honest and the size guard passes) then flip a payload byte
    # so reading to EOF fails CRC validation.
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr("data.csv", b"hello\n")
    raw = bytearray(buf.getvalue())
    idx = raw.find(b"hello\n")
    assert idx != -1
    raw[idx] = ord("H")  # same length, different content -> CRC no longer matches

    report = SourceReport()
    result = read_first_supported_zip_entry(
        io.BytesIO(bytes(raw)),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is None
    assert report.warnings


@pytest.mark.parametrize(
    "exc",
    [zlib.error("malformed deflate"), lzma.LZMAError("malformed lzma")],
)
def test_decompressor_error_is_skipped_not_raised(monkeypatch, exc):
    # A supported member whose deflate/LZMA stream is malformed raises
    # zlib.error / lzma.LZMAError from read() rather than a zipfile error. It must
    # be reported and skipped, not abort the whole source.
    def boom(self, *args, **kwargs):
        raise exc

    monkeypatch.setattr(zipfile.ZipExtFile, "read", boom)
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"data.csv": b"a\n1\n"}),
        context="archive.zip",
        report=report,
        file_types=SUPPORTED,
    )
    assert result is None
    assert report.warnings


def test_bzip2_data_error_is_skipped_not_raised(monkeypatch):
    # bz2 reports a malformed stream as a plain OSError.
    def boom(self, *args, **kwargs):
        raise OSError("Invalid data stream")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_BZIP2) as zf:
        zf.writestr("data.csv", b"a\n1\n")
    buf.seek(0)
    monkeypatch.setattr(zipfile.ZipExtFile, "read", boom)
    report = SourceReport()
    result = read_first_supported_zip_entry(
        buf, context="archive.zip", report=report, file_types=SUPPORTED
    )
    assert result is None
    assert report.warnings


@pytest.mark.parametrize(
    "exc", [OSError("connection reset"), TimeoutError("read timed out")]
)
def test_storage_error_while_reading_member_is_raised(monkeypatch, exc):
    # A failed ranged read is a storage error, not a corrupt entry, so it must
    # reach the caller instead of producing an empty schema.
    def boom(self, *args, **kwargs):
        raise exc

    monkeypatch.setattr(zipfile.ZipExtFile, "read", boom)
    with pytest.raises(type(exc)):
        read_first_supported_zip_entry(
            _make_zip({"data.csv": b"a\n1\n"}),
            context="archive.zip",
            report=SourceReport(),
            file_types=SUPPORTED,
        )


def test_directory_entry_with_supported_suffix_is_ignored():
    # A directory named like a data file (data.csv/) must not be selected over
    # the real data entry.
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv/", b"")
        zf.writestr("real.csv", b"a\n1\n")
    buf.seek(0)
    report = SourceReport()
    result = read_first_supported_zip_entry(
        buf, context="archive.zip", report=report, file_types=SUPPORTED
    )
    assert result is not None
    assert result.data == b"a\n1\n"
