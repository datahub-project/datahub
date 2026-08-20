import io
import zipfile

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.data_lake_common.zip_utils import (
    read_first_supported_zip_entry,
)

SUPPORTED = [".csv", ".json", ".parquet", ".avro", ".tsv"]


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
        supported_suffixes=SUPPORTED,
    )
    assert result is not None
    assert result.data == b"a,b\n1,2\n"
    assert result.suffix == ".csv"
    assert not report.warnings


def test_single_supported_among_unsupported_does_not_warn(caplog):
    # A README next to a single data file must not trigger the multi-entry warning.
    report = SourceReport()
    with caplog.at_level("WARNING"):
        result = read_first_supported_zip_entry(
            _make_zip({"README.txt": b"docs", "data.csv": b"a,b\n1,2\n"}),
            context="archive.zip",
            report=report,
            supported_suffixes=SUPPORTED,
        )
    assert result is not None
    assert result.suffix == ".csv"
    assert not any("supported extension" in r.message for r in caplog.records)


def test_multiple_supported_entries_warns_on_supported_count(caplog):
    report = SourceReport()
    with caplog.at_level("WARNING"):
        result = read_first_supported_zip_entry(
            _make_zip({"README.txt": b"docs", "a.csv": b"x\n1\n", "b.csv": b"y\n2\n"}),
            context="archive.zip",
            report=report,
            supported_suffixes=SUPPORTED,
        )
    assert result is not None
    # Warning counts the two supported entries, not the three total members.
    assert any(
        "2 files with a supported extension" in r.message for r in caplog.records
    )


def test_no_supported_entry_reports_warning():
    report = SourceReport()
    result = read_first_supported_zip_entry(
        _make_zip({"README.txt": b"nothing here"}),
        context="archive.zip",
        report=report,
        supported_suffixes=SUPPORTED,
    )
    assert result is None
    assert report.warnings


def test_bad_zip_reports_warning():
    report = SourceReport()
    result = read_first_supported_zip_entry(
        io.BytesIO(b"this is not a zip"),
        context="archive.zip",
        report=report,
        supported_suffixes=SUPPORTED,
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
        supported_suffixes=SUPPORTED,
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
        supported_suffixes=SUPPORTED,
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
        buf, context="archive.zip", report=report, supported_suffixes=SUPPORTED
    )
    assert result is not None
    assert result.data == b"first\n"


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
        buf, context="archive.zip", report=report, supported_suffixes=SUPPORTED
    )
    assert result is not None
    assert result.data == b"a\n1\n"
