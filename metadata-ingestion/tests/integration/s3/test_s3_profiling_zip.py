"""Profiling-side zip extraction tests.

`SparkProfiler` lives in a module that imports PySpark/Deequ at import time, so
these tests only run in the profiling test environment. They exercise
`_extract_zip_to_tmp` directly (no Spark session required) to cover the zip
handling the schema-inference tests cannot reach.
"""

import io
import os
import zipfile
from types import SimpleNamespace

import pytest

# pydeequ reads SPARK_VERSION at import time; mirror what S3Source.__init__ does.
os.environ.setdefault("SPARK_VERSION", "3.5")

pytest.importorskip("pyspark")
pytest.importorskip("pydeequ")

from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.s3.profiling import SparkProfiler
from datahub.ingestion.source.s3.report import DataLakeSourceReport

pytestmark = pytest.mark.integration


def _make_zip(entries: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


def _profiler() -> SparkProfiler:
    # Bypass __init__ so we don't spin up a Spark session just to test the
    # local zip extraction helper.
    profiler = SparkProfiler.__new__(SparkProfiler)
    profiler.aws_config = None
    profiler.report = DataLakeSourceReport()
    return profiler


def test_extract_zip_to_tmp_reads_first_supported_entry(tmp_path):
    zip_path = tmp_path / "data.csv.zip"
    zip_path.write_bytes(_make_zip({"data.csv": b"name,age\nAlice,30\n"}))

    profiler = _profiler()
    result = profiler._extract_zip_to_tmp(
        str(zip_path), max_entry_size=10 * 1024 * 1024
    )

    assert result is not None
    try:
        assert result.suffix == ".csv"
        with open(result.path, "rb") as f:
            assert f.read() == b"name,age\nAlice,30\n"
    finally:
        os.unlink(result.path)


def test_extract_zip_to_tmp_enforces_size_limit(tmp_path):
    zip_path = tmp_path / "bomb.csv.zip"
    zip_path.write_bytes(_make_zip({"bomb.csv": b"0" * (2 * 1024 * 1024)}))

    profiler = _profiler()
    result = profiler._extract_zip_to_tmp(str(zip_path), max_entry_size=1024)

    assert result is None
    assert profiler.report.warnings


def test_extract_zip_to_tmp_no_supported_entry(tmp_path):
    zip_path = tmp_path / "docs.zip"
    zip_path.write_bytes(_make_zip({"README.txt": b"nothing here"}))

    profiler = _profiler()
    result = profiler._extract_zip_to_tmp(
        str(zip_path), max_entry_size=10 * 1024 * 1024
    )

    assert result is None
    assert profiler.report.warnings


def test_zip_profiling_respects_disabled_compression(monkeypatch):
    # With enable_compression=False the .zip must be handed to Spark untouched
    # (no extraction), matching the schema-inference path. Spark then rejects the
    # .zip extension, which surfaces as a warning.
    profiler = _profiler()

    extract_calls = []
    monkeypatch.setattr(
        profiler,
        "_extract_zip_to_tmp",
        lambda *a, **k: extract_calls.append(a),
    )

    spark_calls = []
    monkeypatch.setattr(
        profiler,
        "read_file_spark",
        lambda path, ext: spark_calls.append((path, ext)),
    )

    table_data = SimpleNamespace(
        full_path="s3://bucket/t/data.csv.zip",
        table_path="s3://bucket/t",
        partitions=None,
        display_name="t",
    )
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv.zip", enable_compression=False
    )

    workunits = list(
        profiler.get_table_profile(table_data, "urn:li:dataset:test", path_spec)
    )

    assert workunits == []
    assert extract_calls == []  # extraction skipped when compression disabled
    assert spark_calls == [("s3://bucket/t/data.csv.zip", ".zip")]
    assert profiler.report.warnings


def test_partitioned_zip_profiling_is_skipped_with_warning():
    # Spark cannot read a partitioned dataset whose files are .zip archives, so
    # profiling should skip it with a clear warning rather than silently failing.
    profiler = _profiler()
    table_data = SimpleNamespace(
        full_path="s3://bucket/t/year=2022/data.csv.zip",
        table_path="s3://bucket/t",
        partitions=[object()],
        display_name="t",
    )
    path_spec = PathSpec(
        include="s3://bucket/{table}/*.csv.zip", enable_compression=True
    )

    workunits = list(
        profiler.get_table_profile(table_data, "urn:li:dataset:test", path_spec)
    )

    assert workunits == []
    assert profiler.report.warnings
