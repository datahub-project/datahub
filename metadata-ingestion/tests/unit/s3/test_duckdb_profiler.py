"""Tests for DuckDBProfiler — local-file (no-network) path."""

import os
from datetime import datetime, timezone

import duckdb

from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.s3.duckdb_profiler import DuckDBProfiler
from datahub.ingestion.source.s3.report import DataLakeSourceReport
from datahub.ingestion.source.s3.source import Folder, TableData
from datahub.metadata.schema_classes import DatasetProfileClass


def _make_parquet(tmp: str) -> str:
    path = os.path.join(tmp, "data.parquet")
    con = duckdb.connect()
    con.execute(
        "COPY (SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'a')) AS v(num, txt)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    return path


def _table_data(full_path: str, display_name: str = "data.parquet") -> TableData:
    return TableData(
        display_name=display_name,
        is_s3=False,
        full_path=full_path,
        timestamp=datetime.now(timezone.utc),
        table_path=full_path,
        size_in_bytes=os.path.getsize(full_path) if os.path.exists(full_path) else 0,
        number_of_files=1,
    )


def _profiling_config() -> GEProfilingConfig:
    return GEProfilingConfig(enabled=True)


def _extract_profile(wus):
    for wu in wus:
        aspect = wu.metadata.aspect if hasattr(wu.metadata, "aspect") else None
        if isinstance(aspect, DatasetProfileClass):
            return aspect
    return None


def test_profiles_local_parquet(tmp_path):
    parquet = _make_parquet(str(tmp_path))
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    urn = "urn:li:dataset:(urn:li:dataPlatform:s3,test,PROD)"
    profile = _extract_profile(profiler.get_table_profile(_table_data(parquet), urn))
    profiler.close()
    assert profile is not None
    assert profile.rowCount == 3
    assert profile.columnCount == 2
    fields = {f.fieldPath: f for f in profile.fieldProfiles}
    assert int(fields["num"].min) == 1
    assert int(fields["num"].max) == 3


def test_unsupported_format_is_reported_not_raised(tmp_path):
    """An unrecognised extension must produce a warning, not an uncaught exception."""
    bad = os.path.join(str(tmp_path), "data.orc")
    open(bad, "w").close()
    report = DataLakeSourceReport()
    profiler = DuckDBProfiler(
        aws_config=None, report=report, profiling_config=_profiling_config()
    )
    urn = "urn:li:dataset:(urn:li:dataPlatform:s3,bad,PROD)"
    wus = list(
        profiler.get_table_profile(_table_data(bad, display_name="data.orc"), urn)
    )
    profiler.close()
    assert wus == []
    assert len(report.warnings) >= 1


def test_close_removes_tempdir():
    """close() must clean up the temporary DuckDB directory."""
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    tmpdir = profiler._tmpdir
    assert os.path.isdir(tmpdir)
    profiler.close()
    assert not os.path.isdir(tmpdir)


def test_path_and_ext_partitioned_appends_glob(tmp_path):
    """For a partitioned table, _path_and_ext must return a /**/*.<ext> glob."""
    folder = Folder(
        creation_time=datetime.now(timezone.utc),
        modification_time=datetime.now(timezone.utc),
        size=0,
        sample_file=str(tmp_path / "year=2024" / "data.parquet"),
    )
    table = TableData(
        display_name="events",
        is_s3=False,
        full_path=str(tmp_path / "year=2024" / "data.parquet"),
        timestamp=datetime.now(timezone.utc),
        table_path=str(tmp_path),
        size_in_bytes=0,
        number_of_files=1,
        partitions=[folder],
    )
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    path, ext = profiler._path_and_ext(table)
    profiler.close()
    assert ext == "parquet"
    assert path == str(tmp_path) + "/**/*.parquet"


def test_path_and_ext_non_partitioned_returns_full_path(tmp_path):
    """For a non-partitioned table, _path_and_ext must return full_path unchanged."""
    full_path = str(tmp_path / "data.parquet")
    table = TableData(
        display_name="events",
        is_s3=False,
        full_path=full_path,
        timestamp=datetime.now(timezone.utc),
        table_path=full_path,
        size_in_bytes=0,
        number_of_files=1,
    )
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    path, ext = profiler._path_and_ext(table)
    profiler.close()
    assert ext == "parquet"
    assert path == full_path


def test_path_and_ext_empty_extension_falls_through(tmp_path):
    """For a file with no extension, _path_and_ext returns full_path (not a bad glob)."""
    full_path = str(tmp_path / "datafile")
    folder = Folder(
        creation_time=datetime.now(timezone.utc),
        modification_time=datetime.now(timezone.utc),
        size=0,
        sample_file=full_path,
    )
    table = TableData(
        display_name="datafile",
        is_s3=False,
        full_path=full_path,
        timestamp=datetime.now(timezone.utc),
        table_path=str(tmp_path),
        size_in_bytes=0,
        number_of_files=1,
        # Even with partitions, an empty extension must not produce a bad glob.
        partitions=[folder],
    )
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    path, ext = profiler._path_and_ext(table)
    profiler.close()
    assert ext == ""
    # Must be the concrete path, NOT something like "/tmp/.../**/*."
    assert path == full_path
