"""Tests for DuckDBProfiler — local-file (no-network) path."""

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock

import duckdb
import sqlalchemy as sa

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.s3.duckdb_profiler import DuckDBProfiler
from datahub.ingestion.source.s3.duckdb_secrets import build_s3_secret_sql
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


def _make_avro(tmp: str) -> str:
    path = os.path.join(tmp, "data.avro")
    con = duckdb.connect()
    # The avro extension ships in the DuckDB core repo (not community).
    con.execute("INSTALL avro; LOAD avro;")
    con.execute(
        "COPY (SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'a')) AS v(num, txt)) "
        f"TO '{path}' (FORMAT avro)"
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


def _sampling_config(**kw: object) -> GEProfilingConfig:
    return GEProfilingConfig(enabled=True, **kw)


def _warning_texts(report: DataLakeSourceReport) -> list:
    """Return all warning message strings from the report."""
    return [w.message for w in report.warnings]


def test_large_table_is_sampled_but_reports_true_rowcount(tmp_path):
    parquet = _make_parquet(str(tmp_path))  # 3-row parquet
    cfg = _sampling_config(use_sampling=True, sample_size=2, profile_table_row_limit=2)
    report = DataLakeSourceReport()
    profiler = DuckDBProfiler(aws_config=None, report=report, profiling_config=cfg)
    profile = _extract_profile(
        profiler.get_table_profile(
            _table_data(parquet), "urn:li:dataset:(urn:li:dataPlatform:s3,t,PROD)"
        )
    )
    profiler.close()
    assert profile is not None
    # 3 rows > row limit 2 and use_sampling -> sampled, but rowCount reports the true 3.
    assert profile.rowCount == 3
    # The sampling branch must have emitted a warning mentioning sampling.
    assert any("sampl" in t.lower() for t in _warning_texts(report))


def test_small_table_not_sampled(tmp_path):
    parquet = _make_parquet(str(tmp_path))  # 3 rows
    cfg = _sampling_config(
        use_sampling=True, sample_size=2, profile_table_row_limit=1000
    )
    report = DataLakeSourceReport()
    profiler = DuckDBProfiler(aws_config=None, report=report, profiling_config=cfg)
    profile = _extract_profile(
        profiler.get_table_profile(
            _table_data(parquet), "urn:li:dataset:(urn:li:dataPlatform:s3,t,PROD)"
        )
    )
    profiler.close()
    assert profile.rowCount == 3  # full scan, all 3 rows
    # 3 rows < row limit 1000 so no sampling warning should be emitted.
    assert not any("sampl" in t.lower() for t in _warning_texts(report))


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


def test_profiles_local_avro(tmp_path):
    avro_path = _make_avro(str(tmp_path))
    cfg = _profiling_config()
    profiler = DuckDBProfiler(
        aws_config=None, report=DataLakeSourceReport(), profiling_config=cfg
    )
    urn = "urn:li:dataset:(urn:li:dataPlatform:s3,a,PROD)"
    profile = _extract_profile(
        profiler.get_table_profile(
            _table_data(avro_path, display_name="data.avro"), urn
        )
    )
    profiler.close()
    assert profile is not None
    assert profile.rowCount == 3
    assert profile.columnCount == 2


def test_reader_expr_escapes_single_quotes():
    """_reader_expr must double-escape single quotes in paths."""
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    expr = profiler._reader_expr("s3://b/it's/f.parquet", "parquet")
    profiler.close()
    assert "it''s" in expr
    assert "it's" not in expr


def test_non_s3_platform_does_not_create_s3_secret():
    """With platform='gcs', _ensure_remote_setup must not emit any S3 secret SQL."""
    aws = AwsConnectionConfig(
        aws_access_key_id="AKIA_EXAMPLE",
        aws_secret_access_key="secret_example",
        aws_region="us-east-1",
    )
    profiler = DuckDBProfiler(
        aws_config=aws,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
        platform="gcs",
    )
    # Build the SQL that *would* have been emitted for S3 so we can check it was NOT sent.
    s3_secret_sql = build_s3_secret_sql(aws)

    # Use a mock connection to capture execute calls.
    mock_conn = MagicMock(spec=sa.engine.Connection)

    # We need httpfs to be already loaded so only the secret branch is tested.
    profiler._httpfs_loaded = True

    profiler._ensure_remote_setup(mock_conn)

    # Assert S3 secret was NOT emitted.
    executed_sqls = [str(call.args[0]) for call in mock_conn.execute.call_args_list]
    assert not any(s3_secret_sql in sql for sql in executed_sqls)
    # _secrets_done must remain False (the guard was never tripped).
    assert profiler._secrets_done is False
    profiler.close()


def _make_nested_parquet(tmp: str) -> str:
    path = os.path.join(tmp, "nested.parquet")
    con = duckdb.connect()
    # A LIST(STRUCT(...)) column — duckdb-engine cannot reflect this type, so
    # the profiler would skip it without the cast-nested-to-JSON handling.
    con.execute(
        "COPY (SELECT * FROM (VALUES "
        "(1, [{'a': 1, 'b': 'x'}]), (2, [{'a': 2, 'b': 'y'}]), (3, [{'a': 1, 'b': 'x'}])"
        ") AS v(num, nested)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    return path


def test_nested_column_is_profiled_as_json(tmp_path):
    parquet = _make_nested_parquet(str(tmp_path))
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    profile = _extract_profile(
        profiler.get_table_profile(
            _table_data(parquet, display_name="nested.parquet"),
            "urn:li:dataset:(urn:li:dataPlatform:s3,nested,PROD)",
        )
    )
    profiler.close()
    assert profile is not None
    fields = {f.fieldPath: f for f in profile.fieldProfiles}
    nested = fields["nested"]
    # The nested column now gets real metrics instead of an empty profile.
    assert nested.nullCount == 0
    assert nested.uniqueCount == 2  # two distinct list-of-struct values
    # Sample values are emitted as clean JSON strings (double-quoted keys).
    assert nested.sampleValues is not None
    assert any('"a"' in v and '"b"' in v for v in nested.sampleValues)
