"""Tests for DuckDBProfiler — local-file (no-network) path."""

import os
import shutil
from datetime import datetime, timezone
from unittest.mock import MagicMock

import duckdb
import sqlalchemy as sa

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.data_lake_common.duckdb_profiler import (
    DuckDBExtensionError,
    DuckDBProfiler,
)
from datahub.ingestion.source.data_lake_common.duckdb_secrets import build_s3_secret_sql
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
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


def _make_tsv(tmp: str) -> str:
    path = os.path.join(tmp, "data.tsv")
    with open(path, "w") as f:
        f.write("num\ttxt\n1\ta\n2\tb\n3\ta\n")
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


def test_tempdir_removed_on_gc_without_close():
    """The temp dir must be cleaned up even when close() is never called (e.g. a
    crash mid-profile) — the weakref finalizer runs on garbage collection."""
    import gc

    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    tmpdir = profiler._tmpdir
    assert os.path.isdir(tmpdir)
    del profiler
    gc.collect()
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


def test_path_and_ext_partitioned_extensionless_returns_sample_and_warns(tmp_path):
    """A partitioned table whose files have no extension (format known only via
    content_type, e.g. Spark ``part-00000``) cannot use a ``*.<ext>`` glob — that
    would match nothing. _path_and_ext must return the concrete sample object
    (not a glob) AND surface a warning that only one partition was profiled.

    Guards against a refactor that loosens the ``partitions and file_ext`` guard
    to ``partitions and ext``: that would build ``/**/*.parquet`` over files named
    ``part-00000`` and silently profile zero rows for every such table.
    """
    sample = str(tmp_path / "year=2024" / "part-00000")
    folder = Folder(
        creation_time=datetime.now(timezone.utc),
        modification_time=datetime.now(timezone.utc),
        size=0,
        sample_file=sample,
    )
    table = TableData(
        display_name="events",
        is_s3=False,
        full_path=sample,
        timestamp=datetime.now(timezone.utc),
        table_path=str(tmp_path),
        size_in_bytes=0,
        number_of_files=1,
        partitions=[folder],
    )
    table.content_type = "application/vnd.apache.parquet"
    report = DataLakeSourceReport()
    profiler = DuckDBProfiler(
        aws_config=None,
        report=report,
        profiling_config=_profiling_config(),
    )
    path, ext = profiler._path_and_ext(table)
    profiler.close()
    # Resolved the format from content_type, but kept the concrete sample path.
    assert ext == "parquet"
    assert path == sample
    assert "*" not in path
    # The partial-profile fallback must be visible in the run summary.
    assert any(
        w.title == "Partitioned table profiled on a single file"
        for w in report.warnings
    )


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


def test_sampling_is_random_not_first_n(tmp_path):
    """Sampling must take a reservoir (random) sample, not the first-N rows.

    Regression guard: on append-/date-ordered data-lake files, a first-n
    `.limit()` biases every statistic toward the smallest values (a ~100x error
    on large tables). With an ordered 0..999 column sampled down to 100 rows,
    first-n would give max=99, whereas a random sample's max is almost surely in
    the high hundreds.
    """
    path = os.path.join(str(tmp_path), "ordered.parquet")
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT range AS n FROM range(1000)) TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    cfg = _sampling_config(
        use_sampling=True, sample_size=100, profile_table_row_limit=100
    )
    report = DataLakeSourceReport()
    profiler = DuckDBProfiler(aws_config=None, report=report, profiling_config=cfg)
    profile = _extract_profile(
        profiler.get_table_profile(
            _table_data(path), "urn:li:dataset:(urn:li:dataPlatform:s3,t,PROD)"
        )
    )
    profiler.close()
    assert profile is not None
    assert profile.rowCount == 1000  # true count, not the sample size
    fields = {f.fieldPath: f for f in profile.fieldProfiles}
    # first-n would cap max at 99; a random sample of 100/1000 effectively never does.
    assert int(fields["n"].max) > 200


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


def test_path_with_single_quote_is_handled_via_binding(tmp_path):
    """A file path containing a single quote profiles correctly — the path is a
    bound parameter, so no SQL injection / escaping concern."""
    # Write to a clean path first (the helper inlines the path), then move the
    # file under a directory whose name contains a single quote.
    clean = _make_parquet(str(tmp_path))
    weird_dir = os.path.join(str(tmp_path), "o'brien")
    os.makedirs(weird_dir, exist_ok=True)
    parquet = os.path.join(weird_dir, "data.parquet")
    shutil.move(clean, parquet)
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    profile = _extract_profile(
        profiler.get_table_profile(
            _table_data(parquet),
            "urn:li:dataset:(urn:li:dataPlatform:s3,quote,PROD)",
        )
    )
    profiler.close()
    assert profile is not None
    assert profile.rowCount == 3


def test_non_s3_platform_does_not_create_s3_secret():
    """With platform='gcs', _ensure_s3_secret must not emit any S3 secret SQL."""
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

    profiler._ensure_s3_secret(mock_conn)

    # Assert S3 secret was NOT emitted.
    executed_sqls = [str(call.args[0]) for call in mock_conn.execute.call_args_list]
    assert not any(s3_secret_sql in sql for sql in executed_sqls)
    # _secrets_done must remain False (the guard was never tripped).
    assert profiler._secrets_done is False
    profiler.close()


def test_s3_credential_chain_loads_aws_extension():
    """Role/instance-profile S3 (no explicit keys) emits `PROVIDER credential_chain`,
    which lives in the `aws` extension — so that extension must be loaded too."""
    profiler = DuckDBProfiler(
        aws_config=AwsConnectionConfig(aws_region="us-east-1"),  # no keys
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
        platform="s3",
    )
    conn = MagicMock()
    profiler._ensure_s3_secret(conn)
    profiler.close()
    sqls = [str(c[0][0]) for c in conn.execute.call_args_list]
    assert any("INSTALL aws" in s and "LOAD aws" in s for s in sqls)
    assert any("PROVIDER credential_chain" in s for s in sqls)
    assert "aws" in profiler._loaded_extensions


def test_s3_explicit_keys_does_not_load_aws_extension():
    """With explicit keys, httpfs alone suffices — the `aws` extension (needed only
    for credential_chain) must not be loaded."""
    aws = AwsConnectionConfig(
        aws_access_key_id="AKIA_EXAMPLE",
        aws_secret_access_key="secret_example",
        aws_region="us-east-1",
    )
    profiler = DuckDBProfiler(
        aws_config=aws,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
        platform="s3",
    )
    conn = MagicMock()
    profiler._ensure_s3_secret(conn)
    profiler.close()
    sqls = [str(c[0][0]) for c in conn.execute.call_args_list]
    assert not any("INSTALL aws" in s for s in sqls)
    assert "aws" not in profiler._loaded_extensions


def test_remote_s3_profiling_loads_httpfs_before_creating_secret():
    """For an s3:// path, httpfs must be loaded before the CREATE SECRET runs.

    A regression that reordered these (secret before httpfs) would break remote
    profiling silently, and moto can't serve DuckDB's httpfs range requests, so
    this ordering is only guarded here at the unit level with a mock connection.
    """
    aws = AwsConnectionConfig(
        aws_access_key_id="AKIA_EXAMPLE",
        aws_secret_access_key="secret_example",
        aws_region="us-east-1",
    )
    profiler = DuckDBProfiler(
        aws_config=aws,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
        platform="s3",
    )
    executed: list = []
    conn = MagicMock()

    def _record_execute(stmt, *a, **k):
        executed.append(str(stmt))
        return MagicMock()

    conn.execute.side_effect = _record_execute
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False
    profiler._engine = engine

    # Profiling itself fails on the mock (DESCRIBE returns a non-iterable), which
    # is caught and reported — but httpfs + the secret have already been issued,
    # which is all this test asserts.
    list(
        profiler.get_table_profile(
            _table_data("s3://bucket/data.parquet"),
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/data.parquet,PROD)",
        )
    )
    profiler.close()

    httpfs_idx = next(i for i, s in enumerate(executed) if "LOAD httpfs" in s)
    secret_idx = next(
        i for i, s in enumerate(executed) if "CREATE OR REPLACE SECRET" in s
    )
    assert httpfs_idx < secret_idx


def test_max_fields_truncation_reports_warning(tmp_path):
    """When a table has more columns than max_number_of_fields_to_profile, the
    drop must surface as a run-summary warning (not silently)."""
    parquet = _make_parquet(str(tmp_path))  # 2 columns: num, txt
    cfg = GEProfilingConfig(enabled=True, max_number_of_fields_to_profile=1)
    report = DataLakeSourceReport()
    profiler = DuckDBProfiler(aws_config=None, report=report, profiling_config=cfg)
    list(
        profiler.get_table_profile(
            _table_data(parquet), "urn:li:dataset:(urn:li:dataPlatform:s3,t,PROD)"
        )
    )
    profiler.close()
    assert any("max_number_of_fields_to_profile" in t for t in _warning_texts(report))


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


def test_load_extension_airgapped_error_is_actionable():
    """When INSTALL+LOAD fails (air-gapped, not pre-staged), it must raise the
    structural DuckDBExtensionError (so it escalates to a run failure) with an
    actionable message mentioning pre-staging and the duckdb_extension_directory."""
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    conn = MagicMock()
    conn.execute.side_effect = Exception("no network")
    try:
        try:
            profiler._load_extension(conn, "httpfs")
            raise AssertionError("expected DuckDBExtensionError")
        except DuckDBExtensionError as e:
            msg = str(e)
            assert "duckdb_extension_directory" in msg
            assert "air-gapped" in msg
        # It must be a RuntimeError subtype so get_table_profile routes it to
        # report_failure, not the per-table warning arm.
        assert issubclass(DuckDBExtensionError, RuntimeError)
    finally:
        profiler.close()


def test_load_extension_uses_single_install_load_statement():
    """The extension is loaded via a single `INSTALL ...; LOAD ...` statement.

    Regression guard for the CI breakage: we must NOT probe with a bare `LOAD`
    first. `_load_extension` runs inside the `engine.begin()` transaction that
    materializes the profile table; a failed `LOAD` (extension not yet present)
    aborts that transaction in DuckDB, so an `INSTALL` retry then fails with
    "current transaction is aborted" and the extension never downloads — the
    profile is silently dropped (exactly what broke the avro integration test).
    """
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    conn = MagicMock()
    profiler._load_extension(conn, "avro")
    profiler.close()
    assert conn.execute.call_count == 1
    sql = str(conn.execute.call_args[0][0])
    assert "INSTALL avro" in sql and "LOAD avro" in sql
    # The INSTALL must precede the LOAD (so a cold cache downloads first).
    assert sql.index("INSTALL avro") < sql.index("LOAD avro")
    assert "avro" in profiler._loaded_extensions


def test_apply_extension_directory_sets_when_configured():
    from datahub.ingestion.source.s3.datalake_profiler_config import (
        DataLakeProfilerConfig,
    )

    cfg = DataLakeProfilerConfig(
        enabled=True, duckdb_extension_directory="/opt/ddb-ext"
    )
    profiler = DuckDBProfiler(
        aws_config=None, report=DataLakeSourceReport(), profiling_config=cfg
    )
    conn = MagicMock()
    profiler._apply_extension_directory(conn)
    profiler.close()
    sql = str(conn.execute.call_args[0][0])
    assert "SET extension_directory" in sql
    assert "/opt/ddb-ext" in sql


def test_apply_extension_directory_noop_when_unset():
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    conn = MagicMock()
    profiler._apply_extension_directory(conn)
    profiler.close()
    conn.execute.assert_not_called()


def test_load_extension_still_installs_when_extension_directory_set():
    """A configured `duckdb_extension_directory` must NOT disable INSTALL.

    The directory's presence does not guarantee the correct binary (right DuckDB
    version/platform) is staged there, so we still issue INSTALL — which no-ops
    offline when the staged binary is correct, and downloads/repairs otherwise.
    Gating INSTALL on the directory would silently break profiling against a
    stale or mismatched directory.
    """
    from datahub.ingestion.source.s3.datalake_profiler_config import (
        DataLakeProfilerConfig,
    )

    cfg = DataLakeProfilerConfig(
        enabled=True, duckdb_extension_directory="/opt/ddb-ext"
    )
    profiler = DuckDBProfiler(
        aws_config=None, report=DataLakeSourceReport(), profiling_config=cfg
    )
    conn = MagicMock()
    profiler._load_extension(conn, "httpfs")
    profiler.close()
    sql = str(conn.execute.call_args[0][0])
    assert "INSTALL httpfs" in sql


def test_profiles_local_tsv(tmp_path):
    """TSV must be read with a tab delimiter (distinct from csv); a dropped
    delimiter would collapse to one column and silently mis-profile."""
    tsv = _make_tsv(str(tmp_path))
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    urn = "urn:li:dataset:(urn:li:dataPlatform:s3,t,PROD)"
    profile = _extract_profile(
        profiler.get_table_profile(_table_data(tsv, display_name="data.tsv"), urn)
    )
    profiler.close()
    assert profile is not None
    assert profile.columnCount == 2  # would be 1 if the tab delimiter were dropped
    fields = {f.fieldPath: f for f in profile.fieldProfiles}
    assert int(fields["num"].min) == 1
    assert int(fields["num"].max) == 3


def test_profiling_time_is_recorded(tmp_path):
    """Per-table profiling duration is appended to the shared times list (drives
    the 'Profiling N table(s)' summary + telemetry, which were stuck at 0)."""
    parquet = _make_parquet(str(tmp_path))
    times: list = []
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
        times_taken=times,
    )
    list(
        profiler.get_table_profile(
            _table_data(parquet), "urn:li:dataset:(urn:li:dataPlatform:s3,t,PROD)"
        )
    )
    profiler.close()
    assert len(times) == 1
    assert times[0] >= 0


def test_raw_connection_failure_is_reported_as_failure(tmp_path, monkeypatch):
    """A structural RuntimeError (raw DuckDB connection unreachable) must surface
    as a report failure — not a per-table warning — so a platform-wide breakage
    isn't hidden among warnings."""
    parquet = _make_parquet(str(tmp_path))
    report = DataLakeSourceReport()
    profiler = DuckDBProfiler(
        aws_config=None, report=report, profiling_config=_profiling_config()
    )

    def _boom(conn):
        raise RuntimeError("raw connection unreachable")

    monkeypatch.setattr(profiler, "_raw_connection", _boom)
    wus = list(
        profiler.get_table_profile(
            _table_data(parquet), "urn:li:dataset:(urn:li:dataPlatform:s3,t,PROD)"
        )
    )
    profiler.close()
    assert wus == []
    assert len(report.failures) == 1
    assert any(f.title == "DuckDB profiling unavailable" for f in report.failures)
    assert len(report.warnings) == 0


def test_extension_load_failure_is_reported_as_failure(tmp_path, monkeypatch):
    """An extension that can't load (air-gapped) raises DuckDBExtensionError, which
    must escalate to a report failure (deduped by title), not a per-table warning.
    Also verifies the DuckDBExtensionError arm precedes the RuntimeError arm."""
    parquet = _make_parquet(str(tmp_path))
    report = DataLakeSourceReport()
    profiler = DuckDBProfiler(
        aws_config=None, report=report, profiling_config=_profiling_config()
    )

    def _raise_ext(*args, **kwargs):
        raise DuckDBExtensionError("avro extension unavailable; pre-stage it")

    monkeypatch.setattr(profiler, "_build_select_list", _raise_ext)
    wus = list(
        profiler.get_table_profile(
            _table_data(parquet), "urn:li:dataset:(urn:li:dataPlatform:s3,t,PROD)"
        )
    )
    profiler.close()
    assert wus == []
    assert any(f.title == "DuckDB profiling unavailable" for f in report.failures)
    assert len(report.warnings) == 0


def test_resolve_ext_for_extensionless_file(tmp_path):
    """Extension-less files resolve format via content_type, then
    path_spec.default_extension (mirroring schema inference); unknown stays ''."""
    profiler = DuckDBProfiler(
        aws_config=None,
        report=DataLakeSourceReport(),
        profiling_config=_profiling_config(),
    )
    # content_type takes precedence
    td = _table_data(str(tmp_path / "part-00000"), display_name="part-00000")
    td.content_type = "application/vnd.apache.parquet"
    assert profiler._path_and_ext(td)[1] == "parquet"
    # else path_spec.default_extension
    td2 = _table_data(str(tmp_path / "part-00001"), display_name="part-00001")
    ps = MagicMock()
    ps.default_extension = "csv"
    assert profiler._path_and_ext(td2, ps)[1] == "csv"
    # nothing known -> "" (reported as unsupported downstream, not a crash)
    td3 = _table_data(str(tmp_path / "part-00002"), display_name="part-00002")
    ps_none = MagicMock()
    ps_none.default_extension = None
    assert profiler._path_and_ext(td3, ps_none)[1] == ""
    profiler.close()


def test_extensionless_file_profiles_via_content_type(tmp_path):
    """An extension-less file (e.g. Spark part-file) profiles end-to-end when its
    content_type names the format — without the resolution it would be skipped."""
    src = _make_parquet(str(tmp_path))
    noext = os.path.join(str(tmp_path), "part-00000")
    os.rename(src, noext)
    report = DataLakeSourceReport()
    profiler = DuckDBProfiler(
        aws_config=None, report=report, profiling_config=_profiling_config()
    )
    td = _table_data(noext, display_name="part-00000")
    td.content_type = "application/vnd.apache.parquet"
    profile = _extract_profile(
        profiler.get_table_profile(td, "urn:li:dataset:(urn:li:dataPlatform:s3,t,PROD)")
    )
    profiler.close()
    assert profile is not None
    assert profile.rowCount == 3
    assert profile.columnCount == 2
    assert not report.warnings  # no "unsupported format" warning
