"""DuckDB-based profiler for data-lake sources (replaces Spark/Deequ)."""

import logging
import os
import shutil
import tempfile
import weakref
from typing import TYPE_CHECKING, Iterable, List, Optional, Protocol, Set

import duckdb
import sqlalchemy as sa

if TYPE_CHECKING:
    from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
    from datahub.ingestion.source.s3.source import TableData

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.data_lake_common.duckdb_secrets import (
    build_s3_secret_sql,
)
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.profiling.common import ProfilerRequest
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
    SQLAlchemyProfiler,
)
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)


class DuckDBExtensionError(RuntimeError):
    """A required DuckDB extension could not be installed/loaded.

    Subclasses ``RuntimeError`` so it routes to ``report_failure`` in
    ``get_table_profile``: an extension that can't load (e.g. httpfs/aws/avro in
    an air-gapped environment without a staged binary) is a structural,
    platform-wide failure that recurs for every table — not a per-table data
    issue like an unsupported format or an unresolvable path (which stay
    warnings).
    """


class _ProfilerReport(Protocol):
    """Minimal report surface the profiler needs.

    Declared structurally rather than importing a concrete source report, so
    this module lives in ``data_lake_common`` without depending on any specific
    source package (s3/abs/gcs/...). The data-lake source reports satisfy it.
    """

    def report_warning(
        self,
        message: str,
        *,
        context: Optional[str] = ...,
        title: Optional[str] = ...,
        exc: Optional[BaseException] = ...,
    ) -> None: ...

    def report_failure(
        self,
        message: str,
        *,
        context: Optional[str] = ...,
        title: Optional[str] = ...,
        exc: Optional[BaseException] = ...,
    ) -> None: ...


_REMOTE_SCHEMES = (
    "s3://",
    "s3a://",
    "gs://",
    "gcs://",
    "az://",
    "abfs://",
    "abfss://",
)

# Map a file extension to the DuckDB core extension required to read it.
# Unlike httpfs (remote-only), format extensions are needed for local files too.
_FORMAT_EXTENSIONS = {"avro": "avro"}

# S3 content-type (MIME) -> profiling extension key, mirroring the schema
# inferrer's content-type handling (see S3Source._get_inferrer). Lets
# extension-less files (e.g. Spark/Hive `part-00000`) profile when their format
# is known from S3 metadata, just as schema inference already does.
_CONTENT_TYPE_EXT = {
    "application/vnd.apache.parquet": "parquet",
    "text/csv": "csv",
    "text/tab-separated-values": "tsv",
    "application/json": "json",
    "application/avro": "avro",
}


class DuckDBProfiler:
    """Profile a single data-lake table via a DuckDB attach layer.

    Opens a temp-file-backed DuckDB database, registers the table's files as a
    view, then delegates statistics collection to SQLAlchemyProfiler using the
    DuckDB dialect adapter.  Only remote paths (s3://, gs://, az://, …) trigger
    httpfs installation and CREATE SECRET; local paths bypass the network stack
    entirely.
    """

    def __init__(
        self,
        aws_config: Optional[AwsConnectionConfig],
        report: _ProfilerReport,
        profiling_config: GEProfilingConfig,
        platform: str = "s3",
        times_taken: Optional[List[float]] = None,
    ) -> None:
        self.aws_config = aws_config
        self.report = report
        self.profiling_config = profiling_config
        self.platform = platform
        # Shared list the source reports per-table profiling durations on (drives
        # the "Profiling N table(s)" summary + telemetry percentiles). Appended to
        # once per profiled table; None disables timing collection.
        self._times_taken = times_taken
        self._tmpdir = tempfile.mkdtemp(prefix="datahub-duckdb-profile-")
        self._db_path = os.path.join(self._tmpdir, "profile.duckdb")
        # Remove the temp dir even if close() is never called (uncaught
        # exception, GC, interpreter exit). finalize() is idempotent, so an
        # explicit close() simply triggers it early.
        self._finalizer = weakref.finalize(
            self, shutil.rmtree, self._tmpdir, ignore_errors=True
        )
        self._engine: Optional[sa.engine.Engine] = None
        self._secrets_done = False
        self._loaded_extensions: Set[str] = set()

    def _engine_lazy(self) -> sa.engine.Engine:
        if self._engine is None:
            self._engine = sa.create_engine(f"duckdb:///{self._db_path}")
        return self._engine

    def _is_remote(self, path: str) -> bool:
        return path.startswith(_REMOTE_SCHEMES)

    def _apply_extension_directory(self, conn: sa.engine.Connection) -> None:
        """Point DuckDB at a pre-staged extension directory, if configured.

        Lets extensions load offline in air-gapped environments: operators copy
        the `.duckdb_extension` binaries into this directory and DuckDB loads them
        without contacting its extension repository.
        """
        ext_dir = getattr(self.profiling_config, "duckdb_extension_directory", None)
        if ext_dir:
            escaped = ext_dir.replace("'", "''")
            conn.execute(sa.text(f"SET extension_directory = '{escaped}'"))

    def _load_extension(self, conn: sa.engine.Connection, extension: str) -> None:
        """Install (if needed) and load a DuckDB extension.

        Issued as a single ``INSTALL ...; LOAD ...`` statement. ``INSTALL`` is a
        no-op when the extension is already present — including a correct binary
        pre-staged in ``duckdb_extension_directory``, in which case it touches no
        network (verified to work fully air-gapped). When the extension is
        missing, or the staged binary doesn't match this DuckDB version/platform,
        ``INSTALL`` downloads the right one; we fail only if that download fails.

        A configured ``duckdb_extension_directory`` is deliberately NOT treated as
        an "offline, never download" switch: its mere presence does not guarantee
        the correct binary is staged there, so gating ``INSTALL`` on it would
        silently disable profiling against a stale/mismatched directory. Always
        attempting ``INSTALL`` lets a correct directory skip the network while a
        missing/wrong one self-heals (or fails loudly).

        We must NOT probe with a bare ``LOAD`` first: this runs inside the
        ``engine.begin()`` transaction that materializes the profile table, and a
        failed ``LOAD`` (extension not yet present) aborts that transaction in
        DuckDB — the ``INSTALL`` retry would then fail with "current transaction
        is aborted" and nothing would download. ``INSTALL ...; LOAD ...`` in one
        statement keeps the download path on a clean transaction.
        """
        if extension in self._loaded_extensions:
            return
        try:
            conn.execute(sa.text(f"INSTALL {extension}; LOAD {extension};"))
        except Exception as e:
            raise DuckDBExtensionError(
                f"Could not load or install the DuckDB '{extension}' extension. "
                f"DuckDB downloads it on first use, which fails in air-gapped "
                f"environments. Pre-stage the matching '{extension}.duckdb_extension' "
                f"binary (for your DuckDB version and platform) and point "
                f"`profiling.duckdb_extension_directory` at it "
                f"({type(e).__name__}: {e})."
            ) from e
        self._loaded_extensions.add(extension)

    def _ensure_s3_secret(self, conn: sa.engine.Connection) -> None:
        """Create the DuckDB S3 secret for credentialed remote reads (once)."""
        if self._secrets_done or self.aws_config is None or self.platform != "s3":
            return
        aws = self.aws_config
        has_explicit_keys = bool(aws.aws_access_key_id and aws.aws_secret_access_key)
        if not has_explicit_keys:
            # Without explicit keys the secret uses `PROVIDER credential_chain`
            # (resolve from the ambient AWS chain: instance profile, role, env,
            # ~/.aws). That provider lives in the `aws` extension, not httpfs.
            self._load_extension(conn, "aws")
        conn.execute(sa.text(build_s3_secret_sql(aws)))
        self._secrets_done = True

    def _ensure_format_extension(self, conn: sa.engine.Connection, ext: str) -> None:
        """Load the DuckDB extension required for a file format (e.g. avro)."""
        extension = _FORMAT_EXTENSIONS.get(ext)
        if extension:
            self._load_extension(conn, extension)

    @staticmethod
    def _resolve_ext(
        table_data: "TableData", path_spec: Optional["PathSpec"]
    ) -> str:
        """Resolve the file format the same way schema inference does
        (S3Source._get_inferrer): the S3 content-type takes precedence, then the
        filename extension, then the path_spec's ``default_extension``. This lets
        extension-less Spark/Hive files (``part-00000``) profile instead of being
        skipped, matching how their schema is already inferred.
        """
        from_content = _CONTENT_TYPE_EXT.get(table_data.content_type or "")
        file_ext = os.path.splitext(table_data.full_path)[1].lstrip(".").lower()
        default_ext = (
            path_spec.default_extension.lower()
            if path_spec is not None and path_spec.default_extension
            else ""
        )
        return from_content or file_ext or default_ext

    def _path_and_ext(
        self, table_data: "TableData", path_spec: Optional["PathSpec"] = None
    ) -> tuple[str, str]:
        """Return (resolved_path, lowercase_extension_without_dot).

        For partitioned tables the table_path is a directory prefix (e.g.
        ``s3://bucket/data/my_table`` or ``/local/data/my_table``). DuckDB
        accepts bare local directories natively for parquet, but remote paths
        need an explicit glob so httpfs can enumerate the objects.  We append
        ``/**/*.<ext>`` for remote partitioned paths to handle both cases
        uniformly.
        """
        partitions = table_data.partitions
        ext = self._resolve_ext(table_data, path_spec)
        file_ext = os.path.splitext(table_data.full_path)[1].lstrip(".").lower()
        if not ext:
            # Unknown format (no extension, content-type, or default_extension) —
            # return the concrete path; _open_relation reports it as unsupported.
            return table_data.full_path, ext
        if partitions and file_ext:
            # Only build an extension glob when the files actually carry that
            # extension; for extension-less files a `*.ext` glob would match
            # nothing, so fall back to the concrete sample path.
            path: str = table_data.table_path
            if not path.endswith(f".{ext}") and not path.endswith("*"):
                path = f"{path.rstrip('/')}/**/*.{ext}"
        else:
            path = table_data.full_path
        return path, ext

    def _estimate_row_count(
        self, conn: sa.engine.Connection, ext: str, path: str
    ) -> int:
        """Row count via the relational API (no SQL string built; `_open_relation`
        is the single source of per-format reader logic)."""
        raw = self._raw_connection(conn)
        result = self._open_relation(raw, ext, path).aggregate("count(*)").fetchone()
        return int(result[0]) if result and result[0] is not None else 0

    @staticmethod
    def _is_nested_duckdb_type(column_type: str) -> bool:
        """True for DuckDB nested types (list/struct/map/union/json).

        duckdb-engine cannot reflect these to a SQLAlchemy type (they come back
        as NullType), so the profiler skips them. We cast them to JSON text so
        they reflect as VARCHAR and get null/unique-count + sample-value profiling.
        """
        t = column_type.upper()
        return "[]" in t or t.startswith(("STRUCT", "MAP", "UNION")) or t == "JSON"

    def _build_select_list(
        self, conn: sa.engine.Connection, ext: str, path: str
    ) -> str:
        """Build the SELECT-list projection, casting nested columns to JSON text.

        Column names/types come from the relation's ``.columns``/``.types`` (no
        ``DESCRIBE`` query). Scalar columns pass through unchanged; nested columns
        are rendered as clean JSON strings via ``CAST(CAST(col AS JSON) AS
        VARCHAR)`` so the SQLAlchemy profiler can compute statistics on them.
        Column names are identifier-quoted. ``"*"`` means "no projection".
        """
        raw = self._raw_connection(conn)
        rel = self._open_relation(raw, ext, path)
        columns = rel.columns
        column_types = [str(t) for t in rel.types]
        if not columns:
            return "*"
        parts = []
        for name, column_type in zip(columns, column_types, strict=True):
            quoted = '"' + name.replace('"', '""') + '"'
            if self._is_nested_duckdb_type(column_type):
                parts.append(f"CAST(CAST({quoted} AS JSON) AS VARCHAR) AS {quoted}")
            else:
                parts.append(quoted)
        return ", ".join(parts)

    @staticmethod
    def _raw_connection(conn: sa.engine.Connection) -> "duckdb.DuckDBPyConnection":
        """Return the underlying ``duckdb.DuckDBPyConnection`` behind a SQLAlchemy
        connection, so we can use DuckDB's relational (Python) API. duckdb-engine
        wraps the real connection in a private attribute; access it defensively
        and fail loudly if that internal ever changes."""
        dbapi = conn.connection.dbapi_connection
        raw = getattr(dbapi, "_ConnectionWrapper__c", None)
        if not isinstance(raw, duckdb.DuckDBPyConnection):
            raise RuntimeError(
                "Could not access the underlying DuckDB connection for profiling "
                f"(duckdb-engine internals changed: {type(dbapi).__name__})."
            )
        return raw

    def _open_relation(
        self, raw: "duckdb.DuckDBPyConnection", ext: str, path: str
    ) -> "duckdb.DuckDBPyRelation":
        """Open the source as a DuckDB relation via the Python relational API.

        The file path is passed as a native Python argument — it is never
        concatenated into a SQL string — and the per-format reader options match
        the SQL table-functions this replaced. Avro has no ``read_avro()`` method,
        so ``table_function`` passes the path as a bound argument instead.
        """
        if ext == "parquet":
            return raw.read_parquet(path, union_by_name=True)
        if ext == "csv":
            return raw.read_csv(path, strict_mode=False)
        if ext == "tsv":
            return raw.read_csv(path, sep="\t", strict_mode=False)
        if ext in ("json", "jsonl"):
            return raw.read_json(path)
        if ext == "avro":
            return raw.table_function("read_avro", [path])
        raise ValueError(f"Unsupported format for DuckDB profiling: {ext!r}")

    def _create_profile_table(
        self,
        conn: sa.engine.Connection,
        table: str,
        ext: str,
        path: str,
        select_list: str,
        sample_rows: Optional[int] = None,
    ) -> None:
        """Materialize the source rows into a temp DuckDB table via DuckDB's
        relational API.

        The file path and the destination table name are native Python arguments,
        so no SQL query string is built from external input. Materializing also
        reads the source once instead of re-reading it on every per-column
        profiler scan (a win for remote object stores). ``select_list`` is the
        identifier-quoted projection (scalar columns plus nested→JSON casts);
        ``"*"`` means "no projection". (The caller drops any pre-existing
        ``profile_target`` before this runs.)

        Sampling uses a reservoir ``USING SAMPLE`` (a *random* sample) rather than
        a first-n ``.limit()`` — on append-/date-ordered data-lake files, first-n
        would bias every statistic (min/max/mean/quantiles) toward the oldest
        rows. DuckDB's sample clause accepts only a constant count (no bind
        parameter), so the validated ``int`` sample size is the single inlined
        value; it is never external input. The relation is registered under a
        fixed internal name and read via an otherwise-constant query.
        """
        raw = self._raw_connection(conn)
        rel = self._open_relation(raw, ext, path)
        if select_list != "*":
            rel = rel.project(select_list)
        if sample_rows is None:
            rel.to_table(table)
            return
        raw.register("__datahub_profile_src", rel)
        try:
            # `table` is the fixed internal constant passed by the caller; the only
            # interpolated value is the validated int sample size.
            raw.execute(
                f"CREATE OR REPLACE TABLE {table} AS "
                "SELECT * FROM __datahub_profile_src "
                f"USING SAMPLE {int(sample_rows)} ROWS"
            )
        finally:
            raw.unregister("__datahub_profile_src")

    def get_table_profile(
        self,
        table_data: "TableData",
        dataset_urn: str,
        path_spec: Optional["PathSpec"] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Profile one table and yield a MetadataWorkUnit containing the profile.

        ``path_spec`` (when provided) supplies ``default_extension`` so
        extension-less files resolve to a format, mirroring schema inference.
        """
        display_name: str = table_data.display_name
        try:
            path, ext = self._path_and_ext(table_data, path_spec)
        except Exception as e:
            self.report.report_warning(
                f"DuckDB profiling failed to resolve path for {dataset_urn}",
                context=display_name,
                exc=e,
            )
            return

        table_name = "profile_target"
        engine = self._engine_lazy()
        # Fresh report per call so warnings from a previous table don't
        # get re-forwarded on subsequent calls.
        profiler_report = SQLSourceReport()
        row_estimate: Optional[int] = None
        timer = PerfTimer()
        timer.start()
        try:
            with engine.begin() as conn:
                # Drop any table left by a previous file up front, so a failure
                # later in this call can never leave a stale `profile_target` to
                # be reflected/profiled under the next file's URN.
                conn.execute(sa.text("DROP TABLE IF EXISTS profile_target"))
                self._apply_extension_directory(conn)
                if self._is_remote(path):
                    self._load_extension(conn, "httpfs")
                    self._ensure_s3_secret(conn)
                self._ensure_format_extension(conn, ext)
                select_list = self._build_select_list(conn, ext, path)
                limit = self.profiling_config.profile_table_row_limit
                sample_rows: Optional[int] = None
                if self.profiling_config.use_sampling and limit:
                    count = self._estimate_row_count(conn, ext, path)
                    if count > limit:
                        row_estimate = count
                        sample_rows = int(self.profiling_config.sample_size)
                        self.report.report_warning(
                            f"Table exceeds profile_table_row_limit ({limit}); profiled a sample of {sample_rows} rows.",
                            context=dataset_urn,
                        )
                self._create_profile_table(
                    conn, table_name, ext, path, select_list, sample_rows
                )
            # engine.begin() auto-commits on __exit__; the table is now persisted
            # in the on-disk database and visible to any subsequent connection.

            # Reflect the table to check if max_number_of_fields_to_profile will
            # silently drop columns, and surface a warning so operators see it in
            # the run summary even when report_dropped_profiles is False (default).
            max_fields = self.profiling_config.max_number_of_fields_to_profile
            if max_fields is not None:
                reflected = sa.Table(
                    table_name, sa.MetaData(), autoload_with=engine, schema=None
                )
                if len(reflected.columns) > max_fields:
                    self.report.report_warning(
                        "Table has more columns than "
                        f"max_number_of_fields_to_profile ({max_fields}); some "
                        "columns were dropped from the profile.",
                        context=dataset_urn,
                        title="Profile columns truncated",
                    )

            profiler = SQLAlchemyProfiler(
                conn=engine,
                report=profiler_report,
                config=self.profiling_config,
                platform="duckdb",
            )
            request = ProfilerRequest(
                pretty_name=display_name,
                batch_kwargs={"schema": None, "table": table_name},
            )
            # Materialize the profiles (the actual profiling work) and stop the
            # timer before yielding, so the recorded per-table duration measures
            # profiling — not the downstream sink time during the yields.
            profiles = list(
                profiler.generate_profiles(
                    [request], max_workers=1, platform="duckdb"
                )
            )
            timer.finish()
            for _req, profile in profiles:
                if profile is not None:
                    if row_estimate is not None:
                        profile.rowCount = row_estimate
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=profile
                    ).as_workunit()

        except DuckDBExtensionError as e:
            # A required extension (httpfs/aws/avro) could not be installed or
            # loaded — structural and platform-wide (it recurs for every table),
            # so surface it as a failure (deduped by title), not a per-table
            # warning. (DuckDBExtensionError subclasses RuntimeError, so this arm
            # must precede the RuntimeError arm below.)
            self.report.report_failure(
                "A required DuckDB extension could not be loaded; profiling is "
                "unavailable.",
                context=dataset_urn,
                title="DuckDB profiling unavailable",
                exc=e,
            )
        except RuntimeError as e:
            # A RuntimeError here is structural, not data-specific — e.g.
            # _raw_connection could not reach the underlying DuckDB connection
            # (duckdb-engine internals changed). It would recur for every table,
            # so surface it as a failure (deduped by title) rather than burying a
            # platform-wide breakage in per-table warnings.
            self.report.report_failure(
                "DuckDB profiling could not access the database connection; "
                "no tables can be profiled.",
                context=dataset_urn,
                title="DuckDB profiling unavailable",
                exc=e,
            )
        except Exception as e:
            self.report.report_warning(
                f"DuckDB profiling failed for {dataset_urn}",
                context=display_name,
                exc=e,
            )
        finally:
            # Record this table's profiling duration on the shared list (drives
            # the run summary + telemetry percentiles).
            if self._times_taken is not None:
                self._times_taken.append(timer.elapsed_seconds())
            # Fold the profiler-local report's entries into the main DataLake
            # report so operators see them in the run summary. Preserve the
            # `title` (category) and forward failures as failures, not warnings,
            # so error-level entries are not silently downgraded or dropped.
            for entry in profiler_report.warnings:
                self.report.report_warning(
                    entry.message,
                    context=", ".join(entry.context) if entry.context else None,
                    title=entry.title,
                )
            for entry in profiler_report.failures:
                self.report.report_failure(
                    entry.message,
                    context=", ".join(entry.context) if entry.context else None,
                    title=entry.title,
                )

    def close(self) -> None:
        """Dispose the SQLAlchemy engine and remove the temporary DuckDB file."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self._finalizer()  # idempotent; removes the temp dir
