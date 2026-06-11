"""DuckDB-based profiler for data-lake sources (replaces Spark/Deequ)."""

import logging
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, Iterable, Optional, Set

import sqlalchemy as sa

if TYPE_CHECKING:
    from datahub.ingestion.source.s3.source import TableData

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.profiling.common import ProfilerRequest
from datahub.ingestion.source.s3.duckdb_secrets import build_s3_secret_sql
from datahub.ingestion.source.s3.report import DataLakeSourceReport
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
    SQLAlchemyProfiler,
)

logger = logging.getLogger(__name__)

_REMOTE_SCHEMES = (
    "s3://",
    "s3a://",
    "gs://",
    "gcs://",
    "az://",
    "abfs://",
    "abfss://",
)

# Extension (lowercased, no dot) -> DuckDB reader template with a {path} placeholder.
_READERS = {
    "parquet": "read_parquet('{path}', union_by_name=true)",
    # strict_mode=false tolerates messy real-world CSVs (multi-line quoted
    # headers, ragged rows, unicode) that DuckDB's strict sniffer rejects but
    # Spark/Deequ used to read.
    "csv": "read_csv_auto('{path}', strict_mode=false)",
    "tsv": "read_csv_auto('{path}', delim='\\t', strict_mode=false)",
    "json": "read_json_auto('{path}')",
    "jsonl": "read_json_auto('{path}')",
    "avro": "read_avro('{path}')",
}

# Map a file extension to the DuckDB core extension required to read it.
# Unlike httpfs (remote-only), format extensions are needed for local files too.
_FORMAT_EXTENSIONS = {"avro": "avro"}


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
        report: DataLakeSourceReport,
        profiling_config: GEProfilingConfig,
        platform: str = "s3",
    ) -> None:
        self.aws_config = aws_config
        self.report = report
        self.profiling_config = profiling_config
        self.platform = platform
        self._tmpdir = tempfile.mkdtemp(prefix="datahub-duckdb-profile-")
        self._db_path = os.path.join(self._tmpdir, "profile.duckdb")
        self._engine: Optional[sa.engine.Engine] = None
        self._httpfs_loaded = False
        self._secrets_done = False
        self._loaded_extensions: Set[str] = set()

    def _engine_lazy(self) -> sa.engine.Engine:
        if self._engine is None:
            self._engine = sa.create_engine(f"duckdb:///{self._db_path}")
        return self._engine

    def _is_remote(self, path: str) -> bool:
        return path.startswith(_REMOTE_SCHEMES)

    def _ensure_remote_setup(self, conn: sa.engine.Connection) -> None:
        """Install and load httpfs, and create an S3 secret when needed."""
        if not self._httpfs_loaded:
            conn.execute(sa.text("INSTALL httpfs; LOAD httpfs;"))
            self._httpfs_loaded = True
        if (
            not self._secrets_done
            and self.aws_config is not None
            and self.platform == "s3"
        ):
            conn.execute(sa.text(build_s3_secret_sql(self.aws_config)))
            self._secrets_done = True

    def _ensure_format_extension(self, conn: sa.engine.Connection, ext: str) -> None:
        """Load the DuckDB core extension required for a file format (e.g. avro).

        Unlike httpfs (remote-only), format extensions are needed for local files
        too, so this is always called before building the view.
        """
        extension = _FORMAT_EXTENSIONS.get(ext)
        if extension and extension not in self._loaded_extensions:
            conn.execute(sa.text(f"INSTALL {extension}; LOAD {extension};"))
            self._loaded_extensions.add(extension)

    def _path_and_ext(self, table_data: "TableData") -> tuple[str, str]:
        """Return (resolved_path, lowercase_extension_without_dot).

        For partitioned tables the table_path is a directory prefix (e.g.
        ``s3://bucket/data/my_table`` or ``/local/data/my_table``). DuckDB
        accepts bare local directories natively for parquet, but remote paths
        need an explicit glob so httpfs can enumerate the objects.  We append
        ``/**/*.<ext>`` for remote partitioned paths to handle both cases
        uniformly.
        """
        partitions = table_data.partitions
        ext = os.path.splitext(table_data.full_path)[1].lstrip(".").lower()
        if not ext:
            # No extension on the sample file — can't build a meaningful glob.
            # Fall through to the concrete full_path and let _reader_expr raise
            # an "unsupported format" error as usual.
            return table_data.full_path, ext
        if partitions:
            path: str = table_data.table_path
            # Remote paths need a glob; local directories work without one but
            # a glob is also valid and avoids relying on DuckDB's auto-detect.
            if not path.endswith(f".{ext}") and not path.endswith("*"):
                path = f"{path.rstrip('/')}/**/*.{ext}"
        else:
            path = table_data.full_path
        return path, ext

    def _reader_expr(self, path: str, ext: str) -> str:
        template = _READERS.get(ext)
        if template is None:
            raise ValueError(f"Unsupported format for DuckDB profiling: {ext!r}")
        # Escape single quotes so the path is safe inside a DuckDB SQL string.
        return template.format(path=path.replace("'", "''"))

    def _estimate_row_count(self, conn: sa.engine.Connection, reader: str) -> int:
        result = conn.execute(sa.text(f"SELECT COUNT(*) FROM {reader}")).scalar()
        return int(result) if result is not None else 0

    @staticmethod
    def _is_nested_duckdb_type(column_type: str) -> bool:
        """True for DuckDB nested types (list/struct/map/union/json).

        duckdb-engine cannot reflect these to a SQLAlchemy type (they come back
        as NullType), so the profiler skips them. We cast them to JSON text so
        they reflect as VARCHAR and get null/unique-count + sample-value profiling.
        """
        t = column_type.upper()
        return "[]" in t or t.startswith(("STRUCT", "MAP", "UNION")) or t == "JSON"

    def _build_select_list(self, conn: sa.engine.Connection, reader: str) -> str:
        """Build the view's SELECT list, casting nested columns to JSON text.

        Scalar columns pass through unchanged; nested columns are rendered as
        clean JSON strings via ``CAST(CAST(col AS JSON) AS VARCHAR)`` so the
        SQLAlchemy profiler can compute statistics on them.
        """
        rows = conn.execute(sa.text(f"DESCRIBE SELECT * FROM {reader}")).fetchall()
        if not rows:
            return "*"
        parts = []
        for row in rows:
            name, column_type = row[0], row[1]
            quoted = '"' + name.replace('"', '""') + '"'
            if self._is_nested_duckdb_type(column_type):
                parts.append(f"CAST(CAST({quoted} AS JSON) AS VARCHAR) AS {quoted}")
            else:
                parts.append(quoted)
        return ", ".join(parts)

    def _create_profile_view(
        self,
        conn: sa.engine.Connection,
        view: str,
        reader: str,
        select_list: str,
        sample_rows: Optional[int] = None,
    ) -> None:
        suffix = f" USING SAMPLE {sample_rows} ROWS" if sample_rows is not None else ""
        conn.execute(
            sa.text(
                f"CREATE OR REPLACE VIEW {view} AS "
                f"SELECT {select_list} FROM {reader}{suffix}"
            )
        )

    def get_table_profile(
        self, table_data: "TableData", dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Profile one table and yield a MetadataWorkUnit containing the profile."""
        display_name: str = table_data.display_name
        try:
            path, ext = self._path_and_ext(table_data)
        except Exception as e:
            self.report.report_warning(
                f"DuckDB profiling failed to resolve path for {dataset_urn}",
                context=display_name,
                exc=e,
            )
            return

        view = "profile_target"
        engine = self._engine_lazy()
        # Fresh report per call so warnings from a previous table don't
        # get re-forwarded on subsequent calls.
        profiler_report = SQLSourceReport()
        row_estimate: Optional[int] = None
        try:
            reader = self._reader_expr(path, ext)
            with engine.begin() as conn:
                if self._is_remote(path):
                    self._ensure_remote_setup(conn)
                self._ensure_format_extension(conn, ext)
                select_list = self._build_select_list(conn, reader)
                limit = self.profiling_config.profile_table_row_limit
                sample_rows: Optional[int] = None
                if self.profiling_config.use_sampling and limit:
                    count = self._estimate_row_count(conn, reader)
                    if count > limit:
                        row_estimate = count
                        sample_rows = int(self.profiling_config.sample_size)
                        self.report.report_warning(
                            f"Table exceeds profile_table_row_limit ({limit}); profiled a sample of {sample_rows} rows.",
                            context=dataset_urn,
                        )
                self._create_profile_view(conn, view, reader, select_list, sample_rows)
            # engine.begin() auto-commits on __exit__; the view is now persisted
            # in the on-disk database and visible to any subsequent connection.

            # Reflect the view to check if max_number_of_fields_to_profile will
            # silently drop columns.  Report via the DataLake report so operators
            # can see the drop in the run summary even when report_dropped_profiles
            # is False (the default).
            max_fields = self.profiling_config.max_number_of_fields_to_profile
            if max_fields is not None:
                reflected = sa.Table(
                    view, sa.MetaData(), autoload_with=engine, schema=None
                )
                if len(reflected.columns) > max_fields:
                    self.report.report_file_dropped(dataset_urn)

            profiler = SQLAlchemyProfiler(
                conn=engine,
                report=profiler_report,
                config=self.profiling_config,
                platform="duckdb",
            )
            request = ProfilerRequest(
                pretty_name=display_name,
                batch_kwargs={"schema": None, "table": view},
            )
            for _req, profile in profiler.generate_profiles(
                [request], max_workers=1, platform="duckdb"
            ):
                if profile is not None:
                    if row_estimate is not None:
                        profile.rowCount = row_estimate
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=profile
                    ).as_workunit()

        except Exception as e:
            self.report.report_warning(
                f"DuckDB profiling failed for {dataset_urn}",
                context=display_name,
                exc=e,
            )
        finally:
            # Fold any warnings from the profiler-local report into the main
            # DataLake report so operators see them in the run summary.
            for entry in profiler_report.warnings:
                self.report.report_warning(
                    entry.message,
                    context=", ".join(entry.context) if entry.context else None,
                )

    def close(self) -> None:
        """Dispose the SQLAlchemy engine and remove the temporary DuckDB file."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        shutil.rmtree(self._tmpdir, ignore_errors=True)
