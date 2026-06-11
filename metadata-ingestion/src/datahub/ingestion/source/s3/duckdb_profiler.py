"""DuckDB-based profiler for data-lake sources (replaces Spark/Deequ)."""

import logging
import os
import shutil
import tempfile
from typing import Iterable, Optional

import sqlalchemy as sa

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
    "csv": "read_csv_auto('{path}')",
    "tsv": "read_csv_auto('{path}', delim='\\t')",
    "json": "read_json_auto('{path}')",
    "jsonl": "read_json_auto('{path}')",
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

    def _path_and_ext(self, table_data: object) -> tuple[str, str]:
        """Return (resolved_path, lowercase_extension_without_dot).

        For partitioned tables the table_path is a directory prefix (e.g.
        ``s3://bucket/data/my_table`` or ``/local/data/my_table``). DuckDB
        accepts bare local directories natively for parquet, but remote paths
        need an explicit glob so httpfs can enumerate the objects.  We append
        ``/**/*.<ext>`` for remote partitioned paths to handle both cases
        uniformly.
        """
        partitions = getattr(table_data, "partitions", None)
        ext = os.path.splitext(table_data.full_path)[1].lstrip(".").lower()  # type: ignore[attr-defined]
        if partitions:
            path: str = table_data.table_path  # type: ignore[attr-defined]
            # Remote paths need a glob; local directories work without one but
            # a glob is also valid and avoids relying on DuckDB's auto-detect.
            if not path.endswith(f".{ext}") and not path.endswith("*"):
                path = f"{path.rstrip('/')}/**/*.{ext}"
        else:
            path = table_data.full_path  # type: ignore[attr-defined]
        return path, ext

    def _reader_expr(self, path: str, ext: str) -> str:
        template = _READERS.get(ext)
        if template is None:
            raise ValueError(f"Unsupported format for DuckDB profiling: {ext!r}")
        # Escape single quotes so the path is safe inside a DuckDB SQL string.
        return template.format(path=path.replace("'", "''"))

    def get_table_profile(
        self, table_data: object, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Profile one table and yield a MetadataWorkUnit containing the profile."""
        display_name: str = getattr(table_data, "display_name", str(table_data))
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
        try:
            reader = self._reader_expr(path, ext)
            with engine.begin() as conn:
                if self._is_remote(path):
                    self._ensure_remote_setup(conn)
                conn.execute(
                    sa.text(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {reader}")
                )
            # engine.begin() auto-commits on __exit__; the view is now persisted
            # in the on-disk database and visible to any subsequent connection.

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
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=profile
                    ).as_workunit()

        except Exception as e:
            self.report.report_warning(
                f"DuckDB profiling failed for {dataset_urn}: {type(e).__name__}: {e}",
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
        shutil.rmtree(self._tmpdir, ignore_errors=True)
