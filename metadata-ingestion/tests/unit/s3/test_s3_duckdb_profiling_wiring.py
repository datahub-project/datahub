"""End-to-end wiring test: S3Source with DuckDB profiling over local files.

Verifies that:
- S3Source builds a DuckDBProfiler (not SparkProfiler) when profiling is enabled.
- A DatasetProfileClass aspect is emitted for a local parquet file.
- S3Source.close() cleans up the profiler's temp directory.
"""

import os
from typing import List

import duckdb

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.s3.duckdb_profiler import DuckDBProfiler
from datahub.ingestion.source.s3.source import S3Source
from datahub.metadata.schema_classes import DatasetProfileClass


def _write_parquet(path: str) -> None:
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT * FROM (VALUES (10,'x'),(20,'y'),(30,'z')) AS v(val, label)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    con.close()


def _make_local_source(include: str) -> S3Source:
    return S3Source.create(
        config_dict={
            "path_spec": {"include": include},
            "platform": "file",
            "profiling": {"enabled": True},
        },
        ctx=PipelineContext(run_id="test-local-duckdb-profiling"),
    )


def _collect_profile_aspects(source: S3Source) -> List[DatasetProfileClass]:
    profiles = []
    for wu in source.get_workunits():
        aspect = getattr(wu.metadata, "aspect", None)
        if isinstance(aspect, DatasetProfileClass):
            profiles.append(aspect)
    return profiles


def test_source_uses_duckdb_profiler(tmp_path):
    """Profiling-enabled source must instantiate DuckDBProfiler, not SparkProfiler."""
    parquet_file = str(tmp_path / "data.parquet")
    _write_parquet(parquet_file)
    source = _make_local_source(include=str(tmp_path / "*.parquet"))
    assert hasattr(source, "profiler"), (
        "profiler should be set when profiling is enabled"
    )
    assert isinstance(source.profiler, DuckDBProfiler), (
        f"expected DuckDBProfiler, got {type(source.profiler)}"
    )
    source.close()


def test_profile_aspect_emitted_for_local_parquet(tmp_path):
    """A DatasetProfileClass aspect must be produced for a single local parquet file."""
    parquet_file = str(tmp_path / "events.parquet")
    _write_parquet(parquet_file)
    source = _make_local_source(include=str(tmp_path / "*.parquet"))
    profiles = _collect_profile_aspects(source)
    source.close()

    assert len(profiles) >= 1, "expected at least one DatasetProfileClass workunit"
    profile = profiles[0]
    assert profile.rowCount == 3
    assert profile.columnCount == 2


def test_close_cleans_up_profiler_tmpdir(tmp_path):
    """S3Source.close() must remove the DuckDB profiler's temporary directory."""
    parquet_file = str(tmp_path / "data.parquet")
    _write_parquet(parquet_file)
    source = _make_local_source(include=str(tmp_path / "*.parquet"))
    # Trigger workunit generation so the profiler's engine is exercised
    list(source.get_workunits())
    tmpdir = source.profiler._tmpdir
    assert os.path.isdir(tmpdir)
    source.close()
    assert not os.path.isdir(tmpdir), "profiler tmpdir should be removed after close()"


def test_close_is_safe_when_profiling_disabled(tmp_path):
    """close() must not raise when no profiler has been created."""
    parquet_file = str(tmp_path / "data.parquet")
    _write_parquet(parquet_file)
    source = S3Source.create(
        config_dict={
            "path_spec": {"include": str(tmp_path / "*.parquet")},
            "platform": "file",
            "profiling": {"enabled": False},
        },
        ctx=PipelineContext(run_id="test-no-profiling"),
    )
    assert not hasattr(source, "profiler") or source.profiler is None
    source.close()  # must not raise
