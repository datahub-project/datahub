"""Tests for DuckDBProfiler — local-file (no-network) path."""

import os
from datetime import datetime, timezone

import duckdb

from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.s3.duckdb_profiler import DuckDBProfiler
from datahub.ingestion.source.s3.report import DataLakeSourceReport
from datahub.ingestion.source.s3.source import TableData
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


def _table_data(full_path: str) -> TableData:
    return TableData(
        display_name="data.parquet",
        is_s3=False,
        full_path=full_path,
        timestamp=datetime.now(timezone.utc),
        table_path=full_path,
        size_in_bytes=os.path.getsize(full_path),
        number_of_files=1,
    )


def _extract_profile(wus):
    for wu in wus:
        aspect = wu.metadata.aspect if hasattr(wu.metadata, "aspect") else None
        if isinstance(aspect, DatasetProfileClass):
            return aspect
    return None


def test_profiles_local_parquet(tmp_path):
    parquet = _make_parquet(str(tmp_path))
    cfg = GEProfilingConfig(enabled=True)
    profiler = DuckDBProfiler(
        aws_config=None, report=DataLakeSourceReport(), profiling_config=cfg
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
