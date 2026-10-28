"""RedshiftProfiler: which tables are handed to the generic profiler."""

from typing import Dict, List
from unittest.mock import patch

from datahub.ingestion.source.profiling.config import ProfilingConfig
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.profile import RedshiftProfiler
from datahub.ingestion.source.redshift.redshift_schema import RedshiftTable
from datahub.ingestion.source.redshift.report import RedshiftReport


def _profiler(
    profile_external_tables: bool = False,
    profile_table_level_only: bool = False,
    is_shared_database: bool = False,
) -> RedshiftProfiler:
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        profiling=ProfilingConfig(
            enabled=True,
            profile_external_tables=profile_external_tables,
            profile_table_level_only=profile_table_level_only,
        ),
    )
    report = RedshiftReport()
    report.is_shared_database = is_shared_database
    return RedshiftProfiler(config=config, report=report, state_handler=None)


def _tables(table_type: str) -> Dict[str, Dict[str, List[RedshiftTable]]]:
    return {
        "dev": {
            "public": [
                RedshiftTable(
                    name="t",
                    schema="public",
                    type=table_type,
                    created=None,
                    comment=None,
                    rows_count=10,
                    size_in_bytes=100,
                )
            ]
        }
    }


def _profiled_names(profiler: RedshiftProfiler, tables: Dict) -> List[str]:
    with patch.object(
        profiler, "generate_profile_workunits", return_value=iter([])
    ) as gen:
        list(profiler.get_workunits(tables))
    if not gen.called:
        return []
    return [req.pretty_name for req in gen.call_args[0][0]]


def test_external_tables_are_skipped_by_default() -> None:
    profiler = _profiler()
    assert _profiled_names(profiler, _tables("EXTERNAL_TABLE")) == []
    assert profiler.report.profiling_skipped_other["public"] == 1
    assert not profiler.report.warnings


def test_external_tables_need_full_profiling_not_table_level_only() -> None:
    profiler = _profiler(profile_external_tables=True, profile_table_level_only=True)
    assert _profiled_names(profiler, _tables("EXTERNAL_TABLE")) == []
    assert "Skipped profiling for external tables" in [
        w.title for w in profiler.report.warnings
    ]


def test_external_tables_are_profiled_when_explicitly_enabled() -> None:
    profiler = _profiler(profile_external_tables=True)
    assert _profiled_names(profiler, _tables("EXTERNAL_TABLE")) == ["dev.public.t"]


def test_shared_database_tables_are_treated_like_external_tables() -> None:
    profiler = _profiler(is_shared_database=True)
    assert _profiled_names(profiler, _tables("BASE TABLE")) == []
    assert profiler.report.profiling_skipped_other["public"] == 1


def test_schema_pattern_filters_profiled_tables() -> None:
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="dev",
        profiling=ProfilingConfig(enabled=True),
        schema_pattern={"deny": ["public"]},
    )
    profiler = RedshiftProfiler(
        config=config, report=RedshiftReport(), state_handler=None
    )
    assert _profiled_names(profiler, _tables("BASE TABLE")) == []
