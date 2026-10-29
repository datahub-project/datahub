"""RedshiftProfiler: which tables are handed to the generic profiler."""

from typing import Dict, List
from unittest.mock import patch

import pytest

from datahub.ingestion.source.profiling.config import ProfilingConfig
from datahub.ingestion.source.redshift.profile import RedshiftProfiler
from datahub.ingestion.source.redshift.redshift_schema import RedshiftTable
from tests.unit.redshift.conftest import DB, make_profiler, make_source, make_table


def _tables(
    table_type: str = "BASE TABLE", name: str = "t", schema: str = "public"
) -> Dict[str, Dict[str, List[RedshiftTable]]]:
    return {
        DB: {
            schema: [
                make_table(
                    name,
                    schema=schema,
                    type=table_type,
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
    profiler = make_profiler()
    assert _profiled_names(profiler, _tables("EXTERNAL_TABLE")) == []
    assert profiler.report.profiling_skipped_other["public"] == 1
    assert not profiler.report.warnings


def test_external_tables_need_full_profiling_not_table_level_only() -> None:
    """Table-level-only stats come from svv_table_info, which has no rows for
    external tables, so that combination is refused with a warning rather than
    silently producing empty profiles."""
    profiler = make_profiler(
        profiling=ProfilingConfig(
            enabled=True, profile_external_tables=True, profile_table_level_only=True
        )
    )
    assert _profiled_names(profiler, _tables("EXTERNAL_TABLE")) == []
    assert "Skipped profiling for external tables" in {
        w.title for w in profiler.report.warnings
    }


def test_external_tables_are_profiled_when_explicitly_enabled() -> None:
    profiler = make_profiler(
        profiling=ProfilingConfig(enabled=True, profile_external_tables=True)
    )
    assert _profiled_names(profiler, _tables("EXTERNAL_TABLE")) == [f"{DB}.public.t"]


def test_shared_database_tables_are_treated_like_external_tables() -> None:
    """Datashare consumers cannot read svv_table_info for shared tables, so a
    shared database follows the external-table rules for every table."""
    profiler = make_profiler(is_shared_database=True)
    assert _profiled_names(profiler, _tables()) == []
    assert profiler.report.profiling_skipped_other["public"] == 1


def test_schema_pattern_filters_profiled_tables() -> None:
    profiler = make_profiler(schema_pattern={"deny": ["public"]})
    assert _profiled_names(profiler, _tables()) == []


@pytest.mark.xfail(
    strict=True,
    reason="RedshiftProfiler.get_dataset_name lowercases `db.schema.table`, so "
    "profiles (and, via the same lowercasing in usage.py, usage and operation "
    "aspects) land on a lowercased URN while schemaMetadata is emitted on the "
    "mixed-case one built by RedshiftSource.gen_dataset_urn.",
)
def test_profile_urn_matches_the_dataset_urn_for_mixed_case_names() -> None:
    profiler = make_profiler()
    names = _profiled_names(profiler, _tables(name="Orders", schema="Sales"))
    assert [profiler.dataset_urn_builder(n) for n in names] == [
        make_source().gen_dataset_urn(f"{DB}.Sales.Orders")
    ]
