"""Shared builders for the Redshift unit tests.

Everything that more than one module needs lives here: cursor/connection
stubs for the redshift_connector boundary, typed record builders, factories
for the source / extractors / profiler, and autospecced stand-ins for the
DataHub-internal collaborators (skip handlers, the lineage extractor and its
aggregator). Test modules import the builders from here instead of from each
other.
"""

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.profiling.config import ProfilingConfig
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.lineage import RedshiftSqlLineage
from datahub.ingestion.source.redshift.profile import RedshiftProfiler
from datahub.ingestion.source.redshift.redshift import RedshiftSource
from datahub.ingestion.source.redshift.redshift_schema import (
    LineageRow,
    RedshiftColumn,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.redshift.usage import RedshiftUsageExtractor
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
    RedundantUsageRunSkipHandler,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

DB = "dev"
HOST_PORT = "localhost:5439"
START = datetime(2024, 1, 1, tzinfo=timezone.utc)
END = datetime(2024, 1, 2, tzinfo=timezone.utc)

AllTables = Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]]

_T = TypeVar("_T")


# --------------------------------------------------------------------------- #
# redshift_connector boundary
# --------------------------------------------------------------------------- #


def make_cursor(
    columns: Sequence[str], rows: Sequence[Tuple], *, paged: bool = True
) -> MagicMock:
    """A redshift_connector cursor that serves ``rows`` through fetchall(),
    fetchone() and (when ``paged``) a single fetchmany() page."""
    cursor = MagicMock()
    cursor.description = [(c,) for c in columns]
    cursor.fetchall.return_value = list(rows)
    cursor.fetchone.return_value = rows[0] if rows else None
    if paged:
        cursor.fetchmany.side_effect = [list(rows), []]
    return cursor


def make_connection(cursor: Optional[MagicMock] = None) -> MagicMock:
    """A redshift_connector connection whose cursor() returns ``cursor`` both
    directly and as a context manager."""
    cursor = cursor if cursor is not None else MagicMock()
    cursor.__enter__.return_value = cursor
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #


def make_table(
    name: str = "t1", schema: str = "public", **overrides: Any
) -> RedshiftTable:
    table = RedshiftTable(
        name=name, schema=schema, type="BASE TABLE", created=None, comment=None
    )
    return replace(table, **overrides)


def make_column(name: str, data_type: str, **overrides: Any) -> RedshiftColumn:
    column = RedshiftColumn(
        name=name,
        ordinal_position=1,
        data_type=data_type,
        comment=None,
        is_nullable=True,
    )
    return replace(column, **overrides)


def make_lineage_row(**overrides: Any) -> LineageRow:
    row = LineageRow(
        source_schema=None,
        source_table=None,
        target_schema=None,
        target_table=None,
        ddl=None,
        filename=None,
        timestamp=datetime(2024, 1, 1, 12, tzinfo=timezone.utc),
        session_id="42",
    )
    return replace(row, **overrides)


def redshift_urn(table: str, db: str = DB) -> str:
    return make_dataset_urn("redshift", f"{db}.{table}")


def aspects_of(
    workunits: Iterable[MetadataWorkUnit], aspect_type: Type[_T]
) -> List[_T]:
    return [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, aspect_type)
    ]


# --------------------------------------------------------------------------- #
# Config / source / extractor factories
# --------------------------------------------------------------------------- #


def make_config(**overrides: object) -> RedshiftConfig:
    values: Dict[str, object] = {
        "host_port": HOST_PORT,
        "database": DB,
        "start_time": START,
        "end_time": END,
    }
    values.update(overrides)
    return RedshiftConfig.model_validate(values)


def make_source(**overrides: object) -> RedshiftSource:
    return RedshiftSource(make_config(**overrides), ctx=PipelineContext(run_id="test"))


def make_lineage_extractor(
    *,
    known_tables: Optional[Sequence[str]] = None,
    skip_handler: Optional[RedundantLineageRunSkipHandler] = None,
    **overrides: object,
) -> RedshiftSqlLineage:
    config = make_config(**overrides)
    extractor = RedshiftSqlLineage(
        config,
        RedshiftReport(),
        PipelineContext(run_id="test"),
        config.database,
        redundant_run_skip_handler=skip_handler,
    )
    extractor.known_urns = {redshift_urn(name) for name in known_tables or []}
    return extractor


def make_usage_extractor(
    *,
    skip_handler: Optional[RedundantUsageRunSkipHandler] = None,
    **overrides: object,
) -> RedshiftUsageExtractor:
    values: Dict[str, object] = {
        "email_domain": "example.com",
        "include_usage_statistics": True,
        "include_operational_stats": False,
    }
    values.update(overrides)
    return RedshiftUsageExtractor(
        config=make_config(**values),
        connection=MagicMock(),
        report=RedshiftReport(),
        dataset_urn_builder=lambda table: make_dataset_urn("redshift", table),
        redundant_run_skip_handler=skip_handler,
    )


def make_profiler(
    *, is_shared_database: bool = False, **overrides: object
) -> RedshiftProfiler:
    values: Dict[str, object] = {"profiling": ProfilingConfig(enabled=True)}
    values.update(overrides)
    report = RedshiftReport()
    report.is_shared_database = is_shared_database
    return RedshiftProfiler(
        config=make_config(**values), report=report, state_handler=None
    )


# --------------------------------------------------------------------------- #
# Autospecced DataHub-internal collaborators
# --------------------------------------------------------------------------- #


def stub_lineage_extractor(source: RedshiftSource) -> MagicMock:
    """A RedshiftSqlLineage stand-in for driving RedshiftSource.extract_lineage_v2
    without parsing anything; its aggregator is specced too so a renamed method
    fails loudly instead of being auto-created."""
    extractor = create_autospec(RedshiftSqlLineage, instance=True)
    extractor.aggregator = create_autospec(SqlParsingAggregator, instance=True)
    extractor.start_time = source.config.start_time
    extractor.end_time = source.config.end_time
    extractor.generate.return_value = iter([])
    return extractor


@dataclass
class ExtractMetadataRun:
    workunits: List[MetadataWorkUnit]
    extract_lineage: MagicMock


def run_extract_metadata(source: RedshiftSource) -> ExtractMetadataRun:
    """Drive RedshiftSource._extract_metadata with the catalog reads and the
    usage / lineage extractors stubbed out, so only the orchestration runs."""
    with (
        patch.object(source, "cache_tables_and_views", return_value=None),
        patch.object(source, "process_schemas", return_value=iter([])),
        patch.object(source, "extract_usage", return_value=iter([])),
        patch.object(
            source, "extract_lineage_v2", return_value=iter([])
        ) as extract_lineage,
    ):
        workunits = list(source._extract_metadata(MagicMock(), DB))
    return ExtractMetadataRun(workunits=workunits, extract_lineage=extract_lineage)


@pytest.fixture
def lineage_skip_handler() -> MagicMock:
    return create_autospec(RedundantLineageRunSkipHandler, instance=True)


@pytest.fixture
def usage_skip_handler() -> MagicMock:
    return create_autospec(RedundantUsageRunSkipHandler, instance=True)


@pytest.fixture
def all_tables() -> AllTables:
    return {DB: {"public": [make_table("users"), make_table("orders")]}}
