"""Shared builders for the Unity Catalog unit tests.

Pure builders (config, SQLAlchemy rows, statement responses, report lookups)
are plain functions; anything that creates mocks or patches module globals is a
fixture. The Databricks WorkspaceClient and the databricks-sql ``connect`` are
the only system boundaries these tests mock.
"""

from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    cast,
)
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    ServiceError,
    StatementResponse,
    StatementState,
    StatementStatus,
)

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import (
    LineageDataSource,
    UnityCatalogSourceConfig,
)
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.ingestion.source.unity.source import UnityCatalogSource

WORKSPACE_URL = "https://my-workspace.cloud.databricks.com"
WAREHOUSE_ID = "wh-1"

# Row objects; untyped so both SA 1.x stubs and SA 2 accept it.
Rows = Sequence[Any]
TagRows = Callable[[str], List[Any]]


def unity_config(**extra: Any) -> UnityCatalogSourceConfig:
    """A valid config with the hive_metastore catalog off; ``extra`` overrides."""
    return UnityCatalogSourceConfig.model_validate(
        {
            "token": "t",
            "workspace_url": WORKSPACE_URL,
            "include_hive_metastore": False,
            **extra,
        }
    )


def sa_rows(columns: List[str], values: List[Tuple[Any, ...]]) -> Rows:
    """Real SQLAlchemy Rows with named columns, as HiveMetastoreProxy._execute_sql
    returns them (not tuples, not databricks Rows)."""
    engine = sa.create_engine("sqlite://")
    with engine.connect() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE out (" + ", ".join(f'"{c}" TEXT' for c in columns) + ")"
        )
        placeholders = ", ".join("?" for _ in columns)
        for row in values:
            conn.exec_driver_sql(f"INSERT INTO out VALUES ({placeholders})", row)
        return conn.execute(sa.text("SELECT * FROM out")).fetchall()


def statement_response(
    state: StatementState, error: Optional[ServiceError] = None
) -> StatementResponse:
    return StatementResponse(
        statement_id="stmt-1", status=StatementStatus(state=state, error=error)
    )


def warning_messages(report: SourceReport) -> Set[str]:
    """The stable keys of the report's warnings (their ``message``), independent
    of emission order and of how many contexts were grouped under each."""
    return {w.message for w in report.warnings}


def failure_messages(report: SourceReport) -> Set[str]:
    return {f.message for f in report.failures}


@pytest.fixture
def workspace_client() -> MagicMock:
    """A WorkspaceClient spec mock pointing at WORKSPACE_URL / WAREHOUSE_ID."""
    ws = MagicMock(spec=WorkspaceClient)
    ws.config.host = WORKSPACE_URL
    ws.config.warehouse_id = WAREHOUSE_ID
    return ws


@pytest.fixture(autouse=True)
def no_real_sql_connection() -> Iterator[MagicMock]:
    """Fail fast on any databricks-sql connection a test did not patch itself.

    The real ``connect`` retries with backoff against the workspace host, so an
    unpatched call does not fail, it hangs the suite. Tests that want SQL
    results patch ``proxy.connect`` themselves, which takes precedence.
    """
    with patch(
        "datahub.ingestion.source.unity.proxy.connect",
        side_effect=AssertionError("unit test opened a real Databricks SQL connection"),
    ) as guard:
        yield guard


@pytest.fixture
def api_proxy() -> Callable[..., UnityCatalogApiProxy]:
    """Factory for a UnityCatalogApiProxy over a mocked client with a fresh report.

    Lineage defaults to the REST API (served by ``api_client.do``): the proxy's
    own AUTO default switches to system tables whenever a warehouse is set,
    which goes through a SQL connection instead.
    """

    def build(ws: MagicMock, **kwargs: Any) -> UnityCatalogApiProxy:
        kwargs.setdefault("lineage_data_source", LineageDataSource.API)
        return UnityCatalogApiProxy(ws, report=UnityCatalogReport(), **kwargs)

    return build


@pytest.fixture
def no_sleep() -> Iterator[MagicMock]:
    """ANALYZE polling sleeps between status checks; stub the clock out."""
    with patch("datahub.ingestion.source.unity.proxy_profiling.time") as fake_time:
        yield fake_time


class FakeSqlRow:
    """The subset of a SQLAlchemy Row the SQLAlchemy profiler reads."""

    def __init__(self, **values: Any) -> None:
        self._values = values

    def _asdict(self) -> Dict[str, Any]:
        return dict(self._values)


class FakeSqlEngine:
    """A SQLAlchemy engine stand-in answering DESCRIBE DETAIL and count(*)."""

    def __init__(
        self,
        size_in_bytes: Optional[int] = None,
        row_count: Optional[int] = None,
        describe_error: Optional[Exception] = None,
        count_error: Optional[Exception] = None,
        no_result_rows: bool = False,
    ) -> None:
        self.executed: List[str] = []
        self._size = size_in_bytes
        self._rows = row_count
        self._describe_error = describe_error
        self._count_error = count_error
        self._no_result_rows = no_result_rows
        self.engine = MagicMock(spec=sa.engine.Engine)
        conn = self.engine.connect.return_value.__enter__.return_value
        conn.dialect.identifier_preparer.quote = lambda name: f"`{name}`"
        conn.execute.side_effect = self._execute

    def _execute(self, statement: Any) -> MagicMock:
        sql = str(statement)
        self.executed.append(sql)
        result = MagicMock()
        if sql.startswith("DESCRIBE DETAIL"):
            if self._describe_error:
                raise self._describe_error
            result.fetchone.return_value = FakeSqlRow(sizeInBytes=self._size)
        else:
            if self._count_error:
                raise self._count_error
            result.fetchone.return_value = FakeSqlRow(numRows=self._rows)
        if self._no_result_rows:
            result.fetchone.return_value = None
        return result


@dataclass
class SourceRun:
    """A completed UnityCatalogSource.get_workunits_internal() run."""

    source: UnityCatalogSource
    workunits: List[MetadataWorkUnit]

    def aspects(self, urn: str, aspect_type: type) -> List[Any]:
        found: List[Any] = []
        for wu in self.workunits:
            mcp = wu.metadata
            if getattr(mcp, "entityUrn", None) != urn:
                continue
            aspect = getattr(mcp, "aspect", None)
            if isinstance(aspect, aspect_type):
                found.append(aspect)
        return found

    def aspect(self, urn: str, aspect_type: type) -> Any:
        aspects = self.aspects(urn, aspect_type)
        assert len(aspects) == 1, f"expected one {aspect_type.__name__} on {urn}"
        return aspects[0]

    def urns(self, aspect_type: type) -> List[str]:
        return [
            cast(str, getattr(wu.metadata, "entityUrn", None))
            for wu in self.workunits
            if isinstance(getattr(wu.metadata, "aspect", None), aspect_type)
        ]


@pytest.fixture
def run_source() -> Callable[..., SourceRun]:
    """Run the source end to end against a mocked WorkspaceClient.

    The databricks-sql ``connect`` used for information_schema tag queries is
    replaced by a cursor whose ``fetchall`` answers from ``tag_rows``. Its
    ``fetchmany`` must return a real empty list: a bare MagicMock there is
    truthy, and the system-tables readers would loop on it forever.
    """

    def run(
        ws: MagicMock,
        config: UnityCatalogSourceConfig,
        tag_rows: Optional[TagRows] = None,
    ) -> SourceRun:
        fake_cursor = MagicMock()
        fake_cursor.fetchall.side_effect = lambda: (
            tag_rows(fake_cursor.execute.call_args[0][0]) if tag_rows else []
        )
        fake_cursor.fetchmany.return_value = []
        fake_connection = MagicMock()
        fake_connection.__enter__.return_value = fake_connection
        fake_connection.cursor.return_value.__enter__.return_value = fake_cursor
        with (
            patch(
                "datahub.ingestion.source.unity.source.create_workspace_client",
                return_value=ws,
            ),
            patch(
                "datahub.ingestion.source.unity.proxy.connect",
                return_value=fake_connection,
            ),
        ):
            source = UnityCatalogSource(ctx=PipelineContext(run_id="t"), config=config)
            workunits = list(source.get_workunits_internal())
            source.close()
        return SourceRun(source=source, workunits=workunits)

    return run
