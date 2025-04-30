import contextlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar

from sqlalchemy import create_engine

from datahub.emitter.aspect import ASPECT_MAP
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.serialization_helper import post_json_transform
from datahub.ingestion.source.datahub.config import DataHubSourceConfig
from datahub.ingestion.source.datahub.report import DataHubSourceReport
from datahub.ingestion.source.sql.sql_config import SQLAlchemyConnectionConfig
from datahub.metadata.schema_classes import ChangeTypeClass, SystemMetadataClass
from datahub.utilities.lossy_collections import LossyDict, LossyList

logger = logging.getLogger(__name__)

# Should work for at least mysql, mariadb, postgres
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

ROW = TypeVar("ROW", bound=Dict[str, Any])


class VersionOrderer(Generic[ROW]):
    """Orders rows by (createdon, version == 0).

    That is, orders rows first by createdon, and for equal timestamps, puts version 0 rows last.
    """

    def __init__(self, enabled: bool):
        # Stores all version 0 aspects for a given createdon timestamp
        # Once we have emitted all aspects for a given timestamp, we can emit the version 0 aspects
        # Guaranteeing that, for a given timestamp, we always ingest version 0 aspects last
        self.queue: Optional[Tuple[datetime, List[ROW]]] = None
        self.enabled = enabled

    def __call__(self, rows: Iterable[ROW]) -> Iterable[ROW]:
        for row in rows:
            yield from self._process_row(row)
        yield from self._flush_queue()

    def _process_row(self, row: ROW) -> Iterable[ROW]:
        if not self.enabled:
            yield row
            return

        yield from self._attempt_queue_flush(row)
        if row["version"] == 0:
            self._add_to_queue(row)
        else:
            yield row

    def _add_to_queue(self, row: ROW) -> None:
        if self.queue is None:
            self.queue = (row["createdon"], [row])
        else:
            self.queue[1].append(row)

    def _attempt_queue_flush(self, row: ROW) -> Iterable[ROW]:
        if self.queue is None:
            return

        if row["createdon"] > self.queue[0]:
            yield from self._flush_queue()

    def _flush_queue(self) -> Iterable[ROW]:
        if self.queue is not None:
            yield from self.queue[1]
            self.queue = None


class DataHubDatabaseReader:
    def __init__(
        self,
        config: DataHubSourceConfig,
        connection_config: SQLAlchemyConnectionConfig,
        report: DataHubSourceReport,
    ):
        self.config = config
        self.report = report
        self.engine = create_engine(
            url=connection_config.get_sql_alchemy_url(),
            **connection_config.options,
        )

    @property
    def soft_deleted_urns_query(self) -> str:
        return f"""
            SELECT DISTINCT mav.urn
            FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)} as mav
            JOIN (
                SELECT *,
                JSON_EXTRACT(metadata, '$.removed') as removed
                FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)}
                WHERE aspect = "status" AND version = 0
            ) as sd ON sd.urn = mav.urn
            WHERE sd.removed = true
            ORDER BY mav.urn
        """

    @property
    def query(self) -> str:
        # May repeat rows for the same date
        # Offset is generally 0, unless we repeat the same createdon twice

        # Ensures stable order, chronological per (urn, aspect)
        # Relies on createdon order to reflect version order
        # Ordering of entries with the same createdon is handled by VersionOrderer
        return f"""
        SELECT *
        FROM (
            SELECT
                mav.urn,
                mav.aspect,
                mav.metadata,
                mav.systemmetadata,
                mav.createdon,
                mav.version,
                removed
            FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)} as mav
            LEFT JOIN (
                SELECT
                    *,
                    JSON_EXTRACT(metadata, '$.removed') as removed
                FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)}
                WHERE aspect = 'status'
                AND version = 0
            ) as sd ON sd.urn = mav.urn
            WHERE 1 = 1
                {"" if self.config.include_all_versions else "AND mav.version = 0"}
                {"" if not self.config.exclude_aspects else "AND mav.aspect NOT IN %(exclude_aspects)s"}
                AND mav.createdon >= %(since_createdon)s
            ORDER BY
                createdon,
                urn,
                aspect,
                version
        ) as t
        WHERE 1=1
            {"" if self.config.include_soft_deleted_entities else "AND (removed = false or removed is NULL)"}
        ORDER BY
            createdon,
            urn,
            aspect,
            version
        """

    def execute_server_cursor(
        self, query: str, params: Dict[str, Any]
    ) -> Iterable[Dict[str, Any]]:
        with self.engine.connect() as conn:
            if self.engine.dialect.name in ["postgresql", "mysql", "mariadb"]:
                with (
                    conn.begin()
                ):  # Transaction required for PostgreSQL server-side cursor
                    # Note that stream_results=True is mainly supported by PostgreSQL and MySQL-based dialects.
                    # https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Connection.execution_options.params.stream_results
                    conn = conn.execution_options(
                        stream_results=True,
                        yield_per=self.config.database_query_batch_size,
                    )
                    result = conn.execute(query, params)
                    for row in result:
                        yield dict(row)
            else:
                raise ValueError(f"Unsupported dialect: {self.engine.dialect.name}")

    def _get_rows(
        self, from_createdon: datetime, stop_time: datetime
    ) -> Iterable[Dict[str, Any]]:
        params = {
            "exclude_aspects": list(self.config.exclude_aspects),
            "since_createdon": from_createdon.strftime(DATETIME_FORMAT),
        }
        yield from self.execute_server_cursor(self.query, params)

    def get_aspects(
        self, from_createdon: datetime, stop_time: datetime
    ) -> Iterable[Tuple[MetadataChangeProposalWrapper, datetime]]:
        orderer = VersionOrderer[Dict[str, Any]](
            enabled=self.config.include_all_versions
        )
        rows = self._get_rows(from_createdon=from_createdon, stop_time=stop_time)
        for row in orderer(rows):
            mcp = self._parse_row(row)
            if mcp:
                yield mcp, row["createdon"]

    def get_soft_deleted_rows(self) -> Iterable[Dict[str, Any]]:
        """
        Fetches all soft-deleted entities from the database.

        Yields:
            Row objects containing URNs of soft-deleted entities
        """
        with self.engine.connect() as conn, contextlib.closing(
            conn.connection.cursor()
        ) as cursor:
            logger.debug("Polling soft-deleted urns from database")
            cursor.execute(self.soft_deleted_urns_query)
            columns = [desc[0] for desc in cursor.description]
            while True:
                rows = cursor.fetchmany(self.config.database_query_batch_size)
                if not rows:
                    return
                for row in rows:
                    yield dict(zip(columns, row))

    def _parse_row(
        self, row: Dict[str, Any]
    ) -> Optional[MetadataChangeProposalWrapper]:
        try:
            json_aspect = post_json_transform(json.loads(row["metadata"]))
            json_metadata = post_json_transform(
                json.loads(row["systemmetadata"] or "{}")
            )
            system_metadata = SystemMetadataClass.from_obj(json_metadata)
            return MetadataChangeProposalWrapper(
                entityUrn=row["urn"],
                aspect=ASPECT_MAP[row["aspect"]].from_obj(json_aspect),
                systemMetadata=system_metadata,
                changeType=ChangeTypeClass.UPSERT,
            )
        except Exception as e:
            logger.warning(
                f"Failed to parse metadata for {row['urn']}: {e}", exc_info=True
            )
            self.report.num_database_parse_errors += 1
            self.report.database_parse_errors.setdefault(
                str(e), LossyDict()
            ).setdefault(row["aspect"], LossyList()).append(row["urn"])
            return None
