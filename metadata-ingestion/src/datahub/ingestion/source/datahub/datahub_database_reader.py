import json
import logging
from datetime import datetime
from typing import Any, Generic, Iterable, List, Optional, Tuple, TypeVar

from sqlalchemy import create_engine
from sqlalchemy.engine import Row
from typing_extensions import Protocol

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


class VersionOrderable(Protocol):
    createdon: Any  # Should restrict to only orderable types
    version: int


ROW = TypeVar("ROW", bound=VersionOrderable)


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
        if row.version == 0:
            self._add_to_queue(row)
        else:
            yield row

    def _add_to_queue(self, row: ROW) -> None:
        if self.queue is None:
            self.queue = (row.createdon, [row])
        else:
            self.queue[1].append(row)

    def _attempt_queue_flush(self, row: ROW) -> Iterable[ROW]:
        if self.queue is None:
            return

        if row.createdon > self.queue[0]:
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
    def query(self) -> str:
        # May repeat rows for the same date
        # Offset is generally 0, unless we repeat the same createdon twice

        # Ensures stable order, chronological per (urn, aspect)
        # Relies on createdon order to reflect version order
        # Ordering of entries with the same createdon is handled by VersionOrderer
        return f"""
            SELECT urn, aspect, metadata, systemmetadata, createdon, version
            FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)}
            WHERE createdon >= %(since_createdon)s
            {"" if self.config.include_all_versions else "AND version = 0"}
            ORDER BY createdon, urn, aspect, version
            LIMIT %(limit)s
            OFFSET %(offset)s
        """

    def get_aspects(
        self, from_createdon: datetime, stop_time: datetime
    ) -> Iterable[Tuple[MetadataChangeProposalWrapper, datetime]]:
        orderer = VersionOrderer[Row](enabled=self.config.include_all_versions)
        rows = self._get_rows(from_createdon=from_createdon, stop_time=stop_time)
        for row in orderer(rows):
            mcp = self._parse_row(row)
            if mcp:
                yield mcp, row.createdon

    def _get_rows(self, from_createdon: datetime, stop_time: datetime) -> Iterable[Row]:
        with self.engine.connect() as conn:
            ts = from_createdon
            offset = 0
            while ts.timestamp() <= stop_time.timestamp():
                logger.debug(f"Polling database aspects from {ts}")
                rows = conn.execute(
                    self.query,
                    since_createdon=ts.strftime(DATETIME_FORMAT),
                    limit=self.config.database_query_batch_size,
                    offset=offset,
                )
                if not rows.rowcount:
                    return

                for i, row in enumerate(rows):
                    yield row

                if ts == row.createdon:
                    offset += i + 1
                else:
                    ts = row.createdon
                    offset = 0

    def _parse_row(self, row: Row) -> Optional[MetadataChangeProposalWrapper]:
        try:
            json_aspect = post_json_transform(json.loads(row.metadata))
            json_metadata = post_json_transform(json.loads(row.systemmetadata or "{}"))
            system_metadata = SystemMetadataClass.from_obj(json_metadata)
            return MetadataChangeProposalWrapper(
                entityUrn=row.urn,
                aspect=ASPECT_MAP[row.aspect].from_obj(json_aspect),
                systemMetadata=system_metadata,
                changeType=ChangeTypeClass.UPSERT,
            )
        except Exception as e:
            logger.warning(
                f"Failed to parse metadata for {row.urn}: {e}", exc_info=True
            )
            self.report.num_database_parse_errors += 1
            self.report.database_parse_errors.setdefault(
                str(e), LossyDict()
            ).setdefault(row.aspect, LossyList()).append(row.urn)
            return None
