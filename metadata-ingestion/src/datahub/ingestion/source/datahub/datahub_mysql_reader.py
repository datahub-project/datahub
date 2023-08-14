import json
import logging
from datetime import datetime, timezone
from typing import Iterable, Optional, Tuple

from sqlalchemy import create_engine
from sqlalchemy.engine import Row

from datahub.emitter.aspect import ASPECT_MAP
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.serialization_helper import post_json_transform
from datahub.ingestion.source.datahub.config import DataHubSourceConfig
from datahub.ingestion.source.datahub.report import DataHubSourceReport
from datahub.metadata.schema_classes import ChangeTypeClass, SystemMetadataClass
from datahub.utilities.lossy_collections import LossyDict, LossyList

logger = logging.getLogger(__name__)

MYSQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


class DataHubMySQLReader:
    def __init__(self, config: DataHubSourceConfig, report: DataHubSourceReport):
        self.config = config
        self.report = report
        self.engine = create_engine(
            url=config.mysql_connection.get_sql_alchemy_url(),
            **config.mysql_connection.options,
        )

    @property
    def query(self) -> str:
        # May repeat rows for the same date
        # Offset is generally 0, unless we repeat the same date twice
        return f"""
            SELECT urn, aspect, metadata, systemmetadata, createdon
            FROM `{self.config.mysql_table_name}`
            WHERE createdon >= %(since_createdon)s
            {"" if self.config.include_all_versions else "AND version = 0"}
            ORDER BY createdon, urn, aspect, version  # Ensures stable ordering
            LIMIT %(limit)s
            OFFSET %(offset)s
        """

    def get_aspects(
        self, from_createdon: datetime, stop_time: datetime
    ) -> Iterable[Tuple[MetadataChangeProposalWrapper, datetime]]:
        with self.engine.connect() as conn:
            ts = from_createdon
            offset = 0
            while ts.timestamp() <= stop_time.timestamp():
                logger.debug(f"Polling MySQL aspects from {ts}")
                rows = conn.execute(
                    self.query,
                    since_createdon=ts.strftime(MYSQL_DATETIME_FORMAT),
                    limit=self.config.mysql_batch_size,
                    offset=offset,
                )
                if not rows:
                    break

                for i, row in enumerate(rows):
                    mcp = self._parse_mysql_row(row)
                    if mcp:
                        yield mcp, row.createdon

                if ts == row.createdon:
                    offset += i
                else:
                    ts = row.createdon
                    offset = 0

    def _parse_mysql_row(self, row: Row) -> Optional[MetadataChangeProposalWrapper]:
        try:
            json_aspect = post_json_transform(json.loads(row.metadata))
            json_metadata = post_json_transform(json.loads(row.systemmetadata or "{}"))
            system_metadata = SystemMetadataClass.from_obj(json_metadata)
            system_metadata.lastObserved = int(row.createdon.timestamp() * 1000)
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
            self.report.num_mysql_parse_errors += 1
            self.report.mysql_parse_errors.setdefault(str(e), LossyDict()).setdefault(
                row.aspect, LossyList()
            ).append(row.urn)
            return None
