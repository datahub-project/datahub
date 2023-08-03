import json
import logging
from datetime import datetime
from typing import Iterable, Optional, Tuple

from sqlalchemy import create_engine
from sqlalchemy.engine import Row

from datahub.emitter.aspect import ASPECT_MAP
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
        return f"""
            SELECT urn, aspect, metadata, createdon
            FROM `{self.config.mysql_table_name}`
            WHERE createdon >= %(since_createdon)s
            {"" if self.config.include_all_versions else "AND version = 0"}
            ORDER BY createdon
        """

    def get_aspects(
        self, from_createdon: datetime
    ) -> Iterable[Tuple[MetadataChangeProposalWrapper, datetime]]:
        with self.engine.connect() as conn:
            for row in conn.execute(
                self.query,
                since_createdon=from_createdon.strftime(MYSQL_DATETIME_FORMAT),
            ):
                mcp = self._parse_mysql_row(row)
                if mcp:
                    yield mcp, row.createdon

    def _parse_mysql_row(self, row: Row) -> Optional[MetadataChangeProposalWrapper]:
        try:
            return MetadataChangeProposalWrapper(
                entityUrn=row.urn,
                # TODO: Get rid of deserialization -- create MCPC?
                aspect=ASPECT_MAP[row.aspect].from_obj(json.loads(row.metadata)),
                systemMetadata=SystemMetadataClass(
                    lastObserved=int(row.createdon.timestamp() * 1000)
                ),
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
