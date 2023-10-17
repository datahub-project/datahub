import json
import logging
from datetime import datetime
from typing import Dict, Iterable, Optional, Tuple

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
        # Version 0 last, only when createdon is the same. Otherwise relies on createdon order
        return f"""
            SELECT urn, aspect, metadata, systemmetadata, createdon
            FROM {self.engine.dialect.identifier_preparer.quote(self.config.database_table_name)}
            WHERE createdon >= %(since_createdon)s
            {"" if self.config.include_all_versions else "AND version = 0"}
            ORDER BY createdon, urn, aspect, CASE WHEN version = 0 THEN 1 ELSE 0 END, version
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
                    row_dict = row._asdict()
                    mcp = self._parse_row(row_dict)
                    if mcp:
                        yield mcp, row_dict["createdon"]

                if ts == row_dict["createdon"]:
                    offset += i
                else:
                    ts = row_dict["createdon"]
                    offset = 0

    def _parse_row(self, d: Dict) -> Optional[MetadataChangeProposalWrapper]:
        try:
            json_aspect = post_json_transform(json.loads(d["metadata"]))
            json_metadata = post_json_transform(json.loads(d["systemmetadata"] or "{}"))
            system_metadata = SystemMetadataClass.from_obj(json_metadata)
            return MetadataChangeProposalWrapper(
                entityUrn=d["urn"],
                aspect=ASPECT_MAP[d["aspect"]].from_obj(json_aspect),
                systemMetadata=system_metadata,
                changeType=ChangeTypeClass.UPSERT,
            )
        except Exception as e:
            logger.warning(
                f"Failed to parse metadata for {d['urn']}: {e}", exc_info=True
            )
            self.report.num_database_parse_errors += 1
            self.report.database_parse_errors.setdefault(
                str(e), LossyDict()
            ).setdefault(d["aspect"], LossyList()).append(d["urn"])
            return None
