"""Table Input step processor for Pentaho transformations."""

import logging
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING, Optional

from datahub.ingestion.source.pentaho.step_processors.base import StepProcessor
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

if TYPE_CHECKING:
    from datahub.ingestion.source.pentaho.context import ProcessingContext

logger = logging.getLogger(__name__)


class TableInputProcessor(StepProcessor):
    """Processor for TableInput steps."""

    def can_process(self, step_type: str) -> bool:
        return step_type == "TableInput"

    def process(
        self,
        step: ET.Element,
        context: "ProcessingContext",
        root: Optional[ET.Element] = None,
    ):
        conn_name = step.findtext("connection")
        table_name = step.findtext("table")
        sql = step.findtext("sql")

        # Get connection type for platform detection
        conn_type = (
            self.source._get_connection_type(root, conn_name or "")
            if root is not None
            else None
        )
        platform = self.source._get_platform_from_connection(
            conn_name or "", "TableInput", conn_type
        )

        # If we have SQL, use the SQL parser
        if sql and sql.strip():
            try:
                parsed_result = create_lineage_sql_parsed_result(
                    query=sql,
                    default_db=None,
                    default_schema=None,
                    platform=platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )

                # Add all input tables from the parsed SQL
                for table_urn in parsed_result.in_tables:
                    context.add_input_dataset(table_urn)

            except Exception as e:
                logger.warning(
                    f"Failed to parse SQL with DataHub parser: {e}, falling back to table name"
                )
                # Fallback to explicit table name if SQL parsing fails
                if table_name:
                    dataset_urn = self.source._create_dataset_urn(platform, table_name)
                    if dataset_urn:
                        context.add_input_dataset(dataset_urn)
        elif table_name:
            # Use explicit table name
            dataset_urn = self.source._create_dataset_urn(platform, table_name)
            if dataset_urn:
                context.add_input_dataset(dataset_urn)
