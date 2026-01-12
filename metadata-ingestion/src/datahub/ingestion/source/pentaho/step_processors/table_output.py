"""Table Output step processor for Pentaho transformations."""

import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING, Optional

from datahub.ingestion.source.pentaho.step_processors.base import StepProcessor

if TYPE_CHECKING:
    from datahub.ingestion.source.pentaho.context import ProcessingContext


class TableOutputProcessor(StepProcessor):
    """Processor for TableOutput steps."""

    def can_process(self, step_type: str) -> bool:
        return step_type == "TableOutput"

    def process(
        self,
        step: ET.Element,
        context: "ProcessingContext",
        root: Optional[ET.Element] = None,
    ):
        conn_name = step.findtext("connection")
        table = step.findtext("table")
        schema = step.findtext("schema")

        # Get connection type for platform detection
        conn_type = (
            self.source._get_connection_type(root, conn_name or "")
            if root is not None
            else None
        )
        platform = self.source._get_platform_from_connection(
            conn_name or "", "TableOutput", conn_type
        )

        if table:
            # Format table name with schema if available
            table_name = f"{schema}.{table}" if schema and schema.strip() else table
            dataset_urn = self.source._create_dataset_urn(platform, table_name)
            if dataset_urn:
                context.add_output_dataset(dataset_urn)
