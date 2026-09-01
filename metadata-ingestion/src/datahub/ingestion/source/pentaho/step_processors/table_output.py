"""Table Output step processor for Pentaho transformations."""

from typing import Optional
from xml.etree.ElementTree import (
    Element,  # nosec B405 - only for type hints; parsing goes through defusedxml
)

from datahub.ingestion.source.pentaho.context import ProcessingContext
from datahub.ingestion.source.pentaho.step_processors.base import StepProcessor


class TableOutputProcessor(StepProcessor):
    """Processor for TableOutput steps."""

    def can_process(self, step_type: str) -> bool:
        return step_type == "TableOutput"

    def process(
        self,
        step: Element,
        context: ProcessingContext,
        root: Optional[Element] = None,
    ) -> None:
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
