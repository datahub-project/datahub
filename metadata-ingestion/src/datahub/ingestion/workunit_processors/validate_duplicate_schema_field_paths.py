import logging
from dataclasses import dataclass
from typing import Iterable, List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaMetadataClass
from datahub.telemetry import telemetry

logger = logging.getLogger(__name__)


@dataclass
class ValidateDuplicateSchemaFieldPathsProcessorReport(WorkunitProcessorReport):
    """Report for ValidateDuplicateSchemaFieldPathsProcessor metrics."""

    total_schema_aspects: int = 0
    schemas_with_duplicates: int = 0
    duplicated_field_paths: int = 0


class ValidateDuplicateSchemaFieldPathsProcessor(
    WorkunitProcessor[ValidateDuplicateSchemaFieldPathsProcessorReport]
):
    """Remove duplicate field paths from schema metadata aspects."""

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        self._platform = ctx.infer_platform()

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        """Count schema metadata aspects with duplicate field paths and emit telemetry."""
        for wu in stream:
            schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
            if schema_metadata:
                self.report.total_schema_aspects += 1

                seen_fields = set()
                dropped_fields = []
                updated_fields: List[SchemaFieldClass] = []
                for field in schema_metadata.fields:
                    if field.fieldPath in seen_fields:
                        dropped_fields.append(field.fieldPath)
                    else:
                        seen_fields.add(field.fieldPath)
                        updated_fields.append(field)

                if dropped_fields:
                    logger.info(
                        f"Fixing duplicate field paths in schema aspect for {wu.get_urn()} by dropping fields: {dropped_fields}"
                    )
                    schema_metadata.fields = updated_fields
                    self.report.schemas_with_duplicates += 1
                    self.report.duplicated_field_paths += len(dropped_fields)

            yield wu

        if self.report.schemas_with_duplicates:
            properties = {
                "platform": self._platform,
                "total_schema_aspects": self.report.total_schema_aspects,
                "schemas_with_duplicates": self.report.schemas_with_duplicates,
                "duplicated_field_paths": self.report.duplicated_field_paths,
            }
            telemetry.telemetry_instance.ping(
                "ingestion_duplicate_schema_field_paths", properties
            )
