import logging
from typing import Iterable, List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaMetadataClass
from datahub.telemetry import telemetry

logger = logging.getLogger(__name__)


class ValidateDuplicateSchemaFieldPathsProcessor(WorkunitProcessor):
    """Remove duplicate field paths from schema metadata aspects."""

    NAME = "validate_duplicate_schema_field_paths"

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        self._platform = ctx.infer_platform()
        self._total_schema_aspects = 0
        self._schemas_with_duplicates = 0
        self._duplicated_field_paths = 0

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        """Count schema metadata aspects with duplicate field paths and emit telemetry."""
        for wu in stream:
            schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
            if schema_metadata:
                self._total_schema_aspects += 1

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
                    self._schemas_with_duplicates += 1
                    self._duplicated_field_paths += len(dropped_fields)

            yield wu

        if self._schemas_with_duplicates:
            properties = {
                "platform": self._platform,
                "total_schema_aspects": self._total_schema_aspects,
                "schemas_with_duplicates": self._schemas_with_duplicates,
                "duplicated_field_paths": self._duplicated_field_paths,
            }
            telemetry.telemetry_instance.ping(
                "ingestion_duplicate_schema_field_paths", properties
            )
