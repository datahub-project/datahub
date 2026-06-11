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


class ValidateEmptySchemaFieldPathsProcessor(WorkunitProcessor):
    """Remove empty field paths from schema metadata aspects."""

    NAME = "validate_empty_schema_field_paths"

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        self._platform = ctx.infer_platform()
        self._total_schema_aspects = 0
        self._schemas_with_empty_fields = 0
        self._empty_field_paths = 0

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        """Count schema metadata aspects with empty field paths and emit telemetry."""
        for wu in stream:
            schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
            if schema_metadata:
                self._total_schema_aspects += 1

                updated_fields: List[SchemaFieldClass] = []
                for field in schema_metadata.fields:
                    if field.fieldPath:
                        updated_fields.append(field)
                    else:
                        self._empty_field_paths += 1

                if self._empty_field_paths > 0:
                    logger.info(
                        f"Fixing empty field paths in schema aspect for {wu.get_urn()} by dropping empty fields"
                    )
                    schema_metadata.fields = updated_fields
                    self._schemas_with_empty_fields += 1

            yield wu

        if self._schemas_with_empty_fields > 0:
            properties = {
                "platform": self._platform,
                "total_schema_aspects": self._total_schema_aspects,
                "schemas_with_empty_fields": self._schemas_with_empty_fields,
                "empty_field_paths": self._empty_field_paths,
            }
            telemetry.telemetry_instance.ping(
                "ingestion_empty_schema_field_paths", properties
            )
