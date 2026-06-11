import logging
from typing import Iterable, List, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)
from datahub.metadata.schema_classes import SchemaFieldClass, SchemaMetadataClass
from datahub.telemetry import telemetry

logger = logging.getLogger(__name__)


def auto_fix_empty_field_paths(
    stream: Iterable[MetadataWorkUnit],
    *,
    platform: Optional[str] = None,
) -> Iterable[MetadataWorkUnit]:
    """Count schema metadata aspects with empty field paths and emit telemetry."""

    total_schema_aspects = 0
    schemas_with_empty_fields = 0
    empty_field_paths = 0

    for wu in stream:
        schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
        if schema_metadata:
            total_schema_aspects += 1

            updated_fields: List[SchemaFieldClass] = []
            for field in schema_metadata.fields:
                if field.fieldPath:
                    updated_fields.append(field)
                else:
                    empty_field_paths += 1

            if empty_field_paths > 0:
                logger.info(
                    f"Fixing empty field paths in schema aspect for {wu.get_urn()} by dropping empty fields"
                )
                schema_metadata.fields = updated_fields
                schemas_with_empty_fields += 1

        yield wu

    if schemas_with_empty_fields > 0:
        properties = {
            "platform": platform,
            "total_schema_aspects": total_schema_aspects,
            "schemas_with_empty_fields": schemas_with_empty_fields,
            "empty_field_paths": empty_field_paths,
        }
        telemetry.telemetry_instance.ping(
            "ingestion_empty_schema_field_paths", properties
        )


class AutoFixEmptyFieldPathsProcessor(WorkunitProcessor):
    """Remove empty field paths from schema metadata aspects."""

    NAME = "auto_fix_empty_field_paths"

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        self._platform = ctx.infer_platform()

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        return auto_fix_empty_field_paths(stream, platform=self._platform)
