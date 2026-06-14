import logging
from dataclasses import dataclass
from typing import Iterable, List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import InputFieldClass, InputFieldsClass

logger = logging.getLogger(__name__)


@dataclass
class ValidateInputFieldsProcessorReport(WorkunitProcessorReport):
    num_input_fields_filtered: int = 0
    num_workunits_with_invalid_fields: int = 0
    num_workunits_skipped_entirely: int = 0


class ValidateInputFieldsProcessor(
    WorkunitProcessor[ValidateInputFieldsProcessorReport]
):
    """Validate input fields, filtering out entries with empty fieldPath values."""

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        for wu in stream:
            input_fields_aspect = wu.get_aspect_of_type(InputFieldsClass)
            if input_fields_aspect and input_fields_aspect.fields:
                valid_fields: List[InputFieldClass] = []
                invalid_count = 0

                for input_field in input_fields_aspect.fields:
                    if (
                        input_field.schemaField
                        and input_field.schemaField.fieldPath
                        and input_field.schemaField.fieldPath.strip()
                    ):
                        valid_fields.append(input_field)
                    else:
                        invalid_count += 1

                if invalid_count > 0:
                    logger.debug(
                        f"Filtered {invalid_count} invalid input field(s) with empty fieldPath for {wu.get_urn()}"
                    )
                    self.report.num_input_fields_filtered += invalid_count
                    self.report.num_workunits_with_invalid_fields += 1
                    self.ctx.source_report.warning(
                        title="Invalid input fields filtered",
                        message="Input fields with empty fieldPath values were filtered out to prevent ingestion errors",
                        context=f"Filtered {invalid_count} invalid input field(s) for {wu.get_urn()}",
                    )

                    if valid_fields:
                        input_fields_aspect.fields = valid_fields
                    else:
                        logger.debug(
                            f"All input fields were invalid for {wu.get_urn()}, skipping InputFieldsClass workunit"
                        )
                        self.report.num_workunits_skipped_entirely += 1
                        continue

            yield wu
