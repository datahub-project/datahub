import logging
from typing import TYPE_CHECKING, Iterable, List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import InputFieldClass, InputFieldsClass

if TYPE_CHECKING:
    from datahub.ingestion.api.source import SourceReport

logger = logging.getLogger(__name__)


class ValidateInputFieldsProcessor:
    def __init__(self, report: "SourceReport"):
        self.report = report

    def validate_input_fields(
        self,
        stream: Iterable[MetadataWorkUnit],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Validate input fields and filter out invalid ones.

        Invalid input fields have empty or missing fieldPath values, which would cause
        URN generation to fail when sent to the server. This processor filters them out
        and reports them as warnings.
        """
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
                    self.report.warning(
                        title="Invalid input fields filtered",
                        message="Input fields with empty fieldPath values were filtered out to prevent ingestion errors",
                        context=f"Filtered {invalid_count} invalid input field(s) for {wu.get_urn()}",
                    )

                    # Update the aspect with only valid fields
                    if valid_fields:
                        input_fields_aspect.fields = valid_fields
                    else:
                        # If no valid fields remain, skip this workunit entirely
                        logger.debug(
                            f"All input fields were invalid for {wu.get_urn()}, skipping InputFieldsClass workunit"
                        )
                        # Don't yield this workunit
                        continue

            yield wu

    def _remove_input_fields_aspect(self, wu: MetadataWorkUnit) -> MetadataWorkUnit:
        """Remove InputFieldsClass aspect from a workunit."""
        # For MCPs, we can simply not yield the aspect
        # For MCEs, we need to remove it from the snapshot
        if hasattr(wu.metadata, "aspect") and isinstance(
            wu.metadata.aspect, InputFieldsClass
        ):
            # This is an MCP with InputFieldsClass, skip it
            return wu

        if hasattr(wu.metadata, "proposedSnapshot"):
            snapshot = wu.metadata.proposedSnapshot
            if hasattr(snapshot, "aspects"):
                snapshot.aspects = [
                    aspect
                    for aspect in snapshot.aspects
                    if not isinstance(aspect, InputFieldsClass)
                ]

        return wu
