import json
import logging
from typing import TYPE_CHECKING, Iterable, List

from datahub.emitter.rest_emitter import INGEST_MAX_PAYLOAD_BYTES
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    SchemaFieldClass,
    SchemaMetadataClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.api.source import SourceReport

logger = logging.getLogger(__name__)


class EnsureAspectSizeProcessor:
    def __init__(
        self, report: "SourceReport", payload_constraint: int = INGEST_MAX_PAYLOAD_BYTES
    ):
        self.report = report
        self.payload_constraint = payload_constraint

    def ensure_dataset_profile_size(
        self, dataset_urn: str, profile: DatasetProfileClass
    ) -> None:
        """
        This is quite arbitrary approach to ensuring dataset profile aspect does not exceed allowed size, might be adjusted
        in the future
        """
        sample_fields_size = 0
        if profile.fieldProfiles:
            logger.debug(f"Length of field profiles: {len(profile.fieldProfiles)}")
            for field in profile.fieldProfiles:
                if field.sampleValues:
                    values_len = 0
                    for value in field.sampleValues:
                        if value:
                            values_len += len(value)
                    logger.debug(
                        f"Field {field.fieldPath} has {len(field.sampleValues)} sample values, taking total bytes {values_len}"
                    )
                    if sample_fields_size + values_len > self.payload_constraint:
                        field.sampleValues = []
                        self.report.warning(
                            title="Dataset profile truncated due to size constraint",
                            message="Dataset profile contained too much data and would have caused ingestion to fail",
                            context=f"Sample values for field {field.fieldPath} were removed from dataset profile for {dataset_urn} due to aspect size constraints",
                        )
                    else:
                        sample_fields_size += values_len
                else:
                    logger.debug(f"Field {field.fieldPath} has no sample values")

    def ensure_schema_metadata_size(
        self, dataset_urn: str, schema: SchemaMetadataClass
    ) -> None:
        """
        This is quite arbitrary approach to ensuring schema metadata aspect does not exceed allowed size, might be adjusted
        in the future
        """
        total_fields_size = 0
        logger.debug(f"Amount of schema fields: {len(schema.fields)}")
        accepted_fields: List[SchemaFieldClass] = []
        for field in schema.fields:
            field_size = len(json.dumps(pre_json_transform(field.to_obj())))
            logger.debug(f"Field {field.fieldPath} takes total {field_size}")
            if total_fields_size + field_size < self.payload_constraint:
                accepted_fields.append(field)
                total_fields_size += field_size
            else:
                self.report.warning(
                    title="Schema truncated due to size constraint",
                    message="Dataset schema contained too much data and would have caused ingestion to fail",
                    context=f"Field {field.fieldPath} was removed from schema for {dataset_urn} due to aspect size constraints",
                )

        schema.fields = accepted_fields

    def ensure_aspect_size(
        self,
        stream: Iterable[MetadataWorkUnit],
    ) -> Iterable[MetadataWorkUnit]:
        """
        We have hard limitation of aspect size being 16 MB. Some aspects can exceed that value causing an exception
        on GMS side and failure of the entire ingestion. This processor will attempt to trim suspected aspects.
        """
        for wu in stream:
            logger.debug(f"Ensuring size of workunit: {wu.id}")

            if schema := wu.get_aspect_of_type(SchemaMetadataClass):
                self.ensure_schema_metadata_size(wu.get_urn(), schema)
            elif profile := wu.get_aspect_of_type(DatasetProfileClass):
                self.ensure_dataset_profile_size(wu.get_urn(), profile)
            yield wu
