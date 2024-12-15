import json
import logging
from typing import Iterable, List

from datahub.emitter.rest_emitter import _MAX_BATCH_INGEST_PAYLOAD_SIZE
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata._schema_classes import SchemaFieldClass
from datahub.metadata.schema_classes import DatasetProfileClass, SchemaMetadataClass

logger = logging.getLogger(__name__)


def ensure_dataset_profile_size(profile: DatasetProfileClass) -> None:
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
                if sample_fields_size + values_len > _MAX_BATCH_INGEST_PAYLOAD_SIZE:
                    logger.warning(
                        f"Adding sample values for field {field.fieldPath} would exceed total allowed size of an aspect, therefor skipping them"
                    )
                    field.sampleValues = []
                else:
                    sample_fields_size += values_len
            else:
                logger.debug(f"Field {field.fieldPath} has no sample values")


def ensure_schema_metadata_size(schema: SchemaMetadataClass) -> None:
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
        if total_fields_size + field_size < _MAX_BATCH_INGEST_PAYLOAD_SIZE:
            accepted_fields.append(field)
            total_fields_size += field_size
        else:
            logger.warning(
                f"Keeping field {field.fieldPath} would make aspect exceed total allowed size, therefor skipping it"
            )
    schema.fields = accepted_fields


def ensure_aspect_size(
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    """
    We have hard limitation of aspect size being 16 MB. Some aspects can exceed that value causing an exception
    on GMS side and failure of the entire ingestion. This processor will attempt to trim suspected aspects.
    """
    for wu in stream:
        logger.debug(f"Ensuring size of workunit: {wu.id}")

        if schema := wu.get_aspect_of_type(SchemaMetadataClass):
            ensure_schema_metadata_size(schema)
        elif profile := wu.get_aspect_of_type(DatasetProfileClass):
            ensure_dataset_profile_size(profile)
        yield wu
