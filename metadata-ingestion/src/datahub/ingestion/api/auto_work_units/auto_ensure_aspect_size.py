import json
import logging
import os
from typing import TYPE_CHECKING, Iterable, List

from datahub.emitter.rest_emitter import INGEST_MAX_PAYLOAD_BYTES
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    QueryPropertiesClass,
    QuerySubjectsClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.api.source import SourceReport


# TODO: ordering
# In the cases where we trim collections of data (e.g. fields in schema, upstream lineage, query subjects), given
# those collections are typically unordered, we should consider sorting them by some criteria (e.g. size, alphabetically)
# so that the trimming is deterministic and predictable and more importantly consistent across executions.
# In the case of schemaMetadata, that's more relevant as currently we may be trimming fields while adding nested ones,
# which may lead to poorly schema rendering in the UI.

logger = logging.getLogger(__name__)

DEFAULT_QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES = 5 * 1024 * 1024  # 5MB
QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES = int(
    os.environ.get(
        "QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES",
        DEFAULT_QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES,
    )
)

QUERY_STATEMENT_TRUNCATION_BUFFER = 100


class EnsureAspectSizeProcessor:
    def __init__(
        self, report: "SourceReport", payload_constraint: int = INGEST_MAX_PAYLOAD_BYTES
    ):
        self.report = report
        self.payload_constraint = payload_constraint
        self.schema_size_constraint = int(self.payload_constraint * 0.985)

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
            if total_fields_size + field_size < self.schema_size_constraint:
                accepted_fields.append(field)
                total_fields_size += field_size
            else:
                self.report.warning(
                    title="Schema truncated due to size constraint",
                    message="Dataset schema contained too much data and would have caused ingestion to fail",
                    context=f"Field {field.fieldPath} was removed from schema for {dataset_urn} due to aspect size constraints",
                )

        schema.fields = accepted_fields

    def ensure_query_subjects_size(
        self, entity_urn: str, query_subjects: QuerySubjectsClass
    ) -> None:
        """
        Ensure query subjects aspect does not exceed allowed size by removing column-level lineage first,
        then table lineage if necessary.
        """
        if not query_subjects.subjects:
            return

        total_subjects_size = 0
        accepted_table_level_subjects = []
        accepted_column_level_subjects = []
        column_level_subjects_with_sizes = []
        table_level_subjects_with_sizes = []

        # Separate column-level and table-level subjects
        for subject in query_subjects.subjects:
            subject_size = len(json.dumps(pre_json_transform(subject.to_obj())))

            if subject.entity.startswith("urn:li:schemaField:"):
                column_level_subjects_with_sizes.append((subject, subject_size))
            else:
                table_level_subjects_with_sizes.append((subject, subject_size))

        # Once we find one that doesn't fit, stop everything else to prevent inconsistencies
        first_skip_done = False

        # First, try to include all table-level subjects
        for subject, subject_size in table_level_subjects_with_sizes:
            if total_subjects_size + subject_size < self.payload_constraint:
                accepted_table_level_subjects.append(subject)
                total_subjects_size += subject_size
            else:
                first_skip_done = True
                break

        # Then, add column-level subjects if there's remaining space
        # Only process if we successfully included all table-level subjects
        if not first_skip_done:
            for subject, subject_size in column_level_subjects_with_sizes:
                if total_subjects_size + subject_size < self.payload_constraint:
                    accepted_column_level_subjects.append(subject)
                    total_subjects_size += subject_size
                else:
                    first_skip_done = True
                    break

        if first_skip_done:
            # Log aggregate warnings
            table_level_skipped_count = len(table_level_subjects_with_sizes) - len(
                accepted_table_level_subjects
            )
            column_level_skipped_count = len(column_level_subjects_with_sizes) - len(
                accepted_column_level_subjects
            )

            self._maybe_warn_query_subjects(
                entity_urn, table_level_skipped_count, "table-level lineage subjects"
            )
            self._maybe_warn_query_subjects(
                entity_urn, column_level_skipped_count, "column-level lineage subjects"
            )

        query_subjects.subjects = (
            accepted_table_level_subjects + accepted_column_level_subjects
        )

    def _maybe_warn_query_subjects(
        self, entity_urn: str, skipped_count: int, item_type: str
    ) -> None:
        """Log warning for query subjects truncation if any items were skipped."""
        if skipped_count > 0:
            self.report.warning(
                title="Query subjects truncated due to size constraint",
                message="Query subjects contained too much data and would have caused ingestion to fail",
                context=f"Skipped {skipped_count} {item_type} for {entity_urn} due to aspect size constraints",
            )

    def _maybe_warn_upstream_lineage(
        self, entity_urn: str, skipped_count: int, item_type: str
    ) -> None:
        """Log warning for upstream lineage truncation if any items were skipped."""
        if skipped_count > 0:
            self.report.warning(
                title="Upstream lineage truncated due to size constraint",
                message="Upstream lineage contained too much data and would have caused ingestion to fail",
                context=f"Skipped {skipped_count} {item_type} for {entity_urn} due to aspect size constraints",
            )

    def ensure_upstream_lineage_size(  # noqa: C901
        self, entity_urn: str, upstream_lineage: UpstreamLineageClass
    ) -> None:
        """
        Ensure upstream lineage aspect does not exceed allowed size by removing lineage in priority order:
        first NONE fine-grained lineages (lowest priority), then FIELD_SET fine-grained lineages,
        then DATASET fine-grained lineages, and finally upstreams (highest priority).
        """
        if not upstream_lineage.fineGrainedLineages and not upstream_lineage.upstreams:
            return

        total_lineage_size = 0
        accepted_upstreams = []
        accepted_dataset_fg_lineages = []
        accepted_field_set_fg_lineages = []
        accepted_none_fg_lineages = []
        upstream_items_with_sizes = []
        dataset_fg_items_with_sizes = []
        field_set_fg_items_with_sizes = []
        none_fg_items_with_sizes = []

        # Add upstreams (highest priority)
        if upstream_lineage.upstreams:
            for upstream in upstream_lineage.upstreams:
                upstream_size = len(json.dumps(pre_json_transform(upstream.to_obj())))
                upstream_items_with_sizes.append((upstream, upstream_size))

        # Separate fine-grained lineage items by upstreamType: DATASET > FIELD_SET > NONE
        if upstream_lineage.fineGrainedLineages:
            for fg_lineage in upstream_lineage.fineGrainedLineages:
                fg_lineage_size = len(
                    json.dumps(pre_json_transform(fg_lineage.to_obj()))
                )

                upstream_type_str = str(fg_lineage.upstreamType)
                if upstream_type_str == "DATASET":
                    dataset_fg_items_with_sizes.append((fg_lineage, fg_lineage_size))
                elif upstream_type_str == "FIELD_SET":
                    field_set_fg_items_with_sizes.append((fg_lineage, fg_lineage_size))
                elif upstream_type_str == "NONE":
                    none_fg_items_with_sizes.append((fg_lineage, fg_lineage_size))

        # Once we find one that doesn't fit, stop everything else to prevent inconsistencies
        first_skip_done = False

        # First, include all upstreams (highest priority)
        for item, item_size in upstream_items_with_sizes:
            if total_lineage_size + item_size < self.payload_constraint:
                accepted_upstreams.append(item)
                total_lineage_size += item_size
            else:
                first_skip_done = True
                break

        # Second, include DATASET fine-grained lineages if no upstreams were skipped
        if not first_skip_done:
            for fg_lineage, fg_lineage_size in dataset_fg_items_with_sizes:
                if total_lineage_size + fg_lineage_size < self.payload_constraint:
                    accepted_dataset_fg_lineages.append(fg_lineage)
                    total_lineage_size += fg_lineage_size
                else:
                    first_skip_done = True
                    break

        # Third, include FIELD_SET fine-grained lineages if no higher priority items were skipped
        if not first_skip_done:
            for fg_lineage, fg_lineage_size in field_set_fg_items_with_sizes:
                if total_lineage_size + fg_lineage_size < self.payload_constraint:
                    accepted_field_set_fg_lineages.append(fg_lineage)
                    total_lineage_size += fg_lineage_size
                else:
                    first_skip_done = True
                    break

        # Finally, include NONE fine-grained lineages if no higher priority items were skipped
        if not first_skip_done:
            for fg_lineage, fg_lineage_size in none_fg_items_with_sizes:
                if total_lineage_size + fg_lineage_size < self.payload_constraint:
                    accepted_none_fg_lineages.append(fg_lineage)
                    total_lineage_size += fg_lineage_size
                else:
                    first_skip_done = True
                    break

        # Log aggregate warnings instead of per-item warnings
        if first_skip_done:
            upstreams_skipped_count = len(upstream_items_with_sizes) - len(
                accepted_upstreams
            )
            dataset_fg_skipped_count = len(dataset_fg_items_with_sizes) - len(
                accepted_dataset_fg_lineages
            )
            field_set_fg_skipped_count = len(field_set_fg_items_with_sizes) - len(
                accepted_field_set_fg_lineages
            )
            none_fg_skipped_count = len(none_fg_items_with_sizes) - len(
                accepted_none_fg_lineages
            )

            self._maybe_warn_upstream_lineage(
                entity_urn, upstreams_skipped_count, "upstream datasets"
            )
            self._maybe_warn_upstream_lineage(
                entity_urn,
                dataset_fg_skipped_count,
                "dataset-level fine-grained lineages",
            )
            self._maybe_warn_upstream_lineage(
                entity_urn,
                field_set_fg_skipped_count,
                "field-set-level fine-grained lineages",
            )
            self._maybe_warn_upstream_lineage(
                entity_urn, none_fg_skipped_count, "none-level fine-grained lineages"
            )

        # Combine all accepted fine-grained lineages
        accepted_fine_grained_lineages = (
            accepted_dataset_fg_lineages
            + accepted_field_set_fg_lineages
            + accepted_none_fg_lineages
        )

        upstream_lineage.upstreams = accepted_upstreams
        upstream_lineage.fineGrainedLineages = (
            accepted_fine_grained_lineages if accepted_fine_grained_lineages else None
        )

    def ensure_query_properties_size(
        self, entity_urn: str, query_properties: QueryPropertiesClass
    ) -> None:
        """
        Ensure query properties aspect does not exceed allowed size by truncating the query statement value.
        Uses a configurable max payload size that is the minimum between QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES
        and INGEST_MAX_PAYLOAD_BYTES.

        We have found surprisingly large query statements (e.g. 20MB+) that caused ingestion to fail;
        that was INSERT INTO VALUES with huge list of values.
        """
        if not query_properties.statement or not query_properties.statement.value:
            return

        max_payload_size = min(
            QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES, self.payload_constraint
        )

        current_size = len(json.dumps(pre_json_transform(query_properties.to_obj())))

        if current_size < max_payload_size:
            return

        reduction_needed = (
            current_size - max_payload_size + QUERY_STATEMENT_TRUNCATION_BUFFER
        )

        statement_value_size = len(query_properties.statement.value)
        original_statement_size = statement_value_size

        # Only truncate if reduction is actually needed and possible
        if statement_value_size > reduction_needed > 0:
            new_statement_length = statement_value_size - reduction_needed
            truncated_statement = query_properties.statement.value[
                :new_statement_length
            ]

            truncation_message = f"... [original value was {original_statement_size} bytes and truncated to {new_statement_length} bytes]"
            query_properties.statement.value = truncated_statement + truncation_message

            self.report.warning(
                title="Query properties truncated due to size constraint",
                message="Query properties contained too much data and would have caused ingestion to fail",
                context=f"Query statement was truncated from {original_statement_size} to {new_statement_length} characters for {entity_urn} due to aspect size constraints",
            )
        else:
            logger.warning(
                f"Cannot truncate query statement for {entity_urn} as it is smaller than or equal to the required reduction size {reduction_needed}. That means that 'ensure_query_properties_size' must be extended to trim other fields different than statement."
            )

    def ensure_aspect_size(
        self,
        stream: Iterable[MetadataWorkUnit],
    ) -> Iterable[MetadataWorkUnit]:
        """
        We have hard limitation of aspect size being 16 MB. Some aspects can exceed that value causing an exception
        on GMS side and failure of the entire ingestion. This processor will attempt to trim suspected aspects.
        """
        for wu in stream:
            # logger.debug(f"Ensuring size of workunit: {wu.id}")

            if schema := wu.get_aspect_of_type(SchemaMetadataClass):
                self.ensure_schema_metadata_size(wu.get_urn(), schema)
            elif profile := wu.get_aspect_of_type(DatasetProfileClass):
                self.ensure_dataset_profile_size(wu.get_urn(), profile)
            elif query_subjects := wu.get_aspect_of_type(QuerySubjectsClass):
                self.ensure_query_subjects_size(wu.get_urn(), query_subjects)
            elif upstream_lineage := wu.get_aspect_of_type(UpstreamLineageClass):
                self.ensure_upstream_lineage_size(wu.get_urn(), upstream_lineage)
            elif query_properties := wu.get_aspect_of_type(QueryPropertiesClass):
                self.ensure_query_properties_size(wu.get_urn(), query_properties)
            yield wu
