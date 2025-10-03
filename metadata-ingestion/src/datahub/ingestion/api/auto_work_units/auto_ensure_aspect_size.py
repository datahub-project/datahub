import json
import logging
from typing import TYPE_CHECKING, Iterable, List

from datahub.emitter.rest_emitter import INGEST_MAX_PAYLOAD_BYTES
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    QuerySubjectsClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
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
        accepted_subjects = []
        column_level_subjects_with_sizes = []
        table_level_subjects_with_sizes = []

        # Separate column-level and table-level subjects
        for subject in query_subjects.subjects:
            subject_size = len(json.dumps(pre_json_transform(subject.to_obj())))

            if subject.entity.startswith("urn:li:schemaField:"):
                column_level_subjects_with_sizes.append((subject, subject_size))
            else:
                table_level_subjects_with_sizes.append((subject, subject_size))

        # First, try to include all table-level subjects
        for subject, subject_size in table_level_subjects_with_sizes:
            if total_subjects_size + subject_size < self.schema_size_constraint:
                accepted_subjects.append(subject)
                total_subjects_size += subject_size
            else:
                self.report.warning(
                    title="Query subjects truncated due to size constraint",
                    message="Query subjects contained too much data and would have caused ingestion to fail",
                    context=f"Table-level lineage subject {subject.entity} was removed from query subjects for {entity_urn} due to aspect size constraints",
                )

        # Then, add column-level subjects if there's remaining space
        for subject, subject_size in column_level_subjects_with_sizes:
            if total_subjects_size + subject_size < self.schema_size_constraint:
                accepted_subjects.append(subject)
                total_subjects_size += subject_size
            else:
                self.report.warning(
                    title="Query subjects truncated due to size constraint",
                    message="Query subjects contained too much data and would have caused ingestion to fail",
                    context=f"Column-level lineage subject {subject.entity} was removed from query subjects for {entity_urn} due to aspect size constraints",
                )

        query_subjects.subjects = accepted_subjects

    def ensure_upstream_lineage_size(
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
        accepted_fine_grained_lineages = []
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

        # First, include all upstreams
        for item, item_size in upstream_items_with_sizes:
            if total_lineage_size + item_size < self.schema_size_constraint:
                accepted_upstreams.append(item)
                total_lineage_size += item_size
            else:
                self.report.warning(
                    title="Upstream lineage truncated due to size constraint",
                    message="Upstream lineage contained too much data and would have caused ingestion to fail",
                    context=f"Upstream dataset {item.dataset} was removed from upstream lineage for {entity_urn} due to aspect size constraints",
                )

        # Second, include DATASET fine-grained lineages
        for item, item_size in dataset_fg_items_with_sizes:
            if total_lineage_size + item_size < self.schema_size_constraint:
                accepted_fine_grained_lineages.append(item)
                total_lineage_size += item_size
            else:
                self.report.warning(
                    title="Upstream lineage truncated due to size constraint",
                    message="Upstream lineage contained too much data and would have caused ingestion to fail",
                    context=f"Dataset-level fine-grained lineage was removed from upstream lineage for {entity_urn} due to aspect size constraints",
                )

        # Third, include FIELD_SET fine-grained lineages
        for item, item_size in field_set_fg_items_with_sizes:
            if total_lineage_size + item_size < self.schema_size_constraint:
                accepted_fine_grained_lineages.append(item)
                total_lineage_size += item_size
            else:
                self.report.warning(
                    title="Upstream lineage truncated due to size constraint",
                    message="Upstream lineage contained too much data and would have caused ingestion to fail",
                    context=f"Field-level fine-grained lineage was removed from upstream lineage for {entity_urn} due to aspect size constraints",
                )

        # Finally, include NONE fine-grained lineages (lowest priority)
        for item, item_size in none_fg_items_with_sizes:
            if total_lineage_size + item_size < self.schema_size_constraint:
                accepted_fine_grained_lineages.append(item)
                total_lineage_size += item_size
            else:
                self.report.warning(
                    title="Upstream lineage truncated due to size constraint",
                    message="Upstream lineage contained too much data and would have caused ingestion to fail",
                    context=f"No-upstream fine-grained lineage was removed from upstream lineage for {entity_urn} due to aspect size constraints",
                )

        upstream_lineage.upstreams = accepted_upstreams
        upstream_lineage.fineGrainedLineages = (
            accepted_fine_grained_lineages if accepted_fine_grained_lineages else None
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
            yield wu
