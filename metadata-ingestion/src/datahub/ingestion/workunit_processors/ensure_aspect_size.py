import json
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from datahub.emitter.rest_emitter import INGEST_MAX_PAYLOAD_BYTES
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    QueryPropertiesClass,
    QuerySubjectsClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)

logger = logging.getLogger(__name__)

DEFAULT_QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES = 5 * 1024 * 1024  # 5MB
QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES = int(
    os.environ.get(
        "QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES",
        DEFAULT_QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES,
    )
)
QUERY_STATEMENT_TRUNCATION_BUFFER = 100


@dataclass
class _TruncationResult:
    truncated_value: str
    original_size: int
    retained_length: int


@dataclass
class EnsureAspectSizeProcessorReport(WorkunitProcessorReport):
    """Tracks how many workunits had each aspect type truncated."""

    num_truncations_by_aspect: Dict[str, int] = field(default_factory=dict)
    # Number of schemaMetadata aspects whose platformSchema blob was dropped
    # wholesale (a coarser, more drastic truncation than dropping fields).
    num_platform_schema_drops: int = 0


class EnsureAspectSizeProcessor(WorkunitProcessor[EnsureAspectSizeProcessorReport]):
    """Ensure aspects don't exceed the 16MB payload limit by truncating in priority order."""

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        self.payload_constraint = INGEST_MAX_PAYLOAD_BYTES
        self.schema_size_constraint = int(self.payload_constraint * 0.985)

    def _record_truncation(self, aspect_name: str) -> None:
        self.report.num_truncations_by_aspect[aspect_name] = (
            self.report.num_truncations_by_aspect.get(aspect_name, 0) + 1
        )

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        for wu in stream:
            urn = wu.get_urn()
            if schema := wu.get_aspect_of_type(SchemaMetadataClass):
                self.ensure_schema_metadata_size(urn, schema)
            if profile := wu.get_aspect_of_type(DatasetProfileClass):
                self.ensure_dataset_profile_size(urn, profile)
            if query_subjects := wu.get_aspect_of_type(QuerySubjectsClass):
                self.ensure_query_subjects_size(urn, query_subjects)
            if upstream_lineage := wu.get_aspect_of_type(UpstreamLineageClass):
                self.ensure_upstream_lineage_size(urn, upstream_lineage)
            if query_properties := wu.get_aspect_of_type(QueryPropertiesClass):
                self.ensure_query_properties_size(urn, query_properties)
            if view_properties := wu.get_aspect_of_type(ViewPropertiesClass):
                self.ensure_view_properties_size(urn, view_properties)
            yield wu

    def ensure_dataset_profile_size(
        self, dataset_urn: str, profile: DatasetProfileClass
    ) -> None:
        sample_fields_size = 0
        truncated = False
        if profile.fieldProfiles:
            for field in profile.fieldProfiles:
                if field.sampleValues:
                    values_len = sum(len(v) for v in field.sampleValues if v)
                    if sample_fields_size + values_len > self.payload_constraint:
                        field.sampleValues = []
                        truncated = True
                        self.ctx.source_report.warning(
                            title="Dataset profile truncated due to size constraint",
                            message="Dataset profile contained too much data and would have caused ingestion to fail",
                            context=f"Sample values for field {field.fieldPath} were removed from dataset profile for {dataset_urn} due to aspect size constraints",
                        )
                    else:
                        sample_fields_size += values_len
        if truncated:
            self._record_truncation("datasetProfile")

    def _schema_size_without_fields(self, schema: SchemaMetadataClass) -> int:
        """Serialized size of the aspect excluding the ``fields`` list.

        The ``fields`` list is the part we trim, but the rest of the aspect is
        emitted too and must count against the budget — chiefly the
        ``platformSchema`` blob, which for deeply-nested schemas can be several
        MB on its own, plus top-level scalars (``hash``, ``schemaName``, …).

        We measure by temporarily clearing ``fields`` and serializing the whole
        aspect, rather than sizing ``platformSchema`` alone, so the budget
        reflects *all* non-field content the server will parse (nothing is missed
        if new top-level fields are added later). The mutation is local and
        restored in the ``finally``.
        """
        original_fields = schema.fields
        try:
            schema.fields = []
            return len(json.dumps(pre_json_transform(schema.to_obj())))
        finally:
            schema.fields = original_fields

    def _drop_platform_schema(self, schema: SchemaMetadataClass) -> bool:
        """Blank the raw platform-schema string(s) on the aspect's platformSchema.

        ``platformSchema`` holds an opaque, source-format dump of the schema —
        ``rawSchema`` / ``documentSchema`` / ``tableSchema`` / ``keySchema`` /
        ``valueSchema`` depending on the union variant. It is the least valuable
        part of the aspect (``fields`` carry the structured, queryable metadata),
        so we shed it before trimming fields. Every string member is blanked, so
        this covers all platformSchema variants generically — not just
        ``OtherSchema``. Returns ``True`` if any content was cleared.
        """
        platform_schema = schema.platformSchema
        if platform_schema is None:
            return False
        cleared = False
        for key, value in platform_schema._inner_dict.items():
            if isinstance(value, str) and value:
                platform_schema._inner_dict[key] = ""
                cleared = True
        return cleared

    def ensure_schema_metadata_size(
        self, dataset_urn: str, schema: SchemaMetadataClass
    ) -> None:
        # Budget against the full serialized aspect, not just the fields list:
        # the platformSchema blob (and other top-level content) is emitted too.
        truncated = False
        non_fields_size = self._schema_size_without_fields(schema)

        # If the non-field content alone exceeds the budget, the platformSchema
        # blob is the culprit and the least valuable content. Drop it BEFORE
        # trimming fields (which carry the metadata users query), then re-measure.
        # Otherwise the field budget would start already-oversized and reject
        # every field while the aspect stays over the limit.
        if (
            non_fields_size >= self.schema_size_constraint
            and self._drop_platform_schema(schema)
        ):
            self.ctx.source_report.warning(
                title="Schema truncated due to size constraint",
                message="Dataset schema contained too much data and would have caused ingestion to fail",
                context=f"Raw platform schema was removed from schema for {dataset_urn} due to aspect size constraints",
            )
            self.report.num_platform_schema_drops += 1
            non_fields_size = self._schema_size_without_fields(schema)
            truncated = True

        total_fields_size = non_fields_size
        accepted_fields: List[SchemaFieldClass] = []
        for schema_field in schema.fields:
            field_size = len(json.dumps(pre_json_transform(schema_field.to_obj())))
            if total_fields_size + field_size < self.schema_size_constraint:
                accepted_fields.append(schema_field)
                total_fields_size += field_size
            else:
                self.ctx.source_report.warning(
                    title="Schema truncated due to size constraint",
                    message="Dataset schema contained too much data and would have caused ingestion to fail",
                    context=f"Field {schema_field.fieldPath} was removed from schema for {dataset_urn} due to aspect size constraints",
                )
        if truncated or len(accepted_fields) < len(schema.fields):
            self._record_truncation("schemaMetadata")
        schema.fields = accepted_fields

    def ensure_query_subjects_size(
        self, entity_urn: str, query_subjects: QuerySubjectsClass
    ) -> None:
        if not query_subjects.subjects:
            return

        total_subjects_size = 0
        accepted_table_level_subjects = []
        accepted_column_level_subjects = []
        column_level_subjects_with_sizes = []
        table_level_subjects_with_sizes = []

        for subject in query_subjects.subjects:
            subject_size = len(json.dumps(pre_json_transform(subject.to_obj())))
            if subject.entity.startswith("urn:li:schemaField:"):
                column_level_subjects_with_sizes.append((subject, subject_size))
            else:
                table_level_subjects_with_sizes.append((subject, subject_size))

        first_skip_done = False
        for subject, subject_size in table_level_subjects_with_sizes:
            if total_subjects_size + subject_size < self.payload_constraint:
                accepted_table_level_subjects.append(subject)
                total_subjects_size += subject_size
            else:
                first_skip_done = True
                break

        if not first_skip_done:
            for subject, subject_size in column_level_subjects_with_sizes:
                if total_subjects_size + subject_size < self.payload_constraint:
                    accepted_column_level_subjects.append(subject)
                    total_subjects_size += subject_size
                else:
                    first_skip_done = True
                    break

        if first_skip_done:
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
            self._record_truncation("querySubjects")

        query_subjects.subjects = (
            accepted_table_level_subjects + accepted_column_level_subjects
        )

    def _maybe_warn_query_subjects(
        self, entity_urn: str, skipped_count: int, item_type: str
    ) -> None:
        if skipped_count > 0:
            self.ctx.source_report.warning(
                title="Query subjects truncated due to size constraint",
                message="Query subjects contained too much data and would have caused ingestion to fail",
                context=f"Skipped {skipped_count} {item_type} for {entity_urn} due to aspect size constraints",
            )

    def _maybe_warn_upstream_lineage(
        self, entity_urn: str, skipped_count: int, item_type: str
    ) -> None:
        if skipped_count > 0:
            self.ctx.source_report.warning(
                title="Upstream lineage truncated due to size constraint",
                message="Upstream lineage contained too much data and would have caused ingestion to fail",
                context=f"Skipped {skipped_count} {item_type} for {entity_urn} due to aspect size constraints",
            )

    def _truncate_or_remove_field(
        self, field_value: Optional[str], reduction_needed: int
    ) -> Optional[_TruncationResult]:
        if not field_value:
            return None
        field_size = len(field_value)
        if field_size > reduction_needed > 0:
            retained_length = field_size - reduction_needed
            truncated_content = field_value[:retained_length]
            truncation_msg = (
                f"... [truncated from {field_size} to {retained_length} chars]"
            )
            return _TruncationResult(
                truncated_value=truncated_content + truncation_msg,
                original_size=field_size,
                retained_length=retained_length,
            )
        else:
            return _TruncationResult(
                truncated_value=f"[removed due to size - original was {field_size} chars]",
                original_size=field_size,
                retained_length=0,
            )

    def ensure_upstream_lineage_size(  # noqa: C901
        self, entity_urn: str, upstream_lineage: UpstreamLineageClass
    ) -> None:
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

        if upstream_lineage.upstreams:
            for upstream in upstream_lineage.upstreams:
                upstream_size = len(json.dumps(pre_json_transform(upstream.to_obj())))
                upstream_items_with_sizes.append((upstream, upstream_size))

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

        first_skip_done = False

        for item, item_size in upstream_items_with_sizes:
            if total_lineage_size + item_size < self.payload_constraint:
                accepted_upstreams.append(item)
                total_lineage_size += item_size
            else:
                first_skip_done = True
                break

        if not first_skip_done:
            for fg_lineage, fg_lineage_size in dataset_fg_items_with_sizes:
                if total_lineage_size + fg_lineage_size < self.payload_constraint:
                    accepted_dataset_fg_lineages.append(fg_lineage)
                    total_lineage_size += fg_lineage_size
                else:
                    first_skip_done = True
                    break

        if not first_skip_done:
            for fg_lineage, fg_lineage_size in field_set_fg_items_with_sizes:
                if total_lineage_size + fg_lineage_size < self.payload_constraint:
                    accepted_field_set_fg_lineages.append(fg_lineage)
                    total_lineage_size += fg_lineage_size
                else:
                    first_skip_done = True
                    break

        if not first_skip_done:
            for fg_lineage, fg_lineage_size in none_fg_items_with_sizes:
                if total_lineage_size + fg_lineage_size < self.payload_constraint:
                    accepted_none_fg_lineages.append(fg_lineage)
                    total_lineage_size += fg_lineage_size
                else:
                    first_skip_done = True
                    break

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
            self._record_truncation("upstreamLineage")

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

        if statement_value_size > reduction_needed > 0:
            new_statement_length = statement_value_size - reduction_needed
            truncated_statement = query_properties.statement.value[
                :new_statement_length
            ]
            truncation_message = f"... [original value was {original_statement_size} bytes and truncated to {new_statement_length} bytes]"
            query_properties.statement.value = truncated_statement + truncation_message
            self.ctx.source_report.warning(
                title="Query properties truncated due to size constraint",
                message="Query properties contained too much data and would have caused ingestion to fail",
                context=f"Query statement was truncated from {original_statement_size} to {new_statement_length} characters for {entity_urn} due to aspect size constraints",
            )
            self._record_truncation("queryProperties")
        else:
            logger.warning(
                f"Cannot truncate query statement for {entity_urn} as it is smaller than or equal to the required reduction size {reduction_needed}."
            )

    def ensure_view_properties_size(
        self, entity_urn: str, view_properties: ViewPropertiesClass
    ) -> None:
        if not view_properties.viewLogic and not view_properties.formattedViewLogic:
            return

        current_size = len(json.dumps(pre_json_transform(view_properties.to_obj())))
        if current_size < self.payload_constraint:
            return

        truncated = False
        reduction_needed = (
            current_size - self.payload_constraint + QUERY_STATEMENT_TRUNCATION_BUFFER
        )
        result = self._truncate_or_remove_field(
            view_properties.formattedViewLogic, reduction_needed
        )
        if result:
            view_properties.formattedViewLogic = result.truncated_value
            self._warn_view_properties_truncation(
                entity_urn, "formattedViewLogic", result
            )
            truncated = True

        current_size = len(json.dumps(pre_json_transform(view_properties.to_obj())))
        if current_size < self.payload_constraint:
            if truncated:
                self._record_truncation("viewProperties")
            return

        reduction_needed = (
            current_size - self.payload_constraint + QUERY_STATEMENT_TRUNCATION_BUFFER
        )
        result = self._truncate_or_remove_field(
            view_properties.viewLogic, reduction_needed
        )
        if result:
            if result.retained_length == 0:
                logger.warning(
                    f"Cannot truncate viewLogic for {entity_urn} as it is smaller than "
                    f"or equal to the required reduction size {reduction_needed}. "
                    f"Removing viewLogic entirely."
                )
            view_properties.viewLogic = result.truncated_value
            self._warn_view_properties_truncation(entity_urn, "viewLogic", result)
            truncated = True

        if truncated:
            self._record_truncation("viewProperties")

    def _warn_view_properties_truncation(
        self, entity_urn: str, field_name: str, result: _TruncationResult
    ) -> None:
        if result.retained_length == 0:
            context = f"{field_name} was removed for {entity_urn} (original was {result.original_size} chars)"
        else:
            context = f"{field_name} was truncated from {result.original_size} to {result.retained_length} chars for {entity_urn}"
        self.ctx.source_report.warning(
            title="View properties truncated due to size constraint",
            message="View properties contained too much data and would have caused ingestion to fail",
            context=context,
        )
