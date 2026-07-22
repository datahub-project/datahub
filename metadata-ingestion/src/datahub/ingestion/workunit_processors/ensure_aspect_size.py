import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional

from datahub.emitter.rest_emitter import INGEST_MAX_PAYLOAD_BYTES
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    BinaryJsonSchemaClass,
    DatasetProfileClass,
    EspressoSchemaClass,
    KafkaSchemaClass,
    KeyValueSchemaClass,
    MySqlDDLClass,
    OracleDDLClass,
    OrcSchemaClass,
    OtherSchemaClass,
    PrestoDDLClass,
    QueryPropertiesClass,
    QuerySubjectsClass,
    SchemaFieldClass,
    SchemalessClass,
    SchemaMetadataClass,
    SemanticModelInfoClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)

logger = logging.getLogger(__name__)

# Cap queryProperties tighter than the generic 16MB aspect limit because
# these MCPs are also emitted over Kafka on the async path, where the producer
# `max.request.size` defaults to 5MB. GMS's 16MB AspectSizePayloadValidator
# is not the binding limit here.
DEFAULT_QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES = 5 * 1024 * 1024  # 5MB
QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES = int(
    os.environ.get(
        "QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES",
        DEFAULT_QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES,
    )
)
QUERY_STATEMENT_TRUNCATION_BUFFER = 100


def _largest_fitting_prefix(text: str, fits: Callable[[int], bool]) -> int:
    """Binary-search the largest prefix length ``n`` of ``text`` for which
    ``fits(n)`` is True.

    ``fits`` must be monotonic: ``fits(k)`` True implies ``fits(k - 1)`` True
    (holds for size predicates, since a shorter prefix is never larger). Returns
    0 if even ``fits(0)`` is False.
    """
    lo, hi, retained = 0, len(text), 0
    while lo <= hi:
        mid = (lo + hi) // 2
        if fits(mid):
            retained = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return retained


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
            if semantic_model_info := wu.get_aspect_of_type(SemanticModelInfoClass):
                self.ensure_semantic_model_info_size(urn, semantic_model_info)
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

        ``platformSchema`` holds an opaque, source-format dump of the schema and
        is the least valuable part of the aspect (``fields`` carry the structured,
        queryable metadata), so we shed it before trimming fields. Each union
        variant is handled explicitly so the type checker verifies the field
        names — a model change surfaces as a mypy error rather than a silent
        no-op. Small markers such as ``KafkaSchema.documentSchemaType`` are left
        intact. An unrecognized variant is warned about and left untouched.
        Returns ``True`` if any content was cleared.
        """
        ps = schema.platformSchema
        if ps is None or isinstance(ps, SchemalessClass):
            return False
        # Direct attribute access (not getattr/setattr) so mypy verifies each
        # field name against the model.
        if isinstance(ps, (OtherSchemaClass, PrestoDDLClass)):
            if ps.rawSchema:
                ps.rawSchema = ""
                return True
            return False
        if isinstance(ps, (MySqlDDLClass, OracleDDLClass)):
            if ps.tableSchema:
                ps.tableSchema = ""
                return True
            return False
        if isinstance(ps, (OrcSchemaClass, BinaryJsonSchemaClass)):
            if ps.schema:
                ps.schema = ""
                return True
            return False
        if isinstance(ps, KafkaSchemaClass):
            cleared = False
            if ps.documentSchema:
                ps.documentSchema = ""
                cleared = True
            if ps.keySchema:
                ps.keySchema = ""
                cleared = True
            return cleared
        if isinstance(ps, EspressoSchemaClass):
            cleared = False
            if ps.documentSchema:
                ps.documentSchema = ""
                cleared = True
            if ps.tableSchema:
                ps.tableSchema = ""
                cleared = True
            return cleared
        if isinstance(ps, KeyValueSchemaClass):
            cleared = False
            if ps.keySchema:
                ps.keySchema = ""
                cleared = True
            if ps.valueSchema:
                ps.valueSchema = ""
                cleared = True
            return cleared
        self.ctx.source_report.warning(
            title="Unrecognized platformSchema variant",
            message="Don't know how to trim the raw schema for this platformSchema "
            "variant, so it was left untouched and the aspect may still exceed the "
            "size limit. The variant likely needs a branch in _drop_platform_schema.",
            context=type(ps).__name__,
        )
        return False

    def _extract_field_path_depth(self, field: SchemaFieldClass) -> int:
        return len(
            [t for t in re.sub(r"\[[^]]*]", "", field.fieldPath).split(".") if t]
        )

    def ensure_schema_metadata_size(
        self, dataset_urn: str, schema: SchemaMetadataClass
    ) -> None:
        # Fast path: if the whole aspect already fits, there's nothing to trim.
        if (
            len(json.dumps(pre_json_transform(schema.to_obj())))
            < self.schema_size_constraint
        ):
            return

        # Over budget. Shed the least-valuable content first — the platformSchema
        # blob (an opaque source-format dump) — so we keep as many fields (the
        # structured, queryable metadata) as possible. Only after that do we trim
        # fields, and only if they still don't fit. This keeps "fields > raw
        # platform schema" even when neither part is individually over the limit
        # (e.g. a ~half-schema / ~half-fields aspect).
        truncated = False
        if self._drop_platform_schema(schema):
            self.ctx.source_report.warning(
                title="Schema truncated due to size constraint",
                message="Dataset schema contained too much data and would have caused ingestion to fail",
                context=f"Raw platform schema was removed from schema for {dataset_urn} due to aspect size constraints",
            )
            self.report.num_platform_schema_drops += 1
            truncated = True

        total_fields_size = self._schema_size_without_fields(schema)
        accepted_fields: List[SchemaFieldClass] = []
        sorted_fields = sorted(schema.fields, key=self._extract_field_path_depth)
        for schema_field in sorted_fields:
            field_size = len(json.dumps(pre_json_transform(schema_field.to_obj())))
            if total_fields_size + field_size >= self.schema_size_constraint:
                break
            accepted_fields.append(schema_field)
            total_fields_size += field_size

        dropped_field_count = len(schema.fields) - len(accepted_fields)
        if dropped_field_count:
            # Report a single warning per entity with the dropped count, rather
            # than one warning per dropped field.
            self.ctx.source_report.warning(
                title="Schema truncated due to size constraint",
                message="Dataset schema contained too much data and would have caused ingestion to fail; some fields were dropped",
                context=f"Dropped {dropped_field_count} of {len(schema.fields)} fields from schema for {dataset_urn} due to aspect size constraints",
            )
        if truncated or dropped_field_count:
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

    def _query_properties_serialized_size(
        self, query_properties: QueryPropertiesClass
    ) -> int:
        return len(json.dumps(pre_json_transform(query_properties.to_obj())))

    def ensure_query_properties_size(
        self, entity_urn: str, query_properties: QueryPropertiesClass
    ) -> None:
        if not query_properties.statement or not query_properties.statement.value:
            return

        max_payload_size = min(
            QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES, self.payload_constraint
        )
        if self._query_properties_serialized_size(query_properties) < max_payload_size:
            return

        full_statement = query_properties.statement.value
        original_statement_size = len(full_statement)

        # Trim by serialized-byte budget, not raw char count: JSON escaping
        # (\n, \", non-ASCII) inflates wire size past raw length, so a raw-vs-byte
        # compare undercounts and lets oversized aspects through (GMS then 400s).
        # Measure the non-statement overhead once so each search step only encodes
        # the candidate, not the whole aspect.
        query_properties.statement.value = ""
        empty_overhead = self._query_properties_serialized_size(query_properties)
        overhead_without_value = empty_overhead - len(json.dumps(""))

        def candidate_value(retained: int) -> str:
            return (
                full_statement[:retained]
                + f"... [original value was {original_statement_size} bytes and truncated to {retained} bytes]"
            )

        def fits(retained: int) -> bool:
            serialized = overhead_without_value + len(
                json.dumps(candidate_value(retained))
            )
            return serialized < max_payload_size

        retained_length = _largest_fitting_prefix(full_statement, fits)
        query_properties.statement.value = candidate_value(retained_length)

        # If even an empty statement can't make it fit, the other fields are over
        # budget on their own; drop them as a last resort.
        dropped_fields: List[str] = []
        if self._query_properties_serialized_size(query_properties) >= max_payload_size:
            if query_properties.name:
                query_properties.name = None
                dropped_fields.append("name")
            if query_properties.description:
                query_properties.description = None
                dropped_fields.append("description")

        # Re-verify: if another field (e.g. a large customProperties) still keeps
        # the aspect over the limit, don't claim a successful truncation — warn
        # instead so the likely GMS 400 is visible rather than silent.
        final_size = self._query_properties_serialized_size(query_properties)
        if final_size >= max_payload_size:
            self.ctx.source_report.warning(
                title="Query properties could not be truncated below size constraint",
                message="Query properties remained too large after truncating the statement and dropping name/description; the aspect may be rejected by GMS",
                context=(
                    f"entity_urn={entity_urn}, "
                    f"serialized_size={final_size}, "
                    f"budget={max_payload_size}"
                ),
            )
            return

        context = (
            f"entity_urn={entity_urn}, "
            f"original_size={original_statement_size}, "
            f"retained_size={retained_length}"
        )
        if dropped_fields:
            context += f", dropped_fields={','.join(dropped_fields)}"
        self.ctx.source_report.warning(
            title="Query properties truncated due to size constraint",
            message="Query properties contained too much data and would have caused ingestion to fail",
            context=context,
        )
        self._record_truncation("queryProperties")

    def _view_properties_serialized_size(
        self, view_properties: ViewPropertiesClass
    ) -> int:
        return len(json.dumps(pre_json_transform(view_properties.to_obj())))

    def _shrink_view_properties_field(
        self,
        view_properties: ViewPropertiesClass,
        field_name: str,
    ) -> Optional[_TruncationResult]:
        # Serialized-byte budget + overhead-measured-once pattern; see
        # ensure_query_properties_size for the rationale.
        original_value: Optional[str] = getattr(view_properties, field_name)
        if not original_value:
            return None
        original_size = len(original_value)

        setattr(view_properties, field_name, "")
        empty_overhead = self._view_properties_serialized_size(view_properties)
        overhead_without_value = empty_overhead - len(json.dumps(""))

        def candidate_value(retained: int) -> str:
            if retained == 0:
                return f"[removed due to size - original was {original_size} chars]"
            return (
                original_value[:retained]
                + f"... [truncated from {original_size} to {retained} chars]"
            )

        def fits(retained: int) -> bool:
            serialized = overhead_without_value + len(
                json.dumps(candidate_value(retained))
            )
            return serialized < self.payload_constraint

        retained_length = _largest_fitting_prefix(original_value, fits)
        new_value = candidate_value(retained_length)
        setattr(view_properties, field_name, new_value)
        if retained_length == 0:
            logger.warning(
                f"Cannot fit {field_name} within payload_constraint "
                f"{self.payload_constraint}; removing content entirely "
                f"(original was {original_size} chars)."
            )
        return _TruncationResult(
            truncated_value=new_value,
            original_size=original_size,
            retained_length=retained_length,
        )

    def ensure_view_properties_size(
        self, entity_urn: str, view_properties: ViewPropertiesClass
    ) -> None:
        if not view_properties.viewLogic and not view_properties.formattedViewLogic:
            return

        if (
            self._view_properties_serialized_size(view_properties)
            < self.payload_constraint
        ):
            return

        truncated = False

        # Trim the compiled view first — it's the derived form; the raw viewLogic
        # is the source of truth and only truncated if trimming the compiled form
        # isn't enough.
        result = self._shrink_view_properties_field(
            view_properties, "formattedViewLogic"
        )
        if result:
            self._warn_view_properties_truncation(
                entity_urn, "formattedViewLogic", result
            )
            truncated = True

        if (
            self._view_properties_serialized_size(view_properties)
            < self.payload_constraint
        ):
            if truncated:
                self._record_truncation("viewProperties")
            return

        result = self._shrink_view_properties_field(view_properties, "viewLogic")
        if result:
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

    def _semantic_model_info_serialized_size(
        self, semantic_model_info: SemanticModelInfoClass
    ) -> int:
        return len(json.dumps(pre_json_transform(semantic_model_info.to_obj())))

    def ensure_semantic_model_info_size(
        self, entity_urn: str, semantic_model_info: SemanticModelInfoClass
    ) -> None:
        # semanticModelInfo's large parts are nativeDefinition (the DDL) and the
        # datasets array. Only nativeDefinition is trimmed, and only when the whole
        # aspect exceeds the payload limit; the datasets array carries the primary
        # queryable metadata and is left intact. See ensure_query_properties_size.
        original_value = semantic_model_info.nativeDefinition
        if not original_value:
            return

        if (
            self._semantic_model_info_serialized_size(semantic_model_info)
            < self.payload_constraint
        ):
            return

        original_size = len(original_value)

        semantic_model_info.nativeDefinition = ""
        empty_overhead = self._semantic_model_info_serialized_size(semantic_model_info)
        overhead_without_value = empty_overhead - len(json.dumps(""))

        def candidate_value(retained: int) -> str:
            if retained == 0:
                return f"[removed due to size - original was {original_size} chars]"
            return (
                original_value[:retained]
                + f"... [truncated from {original_size} to {retained} chars]"
            )

        def fits(retained: int) -> bool:
            serialized = overhead_without_value + len(
                json.dumps(candidate_value(retained))
            )
            return serialized < self.payload_constraint

        retained_length = _largest_fitting_prefix(original_value, fits)
        semantic_model_info.nativeDefinition = candidate_value(retained_length)

        if retained_length == 0:
            context = f"nativeDefinition was removed for {entity_urn} (original was {original_size} chars)"
        else:
            context = f"nativeDefinition was truncated from {original_size} to {retained_length} chars for {entity_urn}"
        self.ctx.source_report.warning(
            title="Semantic model info truncated due to size constraint",
            message="Semantic model info contained too much data and would have caused ingestion to fail",
            context=context,
        )
        self._record_truncation("semanticModelInfo")

        # If the aspect is still over budget after minimizing nativeDefinition, the
        # datasets array alone is oversized. We deliberately do not trim it (it is the
        # primary structured payload and dropping fields would corrupt the model), so
        # warn that the aspect may still be rejected by GMS rather than fail silently.
        if (
            self._semantic_model_info_serialized_size(semantic_model_info)
            >= self.payload_constraint
        ):
            self.ctx.source_report.warning(
                title="Semantic model info remains oversized after truncation",
                message="semanticModelInfo remained too large after truncating "
                "nativeDefinition; the datasets structure was not trimmed and the "
                "aspect may be rejected by GMS",
                context=entity_urn,
            )
