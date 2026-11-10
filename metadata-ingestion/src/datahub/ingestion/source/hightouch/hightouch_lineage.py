import logging
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import (
    QUERY_TYPE_RAW_SQL,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.hightouch_utils import (
    SchemaMetadataCache,
    fetch_schema_metadata_cached,
    normalize_column_name,
    reraise_if_programming_error,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchColumnPair,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
)
from datahub.ingestion.source.hightouch.protocols import (
    GetAggregatorForPlatform,
    GetPlatformForSource,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SchemaFieldClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)


class HightouchLineageHandler:
    def __init__(
        self,
        config: HightouchSourceConfig,
        api_client: HightouchAPIClient,
        report: HightouchSourceReport,
        urn_builder: HightouchUrnBuilder,
        graph: Optional[DataHubGraph],
        registered_urns: Set[str],
        model_schema_fields_cache: Dict[str, List[SchemaFieldClass]],
        sql_aggregators: Dict[
            Tuple[str, Optional[str], str], Optional[SqlParsingAggregator]
        ],
        schema_metadata_cache: SchemaMetadataCache,
    ) -> None:
        self.config = config
        self.api_client = api_client
        self.report = report
        self.urn_builder = urn_builder
        self.graph = graph
        self._registered_urns = registered_urns
        self._model_schema_fields_cache = model_schema_fields_cache
        # Upstream table URNs parsed from each raw-SQL model, by model id.
        self._sql_upstreams: Dict[str, List[str]] = {}
        self._sql_aggregators = sql_aggregators
        self._schema_metadata_cache = schema_metadata_cache

    def _fetch_schema_field_paths(self, urn: str, kind: str) -> Optional[List[str]]:
        if not self.graph:
            return None
        try:
            metadata = fetch_schema_metadata_cached(
                self.graph, self._schema_metadata_cache, urn
            )
            if metadata and metadata.fields:
                return [f.fieldPath for f in metadata.fields]
        except Exception as e:
            reraise_if_programming_error(e, f"fetching {kind} schema")
            self.report.report_datahub_lookup_failure(
                f"{kind} schema for column matching", urn, e
            )
        return None

    def get_upstream_field_casing(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        sql_table_urns: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        # Map normalized field names to their upstream casing so model schemas line
        # up with the source table for sibling visualization. Only resolvable for
        # table models and single-table SQL models.
        if not source or not self.graph:
            return {}

        upstream_urn = None

        table_urn = self.urn_builder.source_table_urn(model, source)
        if table_urn:
            upstream_urn = table_urn

        # For SQL models (raw_sql, custom, dbt, etc.), upstream casing only makes
        # sense when the SQL references exactly one table.
        elif sql_table_urns:
            if len(sql_table_urns) == 1:
                upstream_urn = sql_table_urns[0]
            else:
                return {}
        else:
            return {}

        try:
            upstream_schema = fetch_schema_metadata_cached(
                self.graph, self._schema_metadata_cache, str(upstream_urn)
            )
            if not upstream_schema or not upstream_schema.fields:
                return {}

            field_casing_map: Dict[str, str] = {}
            for field in upstream_schema.fields:
                normalized = normalize_column_name(field.fieldPath)
                field_casing_map[normalized] = field.fieldPath

            return field_casing_map

        except Exception as e:
            reraise_if_programming_error(
                e, f"fetching field casing for model {model.slug}"
            )
            self.report.report_datahub_lookup_failure(
                "upstream field casing", f"model_slug: {model.slug}", e
            )
            return {}

    def generate_table_model_column_lineage(
        self,
        model: HightouchModel,
        model_urn: str,
        upstream_table_urn: str,
        model_schema_fields: List[SchemaFieldClass],
    ) -> List[FineGrainedLineageClass]:
        if not self.graph:
            return []

        fine_grained_lineages = []

        try:
            upstream_schema = fetch_schema_metadata_cached(
                self.graph, self._schema_metadata_cache, upstream_table_urn
            )

            if not upstream_schema or not upstream_schema.fields:
                return []

            if not model_schema_fields:
                return []

            upstream_fields = {f.fieldPath: f for f in upstream_schema.fields}
            model_fields = {f.fieldPath: f for f in model_schema_fields}

            # For table models, columns map 1:1 from upstream to model. Fuzzy match
            # to handle casing differences (e.g. Snowflake lowercasing).
            for model_field_path in model_fields:
                matched_upstream_field = None
                normalized_model_field = normalize_column_name(model_field_path)

                for upstream_field_path in upstream_fields:
                    if (
                        normalize_column_name(upstream_field_path)
                        == normalized_model_field
                    ):
                        matched_upstream_field = upstream_field_path
                        break

                if matched_upstream_field:
                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                make_schema_field_urn(
                                    upstream_table_urn, matched_upstream_field
                                )
                            ],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            downstreams=[
                                make_schema_field_urn(model_urn, model_field_path)
                            ],
                        )
                    )

        except Exception as e:
            reraise_if_programming_error(
                e, f"generating column lineage for {model.slug}"
            )
            self.report.report_lineage_resolution_failure(
                f"table_model_cll model_slug: {model.slug}"
            )
            self.report.warning(
                title="Could not generate column-level lineage for table model",
                message="Column-level lineage for this table model could not be "
                "resolved from the upstream schema and will be missing.",
                context=f"model_slug: {model.slug}",
                exc=e,
            )

        return fine_grained_lineages

    def register_model_lineage(
        self,
        model: HightouchModel,
        model_urn: str,
        source: HightouchSourceConnection,
        get_platform_for_source_fn: GetPlatformForSource,
        get_aggregator_for_platform_fn: GetAggregatorForPlatform,
    ) -> None:
        source_platform = get_platform_for_source_fn(source)
        if not source_platform.platform:
            return

        aggregator = get_aggregator_for_platform_fn(source_platform)
        if not aggregator:
            return

        # Table models emit their own UpstreamLineage in the model handler. For
        # raw_sql models, register the view definition for SQL parsing (skipped when
        # SQL parsing is disabled so no lineage is derived from the model's SQL).
        # Without the schema in URNs the model handler emits the lineage instead,
        # because the aggregator would keep the schema segment.
        if (
            self.config.parse_model_sql
            and model.raw_sql
            and model.query_type == QUERY_TYPE_RAW_SQL
            and source_platform.include_schema_in_urn
        ):
            self.report.sql_parsing_attempts += 1

            try:
                aggregator.add_view_definition(
                    view_urn=model_urn,
                    view_definition=model.raw_sql,
                    default_db=source_platform.database,
                    default_schema=None,
                )
                self.report.sql_parsing_successes += 1
            except Exception as e:
                reraise_if_programming_error(
                    e, f"registering view definition for {model.id}"
                )
                self.report.sql_parsing_failures += 1
                self.report.warning(
                    title="View definition registration error",
                    message=f"Could not register view definition for model '{model.name}'. "
                    f"This may be due to an unsupported SQL dialect or parsing error. "
                    f"Basic lineage will still be emitted.",
                    context=f"model_id: {model.id}, platform: {source_platform.platform}",
                    exc=e,
                )

    def sql_upstream_urns(
        self, model: HightouchModel, source: HightouchSourceConnection
    ) -> List[str]:
        """Upstream tables of a raw-SQL model, parsed once per model and normalized
        to the source platform's URN scheme (honoring include_schema_in_urn)."""
        if model.id in self._sql_upstreams:
            return self._sql_upstreams[model.id]
        upstreams = self._parse_sql_upstreams(model, source)
        self._sql_upstreams[model.id] = upstreams
        return upstreams

    def _parse_sql_upstreams(
        self, model: HightouchModel, source: HightouchSourceConnection
    ) -> List[str]:
        if not (self.config.parse_model_sql and model.raw_sql):
            return []

        source_platform = self.urn_builder.get_platform_for_source(source)
        if not source_platform.platform:
            return []

        self.report.sql_parsing_attempts += 1
        result = create_lineage_sql_parsed_result(
            query=model.raw_sql,
            default_db=source_platform.database,
            default_schema=None,
            platform=source_platform.platform,
            platform_instance=source_platform.platform_instance,
            env=source_platform.env or self.config.env,
            graph=None,
            schema_aware=False,
        )

        if result.debug_info.table_error:
            self.report.sql_parsing_failures += 1
            self.report.report_lineage_resolution_failure(
                f"sql_model_upstreams model_slug: {model.slug}"
            )
            self.report.warning(
                title="Could not parse model SQL for upstream lineage",
                message="Upstream table lineage could not be derived from the "
                "model's SQL and will be missing for this model and its syncs.",
                context=f"model_slug: {model.slug}",
                exc=result.debug_info.table_error,
            )
            return []

        self.report.sql_parsing_successes += 1
        return [
            self.urn_builder.normalize_parsed_upstream_urn(urn, source)
            for urn in result.in_tables
        ]

    def normalize_and_match_column(
        self,
        source_field: str,
        destination_field: str,
        model_schema: Optional[List[str]] = None,
        dest_schema: Optional[List[str]] = None,
    ) -> HightouchColumnPair:
        if not model_schema and not dest_schema:
            return HightouchColumnPair(
                source_field=source_field, destination_field=destination_field
            )

        validated_source = source_field
        validated_dest = destination_field

        if model_schema:
            normalized_source = normalize_column_name(source_field)
            for schema_field in model_schema:
                if normalize_column_name(schema_field) == normalized_source:
                    validated_source = schema_field
                    break

        if dest_schema:
            normalized_dest = normalize_column_name(destination_field)
            for schema_field in dest_schema:
                if normalize_column_name(schema_field) == normalized_dest:
                    validated_dest = schema_field
                    break

        return HightouchColumnPair(
            source_field=validated_source, destination_field=validated_dest
        )

    def generate_column_lineage(
        self,
        sync: HightouchSync,
        model: HightouchModel,
        inlet_urn: Union[str, DatasetUrn],
        outlet_urn: Union[str, DatasetUrn],
        model_schema_fields_override: Optional[List[SchemaFieldClass]] = None,
    ) -> List[FineGrainedLineageClass]:
        field_mappings = self.api_client.extract_field_mappings(sync)
        if not field_mappings:
            return []

        fine_grained_lineages = []
        model_schema_fields = None
        dest_schema_fields = None

        if model_schema_fields_override:
            model_schema_fields = [f.fieldPath for f in model_schema_fields_override]
        elif self.graph:
            model_schema_fields = self._fetch_schema_field_paths(
                str(inlet_urn), "model"
            )

        # Destination schema is resolved independently of how the model schema was
        # obtained; otherwise a supplied model override would leave destination
        # field mappings in API casing and produce mismatched schema-field URNs.
        if self.graph:
            dest_schema_fields = self._fetch_schema_field_paths(
                str(outlet_urn), "destination"
            )

        for mapping in field_mappings:
            column_pair = self.normalize_and_match_column(
                mapping.source_field,
                mapping.destination_field,
                model_schema_fields,
                dest_schema_fields,
            )

            fine_grained_lineages.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[
                        make_schema_field_urn(str(inlet_urn), column_pair.source_field)
                    ],
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[
                        make_schema_field_urn(
                            str(outlet_urn), column_pair.destination_field
                        )
                    ],
                )
            )

        if fine_grained_lineages:
            self.report.column_lineage_emitted += len(fine_grained_lineages)

        return fine_grained_lineages

    def emit_sibling_aspects(
        self, model_urn: str, source_table_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        # Patches, not full Siblings aspects: the source table may already have
        # siblings (e.g. from dbt), and several models can share one source table.
        for mcp in (
            DatasetPatchBuilder(model_urn)
            .add_sibling(source_table_urn, primary=True)
            .build()
        ):
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
            )

        # Only link the source table back if it was preloaded (i.e. it exists in
        # DataHub), to avoid creating ghost entities.
        if source_table_urn in self._registered_urns:
            for mcp in (
                DatasetPatchBuilder(source_table_urn).add_sibling(model_urn).build()
            ):
                yield MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(mcp),
                    mcp_raw=mcp,
                    is_primary_source=False,
                )
