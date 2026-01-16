import logging
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Union

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.models import (
    HightouchColumnPair,
    HightouchDestinationLineageInfo,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SchemaFieldClass,
    SiblingsClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)


def normalize_column_name(name: str) -> str:
    """Normalize column name for fuzzy matching (lowercase, remove underscores/hyphens)."""
    return name.lower().replace("_", "").replace("-", "")


class HightouchLineageHandler:
    def __init__(
        self,
        api_client: HightouchAPIClient,
        report: HightouchSourceReport,
        urn_builder: HightouchUrnBuilder,
        graph: Optional[DataHubGraph],
        registered_urns: Set[str],
        model_schema_fields_cache: Dict[str, List[SchemaFieldClass]],
        destination_lineage: Dict[str, HightouchDestinationLineageInfo],
        sql_aggregators: Dict[str, Optional[SqlParsingAggregator]],
    ) -> None:
        self.api_client = api_client
        self.report = report
        self.urn_builder = urn_builder
        self.graph = graph
        self._registered_urns = registered_urns
        self._model_schema_fields_cache = model_schema_fields_cache
        self._destination_lineage = destination_lineage
        self._sql_aggregators = sql_aggregators

    def get_upstream_field_casing(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        sql_table_urns: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """
        For table models and SQL models with a single upstream table, fetch upstream table schema
        and return a mapping from normalized field names to the actual upstream field names with correct casing.
        This ensures Hightouch model schemas match upstream casing for proper sibling visualization.

        Works for:
        - table models: Uses model.name to build upstream URN
        - SQL models (raw_sql, custom, dbt, etc.): Uses parsed SQL table URNs if exactly one table is referenced
        """
        if not source or not self.graph:
            return {}

        upstream_urn = None

        if model.query_type == "table" and model.name:
            table_name = model.name
            if source.configuration:
                database = source.configuration.get("database", "")
                schema = source.configuration.get("schema", "")
                source_details = self.urn_builder._get_cached_source_details(source)
                if source_details.include_schema_in_urn and schema:
                    table_name = f"{database}.{schema}.{table_name}"
                elif database and "." not in table_name:
                    table_name = f"{database}.{table_name}"

            upstream_urn = self.urn_builder.make_upstream_table_urn(table_name, source)
            if not upstream_urn:
                logger.debug(
                    f"Could not generate upstream URN for table model {model.slug} (table_name={table_name})"
                )
                return {}

        # For SQL models (raw_sql, custom, dbt, etc.), use SQL-parsed table URNs if exactly one table is referenced
        elif sql_table_urns:
            if len(sql_table_urns) == 1:
                upstream_urn = sql_table_urns[0]
                logger.debug(
                    f"Using SQL-parsed upstream URN for model {model.slug} (query_type={model.query_type}): {upstream_urn}"
                )
            else:
                logger.debug(
                    f"Skipping upstream casing for model {model.slug} (query_type={model.query_type}): "
                    f"model references {len(sql_table_urns)} tables (need exactly 1)"
                )
                return {}
        else:
            return {}

        try:
            logger.debug(
                f"Fetching upstream schema for model {model.slug} from URN: {upstream_urn}"
            )
            upstream_schema = self.graph.get_schema_metadata(str(upstream_urn))
            if not upstream_schema or not upstream_schema.fields:
                logger.debug(
                    f"No upstream schema found for {upstream_urn} for model {model.slug}"
                )
                return {}

            # Create mapping: normalized field name -> actual upstream field name
            field_casing_map: Dict[str, str] = {}
            for field in upstream_schema.fields:
                normalized = normalize_column_name(field.fieldPath)
                field_casing_map[normalized] = field.fieldPath

            logger.debug(
                f"Fetched {len(field_casing_map)} field casings from upstream table {upstream_urn} "
                f"for model {model.slug}. Field mapping: {field_casing_map}"
            )
            return field_casing_map

        except (AttributeError, TypeError) as e:
            logger.error(
                f"Programming error fetching field casing for model {model.slug}: {type(e).__name__}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.debug(
                f"Could not fetch upstream field casing for model {model.slug} from {upstream_urn} (optional feature): {e}"
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
            upstream_schema = self.graph.get_schema_metadata(upstream_table_urn)

            if not upstream_schema or not upstream_schema.fields:
                logger.debug(
                    f"No upstream schema found for {upstream_table_urn}, skipping column lineage"
                )
                return []

            if not model_schema_fields:
                logger.debug(
                    f"No model schema fields provided for {model_urn}, skipping column lineage"
                )
                return []

            upstream_fields = {f.fieldPath: f for f in upstream_schema.fields}
            model_fields = {f.fieldPath: f for f in model_schema_fields}

            # For table models, columns map 1:1 from upstream to model
            # Use fuzzy matching to handle casing differences (e.g., Snowflake lowercasing)
            for model_field_path in model_fields:
                # Try to find matching upstream field using normalization
                matched_upstream_field = None
                normalized_model_field = normalize_column_name(model_field_path)

                for upstream_field_path in upstream_fields:
                    if (
                        normalize_column_name(upstream_field_path)
                        == normalized_model_field
                    ):
                        matched_upstream_field = upstream_field_path
                        logger.debug(
                            f"Matched model field '{model_field_path}' to upstream field '{upstream_field_path}'"
                        )
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
                else:
                    logger.debug(
                        f"No matching upstream field found for model field '{model_field_path}'"
                    )

            if fine_grained_lineages:
                logger.debug(
                    f"Generated {len(fine_grained_lineages)} column lineage edges for table model {model.slug}"
                )

        except (AttributeError, TypeError, KeyError) as e:
            logger.error(
                f"Programming error generating column lineage for {model.slug}: {type(e).__name__}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.debug(
                f"Failed to generate column lineage for table model {model.slug} (optional feature): {e}"
            )

        return fine_grained_lineages

    def register_model_lineage(
        self,
        model: HightouchModel,
        model_urn: str,
        source: HightouchSourceConnection,
        get_platform_for_source_fn: Callable[
            [HightouchSourceConnection], PlatformDetail
        ],
        get_aggregator_for_platform_fn: Callable[
            [PlatformDetail], Optional[SqlParsingAggregator]
        ],
    ) -> None:
        source_platform = get_platform_for_source_fn(source)
        if not source_platform.platform:
            logger.debug(
                f"No platform mapping for source {source.type}, skipping lineage registration"
            )
            return

        aggregator = get_aggregator_for_platform_fn(source_platform)
        if not aggregator:
            logger.debug(
                f"No SQL aggregator for platform {source_platform.platform}, skipping lineage registration"
            )
            return

        if model.query_type == "table" and model.name:
            table_name = model.name
            if source.configuration:
                database = source.configuration.get("database", "")
                schema = source.configuration.get("schema", "")
                source_details = self.urn_builder._get_cached_source_details(source)
                if source_details.include_schema_in_urn and schema:
                    table_name = f"{database}.{schema}.{table_name}"
                elif database and "." not in table_name:
                    table_name = f"{database}.{table_name}"

            upstream_urn = self.urn_builder.make_upstream_table_urn(table_name, source)

            try:
                aggregator.add_known_lineage_mapping(
                    upstream_urn=str(upstream_urn),
                    downstream_urn=model_urn,
                    lineage_type=DatasetLineageTypeClass.COPY,
                )
                logger.debug(
                    f"Registered known lineage: {upstream_urn} -> {model_urn} (table reference) "
                    f"on platform {source_platform.platform}"
                )

                # Column-level lineage for table models is emitted directly in _emit_model_aspects
                # via the UpstreamLineage aspect with fineGrainedLineages, so no need to register
                # it separately with the SQL aggregator
                logger.debug(
                    f"Table model {model.slug} will have column lineage emitted directly via UpstreamLineage aspect"
                )

            except (AttributeError, TypeError) as e:
                logger.error(
                    f"Programming error registering lineage for {model.id}: {type(e).__name__}: {e}",
                    exc_info=True,
                )
                raise
            except Exception as e:
                logger.debug(
                    f"Failed to register known lineage for model {model.id} (optional SQL parsing feature): {e}"
                )

        # For raw_sql models, register view definition for SQL parsing
        elif (
            model.raw_sql
            and model.query_type == "raw_sql"
            and hasattr(aggregator, "add_view_definition")
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
                logger.debug(
                    f"Registered view definition for model {model.id} in aggregator "
                    f"for platform {source_platform.platform}"
                )
            except (AttributeError, TypeError) as e:
                logger.error(
                    f"Programming error registering view definition for {model.id}: {type(e).__name__}: {e}",
                    exc_info=True,
                )
                raise
            except Exception as e:
                logger.debug(
                    f"Error registering view definition for model {model.id}: {e}"
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

    def normalize_and_match_column(
        self,
        source_field: str,
        destination_field: str,
        model_schema: Optional[List[str]] = None,
        dest_schema: Optional[List[str]] = None,
    ) -> HightouchColumnPair:
        """
        Normalize and fuzzy match column names against provided schemas.

        Returns a HightouchColumnPair with the validated field names.
        """
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
                    logger.debug(
                        f"Fuzzy matched source field '{source_field}' to schema field '{schema_field}'"
                    )
                    break

        if dest_schema:
            normalized_dest = normalize_column_name(destination_field)
            for schema_field in dest_schema:
                if normalize_column_name(schema_field) == normalized_dest:
                    validated_dest = schema_field
                    logger.debug(
                        f"Fuzzy matched destination field '{destination_field}' to schema field '{schema_field}'"
                    )
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

        # Use provided schema fields if available (with normalized casing)
        if model_schema_fields_override:
            model_schema_fields = [f.fieldPath for f in model_schema_fields_override]
        elif self.graph:
            try:
                model_schema_metadata = self.graph.get_schema_metadata(str(inlet_urn))
                if model_schema_metadata and model_schema_metadata.fields:
                    model_schema_fields = [
                        f.fieldPath for f in model_schema_metadata.fields
                    ]
            except (AttributeError, TypeError) as e:
                logger.error(
                    f"Programming error fetching model schema: {type(e).__name__}: {e}",
                    exc_info=True,
                )
                raise
            except Exception as e:
                logger.debug(
                    f"Could not fetch model schema for column matching (optional): {e}"
                )

            try:
                dest_schema_metadata = self.graph.get_schema_metadata(str(outlet_urn))
                if dest_schema_metadata and dest_schema_metadata.fields:
                    dest_schema_fields = [
                        f.fieldPath for f in dest_schema_metadata.fields
                    ]
            except (AttributeError, TypeError) as e:
                logger.error(
                    f"Programming error fetching destination schema: {type(e).__name__}: {e}",
                    exc_info=True,
                )
                raise
            except Exception as e:
                logger.debug(
                    f"Could not fetch destination schema for column matching (optional): {e}"
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
            logger.debug(
                f"Generated {len(fine_grained_lineages)} column lineage mappings "
                f"for sync {sync.slug}"
            )

        return fine_grained_lineages

    def accumulate_destination_lineage(
        self,
        destination_urn: str,
        upstreams: Sequence[Union[str, DatasetUrn]],
        fine_grained_lineages: Optional[List[FineGrainedLineageClass]],
    ) -> None:
        """Accumulate lineage across syncs to avoid overwriting when multiple syncs target same destination."""
        if destination_urn not in self._destination_lineage:
            self._destination_lineage[destination_urn] = (
                HightouchDestinationLineageInfo()
            )

        for inlet_urn in upstreams:
            self._destination_lineage[destination_urn].upstreams.add(str(inlet_urn))

        if fine_grained_lineages:
            self._destination_lineage[destination_urn].fine_grained_lineages.extend(
                fine_grained_lineages
            )

    def emit_all_destination_lineage(self) -> Iterable[MetadataWorkUnit]:
        """Called after all syncs processed to emit consolidated lineage."""
        for destination_urn, lineage_data in self._destination_lineage.items():
            upstreams = [
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.COPY,
                )
                for upstream_urn in lineage_data.upstreams
            ]

            fine_grained_lineages = (
                lineage_data.fine_grained_lineages
                if lineage_data.fine_grained_lineages
                else None
            )

            if upstreams:
                yield MetadataChangeProposalWrapper(
                    entityUrn=destination_urn,
                    aspect=UpstreamLineageClass(
                        upstreams=upstreams,
                        fineGrainedLineages=fine_grained_lineages,
                    ),
                ).as_workunit()

                logger.info(
                    f"Emitted consolidated upstream lineage for destination {destination_urn}: "
                    f"{len(upstreams)} upstream(s), "
                    f"{len(fine_grained_lineages) if fine_grained_lineages else 0} fine-grained lineage(s)"
                )

    def emit_sibling_aspects(
        self, model_urn: str, source_table_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        # Always emit sibling aspect on Hightouch model (primary)
        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=SiblingsClass(
                primary=True,
                siblings=[source_table_urn],
            ),
        ).as_workunit()

        # Only emit sibling aspect on source table if it was preloaded (exists in DataHub)
        # This prevents creating ghost entities with no useful metadata
        if source_table_urn in self._registered_urns:
            yield MetadataChangeProposalWrapper(
                entityUrn=source_table_urn,
                aspect=SiblingsClass(
                    primary=False,
                    siblings=[model_urn],
                ),
            ).as_workunit()
            logger.debug(
                f"Emitted bidirectional sibling relationship: {model_urn} (primary) <-> {source_table_urn} (secondary)"
            )
        else:
            logger.debug(
                f"Emitted sibling aspect on {model_urn} only - source table {source_table_urn} not in registered URNs (prevents ghost entity creation)"
            )
