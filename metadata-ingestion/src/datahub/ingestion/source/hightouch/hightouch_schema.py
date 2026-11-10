import logging
from typing import List, Optional, Set

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.hightouch.config import HightouchSourceReport
from datahub.ingestion.source.hightouch.constants import (
    QUERY_TYPE_RAW_SQL,
)
from datahub.ingestion.source.hightouch.hightouch_utils import (
    SchemaMetadataCache,
    fetch_schema_metadata_cached,
    reraise_if_programming_error,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchModel,
    HightouchSchemaField,
    HightouchSourceConnection,
    HightouchSync,
)
from datahub.ingestion.source.hightouch.protocols import (
    ExtractTableUrns,
    GetAggregatorForPlatform,
    GetDestination,
    GetModel,
    GetOutletUrnForSync,
    GetPlatformForSource,
    GetSource,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)


class HightouchSchemaHandler:
    def __init__(
        self,
        report: HightouchSourceReport,
        graph: Optional[DataHubGraph],
        urn_builder: HightouchUrnBuilder,
        schema_metadata_cache: Optional[SchemaMetadataCache] = None,
    ) -> None:
        self.report = report
        self.graph = graph
        self.urn_builder = urn_builder
        # Shared, run-scoped cache injected by the source so preload, model, and
        # lineage resolution reuse the same fetched schema metadata. Defaults to a
        # private cache when constructed standalone (e.g. in unit tests).
        self._schema_metadata_cache: SchemaMetadataCache = (
            schema_metadata_cache if schema_metadata_cache is not None else {}
        )

    def resolve_schema(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        referenced_columns: Optional[List[str]] = None,
    ) -> Optional[List[HightouchSchemaField]]:
        # The API returns no column list for a model, so the schema comes from the
        # table the model reads, as ingested into DataHub, or failing that from the
        # columns its syncs reference.
        schema_fields: Optional[List[HightouchSchemaField]] = None
        if source and self.graph:
            schema_fields = self._fetch_schema_from_datahub(model=model, source=source)

        if not schema_fields and referenced_columns:
            schema_fields = self._schema_from_referenced_columns(
                referenced_columns=referenced_columns, primary_key=model.primary_key
            )

        if schema_fields and model.primary_key:
            schema_fields = self._mark_primary_key_in_schema(
                schema_fields=schema_fields, primary_key=model.primary_key
            )

        return schema_fields

    def _mark_primary_key_in_schema(
        self, schema_fields: List[HightouchSchemaField], primary_key: str
    ) -> List[HightouchSchemaField]:
        updated_fields = []
        for field in schema_fields:
            if field.name.upper() == primary_key.upper():
                updated_fields.append(
                    HightouchSchemaField(
                        name=field.name,
                        type=field.type,
                        description=field.description,
                        is_primary_key=True,
                    )
                )
            else:
                updated_fields.append(field)
        return updated_fields

    def _schema_from_referenced_columns(
        self, referenced_columns: List[str], primary_key: Optional[str] = None
    ) -> Optional[List[HightouchSchemaField]]:
        if not referenced_columns:
            return None

        schema_fields: List[HightouchSchemaField] = []
        for column_name in referenced_columns:
            is_pk = primary_key and column_name.upper() == primary_key.upper()

            schema_fields.append(
                HightouchSchemaField(
                    name=column_name,
                    type="STRING",
                    description=None,
                    is_primary_key=is_pk or False,
                )
            )

        self.report.schemas_from_referenced_columns += 1

        return schema_fields

    def _fetch_schema_from_datahub(
        self, model: HightouchModel, source: HightouchSourceConnection
    ) -> Optional[List[HightouchSchemaField]]:
        if not self.graph:
            return None

        try:
            upstream_urn = self.urn_builder.source_table_urn(model, source)
            if not upstream_urn:
                return None

            schema_metadata = fetch_schema_metadata_cached(
                self.graph, self._schema_metadata_cache, str(upstream_urn)
            )

            if not schema_metadata or not schema_metadata.fields:
                self.report.report_model_schema_datahub_not_found(model.slug)
                return None

            schema_fields = []
            for field in schema_metadata.fields:
                schema_fields.append(
                    HightouchSchemaField(
                        name=field.fieldPath,
                        type=field.nativeDataType or "UNKNOWN",
                        description=field.description if field.description else None,
                    )
                )

            self.report.report_model_schema_from_datahub()

            return schema_fields

        except Exception as e:
            reraise_if_programming_error(e, f"fetching schema of model {model.slug}")
            self.report.report_datahub_lookup_failure(
                "model schema from the upstream table", f"model_slug: {model.slug}", e
            )
            return None

    def fetch_and_register_schema(
        self,
        urn: str,
        aggregator: SqlParsingAggregator,
        registered_urns: Set[str],
    ) -> bool:
        if urn in registered_urns:
            return False

        if not self.graph:
            return False

        try:
            schema_metadata = fetch_schema_metadata_cached(
                self.graph, self._schema_metadata_cache, urn
            )
            if schema_metadata and schema_metadata.fields:
                aggregator.register_schema(urn, schema_metadata)
                registered_urns.add(urn)
                return True
        except Exception as e:
            reraise_if_programming_error(e, f"preloading schema for {urn}")
            self.report.report_datahub_lookup_failure(
                "schema preload for SQL parsing", urn, e
            )

        return False

    def preload_model_schemas(
        self,
        model: HightouchModel,
        source: HightouchSourceConnection,
        aggregator: SqlParsingAggregator,
        registered_urns: Set[str],
        extract_table_urns_fn: ExtractTableUrns,
    ) -> None:
        upstream_urn = self.urn_builder.source_table_urn(model, source)
        if upstream_urn:
            self.fetch_and_register_schema(upstream_urn, aggregator, registered_urns)

        if model.raw_sql and model.query_type == QUERY_TYPE_RAW_SQL:
            sql_table_urns = extract_table_urns_fn(model, source)
            for table_urn in sql_table_urns:
                self.fetch_and_register_schema(table_urn, aggregator, registered_urns)

    def preload_sync_schemas(
        self,
        sync: HightouchSync,
        registered_urns: Set[str],
        get_model: GetModel,
        get_source: GetSource,
        get_destination: GetDestination,
        get_platform_for_source: GetPlatformForSource,
        get_aggregator_for_platform: GetAggregatorForPlatform,
        get_outlet_urn_for_sync: GetOutletUrnForSync,
    ) -> None:
        model = get_model(sync.model_id)
        if not model:
            return

        source = get_source(model.source_id)
        if not source:
            return

        source_platform = get_platform_for_source(source)
        if not source_platform.platform:
            return

        aggregator = get_aggregator_for_platform(source_platform)
        if not aggregator:
            return

        model_urn = str(self.urn_builder.make_model_urn(model, source))
        self.fetch_and_register_schema(model_urn, aggregator, registered_urns)

        destination = get_destination(sync.destination_id)
        if not destination:
            return

        outlet_urn = get_outlet_urn_for_sync(sync, destination)
        if outlet_urn:
            self.fetch_and_register_schema(str(outlet_urn), aggregator, registered_urns)

    def preload_schemas_for_sql_parsing(
        self,
        models: List[HightouchModel],
        syncs: List[HightouchSync],
        registered_urns: Set[str],
        get_source: GetSource,
        get_model: GetModel,
        get_destination: GetDestination,
        get_platform_for_source: GetPlatformForSource,
        get_aggregator_for_platform: GetAggregatorForPlatform,
        extract_table_urns_fn: ExtractTableUrns,
        get_outlet_urn_for_sync: GetOutletUrnForSync,
    ) -> None:
        if not self.graph:
            return

        registered_urns.clear()

        # Preloading only improves SQL parsing and sibling links, so an API or
        # DataHub error here skips that one model or sync instead of the run.
        for model in models:
            try:
                source = get_source(model.source_id)
                if not source:
                    continue

                source_platform = get_platform_for_source(source)
                if not source_platform.platform:
                    continue

                aggregator = get_aggregator_for_platform(source_platform)
                if not aggregator:
                    continue

                self.preload_model_schemas(
                    model, source, aggregator, registered_urns, extract_table_urns_fn
                )
            except Exception as e:
                reraise_if_programming_error(e, f"preloading model {model.slug}")
                self._report_preload_failure(f"model_slug: {model.slug}", e)

        for sync in syncs:
            try:
                self.preload_sync_schemas(
                    sync,
                    registered_urns,
                    get_model,
                    get_source,
                    get_destination,
                    get_platform_for_source,
                    get_aggregator_for_platform,
                    get_outlet_urn_for_sync,
                )
            except Exception as e:
                reraise_if_programming_error(e, f"preloading sync {sync.slug}")
                self._report_preload_failure(f"sync_slug: {sync.slug}", e)

    def _report_preload_failure(self, context: str, exc: Exception) -> None:
        self.report.warning(
            title="Could not preload schemas for SQL parsing",
            message="Reading this entity before ingestion failed, so its SQL lineage "
            "and sibling links may be less complete. It is still ingested.",
            context=context,
            exc=exc,
        )
