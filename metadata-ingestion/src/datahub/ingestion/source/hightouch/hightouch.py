import logging
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union

import requests

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceReport,
    StructuredLogCategory,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.constants import (
    HIGHTOUCH_PLATFORM,
    KNOWN_DESTINATION_PLATFORM_MAPPING,
    KNOWN_SOURCE_PLATFORM_MAPPING,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.hightouch_container import (
    HightouchContainerHandler,
)
from datahub.ingestion.source.hightouch.hightouch_contract import (
    HightouchContractHandler,
)
from datahub.ingestion.source.hightouch.hightouch_lineage import (
    HightouchLineageHandler,
)
from datahub.ingestion.source.hightouch.hightouch_model import HightouchModelHandler
from datahub.ingestion.source.hightouch.hightouch_schema import HightouchSchemaHandler
from datahub.ingestion.source.hightouch.hightouch_sync import HightouchSyncHandler
from datahub.ingestion.source.hightouch.hightouch_utils import (
    SchemaMetadataCache,
    reraise_if_programming_error,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
)
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)

# SQL parsing aggregators must not be shared across sources that resolve to the
# same platform but different instances or environments, otherwise SQL-derived
# lineage is emitted under the wrong dataset URNs. Key the cache by all three.
AggregatorCacheKey = Tuple[str, Optional[str], str]


@platform_name("Hightouch")
@config_class(HightouchSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, showing data flow from source -> model -> sync -> destination",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, emitting column-level lineage from field mappings",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
class HightouchSource(StatefulIngestionSourceBase, TestableSource):
    config: HightouchSourceConfig
    report: HightouchSourceReport
    platform: str = HIGHTOUCH_PLATFORM

    def __init__(self, config: HightouchSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = HightouchSourceReport()
        self.api_client = HightouchAPIClient(self.config.api_config, self.report)

        self.graph: Optional[DataHubGraph] = ctx.graph

        self._sources_cache: Dict[str, HightouchSourceConnection] = {}
        self._models_cache: Dict[str, HightouchModel] = {}
        self._destinations_cache: Dict[str, HightouchDestination] = {}
        self._source_platforms: Dict[str, PlatformDetail] = {}
        self._destination_platforms: Dict[str, PlatformDetail] = {}
        self._registered_urns: Set[str] = set()
        self._model_schema_fields_cache: Dict[str, List[SchemaFieldClass]] = {}
        self._schema_metadata_cache: SchemaMetadataCache = {}

        self._urn_builder = HightouchUrnBuilder(
            config=self.config,
            get_platform_for_source=self._get_platform_for_source,
            get_platform_for_destination=self._get_platform_for_destination,
        )
        self._sql_aggregators: Dict[
            AggregatorCacheKey, Optional[SqlParsingAggregator]
        ] = {}

        self._container_handler = HightouchContainerHandler(config=self.config)

        self._schema_handler = HightouchSchemaHandler(
            report=self.report,
            graph=self.graph,
            urn_builder=self._urn_builder,
            schema_metadata_cache=self._schema_metadata_cache,
        )

        self._lineage_handler = HightouchLineageHandler(
            config=self.config,
            api_client=self.api_client,
            report=self.report,
            urn_builder=self._urn_builder,
            graph=self.graph,
            registered_urns=self._registered_urns,
            schema_metadata_cache=self._schema_metadata_cache,
        )

        self._model_handler = HightouchModelHandler(
            config=self.config,
            report=self.report,
            urn_builder=self._urn_builder,
            schema_handler=self._schema_handler,
            lineage_handler=self._lineage_handler,
            container_handler=self._container_handler,
            get_platform_for_source=self._get_platform_for_source,
            get_aggregator_for_platform=self._get_aggregator_for_platform,
            model_schema_fields_cache=self._model_schema_fields_cache,
        )

        self._sync_handler = HightouchSyncHandler(
            config=self.config,
            report=self.report,
            api_client=self.api_client,
            urn_builder=self._urn_builder,
            lineage_handler=self._lineage_handler,
            container_handler=self._container_handler,
            model_schema_fields_cache=self._model_schema_fields_cache,
            get_model=self._get_model,
            get_source=self._get_source,
            get_destination=self._get_destination,
        )

        self._contract_handler = HightouchContractHandler(
            config=self.config,
            report=self.report,
        )

    def _get_aggregator_for_platform(
        self, source_platform: PlatformDetail
    ) -> Optional[SqlParsingAggregator]:
        platform = source_platform.platform
        if not platform:
            return None

        env = source_platform.env or self.config.env
        cache_key: AggregatorCacheKey = (
            platform,
            source_platform.platform_instance,
            env,
        )

        if cache_key not in self._sql_aggregators:
            try:
                self._sql_aggregators[cache_key] = SqlParsingAggregator(
                    platform=platform,
                    platform_instance=source_platform.platform_instance,
                    env=env,
                    graph=self.graph,
                    eager_graph_load=False,
                    generate_lineage=True,
                    generate_queries=False,
                    generate_usage_statistics=False,
                    generate_operations=False,
                )
            except Exception as e:
                reraise_if_programming_error(
                    e, f"creating SQL aggregator for platform {platform}"
                )
                self.report.report_lineage_resolution_failure(f"platform: {platform}")
                self.report.warning(
                    title="Could not create SQL parsing aggregator",
                    message="SQL parsing will be disabled for this platform, so "
                    "SQL-derived lineage will be missing; basic lineage is unaffected.",
                    context=f"platform: {platform}",
                    exc=e,
                )
                self._sql_aggregators[cache_key] = None
                return None

        return self._sql_aggregators[cache_key]

    def _get_source(self, source_id: str) -> Optional[HightouchSourceConnection]:
        if source_id not in self._sources_cache:
            self.report.report_api_call()
            source = self.api_client.get_source_by_id(source_id)
            if source:
                self._sources_cache[source_id] = source
        return self._sources_cache.get(source_id)

    def _get_model(self, model_id: str) -> Optional[HightouchModel]:
        if model_id not in self._models_cache:
            self.report.report_api_call()
            model = self.api_client.get_model_by_id(model_id)
            if model:
                self._models_cache[model_id] = model
        return self._models_cache.get(model_id)

    def _get_destination(self, destination_id: str) -> Optional[HightouchDestination]:
        if destination_id not in self._destinations_cache:
            self.report.report_api_call()
            destination = self.api_client.get_destination_by_id(destination_id)
            if destination:
                self._destinations_cache[destination_id] = destination
        return self._destinations_cache.get(destination_id)

    def _get_platform_for_source(
        self, source: HightouchSourceConnection
    ) -> PlatformDetail:
        # Resolved once per source, on a copy: the recipe's own PlatformDetail must
        # not be filled in, and the unknown-platform message is reported once.
        if source.id in self._source_platforms:
            return self._source_platforms[source.id]
        configured = self.config.sources_to_platform_instance.get(source.id)
        details = configured.model_copy() if configured else PlatformDetail()

        if details.platform is None:
            if source.type.lower() in KNOWN_SOURCE_PLATFORM_MAPPING:
                details.platform = KNOWN_SOURCE_PLATFORM_MAPPING[source.type.lower()]
            else:
                self.report.info(
                    title="Unknown source platform type",
                    message="The source type is not in the known platform mapping, so "
                    "it is used as the platform name. Map the source id in "
                    "sources_to_platform_instance to set the platform.",
                    context=f"source_name: {source.name} (source_id: {source.id}, type: {source.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                details.platform = source.type.lower()

        if details.env is None:
            details.env = self.config.env

        if details.database is None and source.configuration:
            details.database = source.configuration.get("database")

        self._source_platforms[source.id] = details
        return details

    def _get_platform_for_destination(
        self, destination: HightouchDestination
    ) -> PlatformDetail:
        if destination.id in self._destination_platforms:
            return self._destination_platforms[destination.id]
        configured = self.config.destination_to_platform_instance.get(destination.id)
        details = configured.model_copy() if configured else PlatformDetail()

        if details.platform is None:
            if destination.type.lower() in KNOWN_DESTINATION_PLATFORM_MAPPING:
                details.platform = KNOWN_DESTINATION_PLATFORM_MAPPING[
                    destination.type.lower()
                ]
            else:
                self.report.info(
                    title="Unknown destination platform type",
                    message="The destination type is not in the known platform "
                    "mapping, so it is used as the platform name. Map the destination "
                    "id in destination_to_platform_instance to set the platform.",
                    context=f"destination_name: {destination.name} (destination_id: {destination.id}, type: {destination.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                details.platform = destination.type.lower()

        if details.env is None:
            details.env = self.config.env

        self._destination_platforms[destination.id] = details
        return details

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = HightouchSourceConfig.parse_obj_allow_extras(config_dict)
            HightouchAPIClient(config.api_config).check_access()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            reason = (
                f"Hightouch rejected the API key (HTTP {status}). Check that the key "
                "is valid and belongs to the workspace to ingest."
                if status in (401, 403)
                else f"The Hightouch API returned an error: {e}"
            )
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=reason
            )
        except requests.exceptions.RequestException as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Could not reach the Hightouch API at the configured "
                f"base_url: {e}",
            )
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"Invalid configuration: {e}"
            )
        return test_report

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        yield from self._container_handler.emit_models_container()
        yield from self._container_handler.emit_syncs_container()

        self.report.report_api_call()
        syncs = self.api_client.get_syncs()

        filtered_syncs: List[HightouchSync] = []
        for sync in syncs:
            if self.config.sync_patterns.allowed(sync.slug):
                filtered_syncs.append(sync)
            else:
                self.report.report_syncs_dropped(sync.slug)

        all_models: List[HightouchModel] = []
        if self.config.emit_models_as_datasets:
            self.report.report_api_call()
            all_models = self.api_client.get_models()
            # The bulk list already carries every field, so syncs read their model
            # from here instead of fetching it again.
            self._models_cache.update({model.id: model for model in all_models})

        if self.graph and (filtered_syncs or all_models):
            self._schema_handler.preload_schemas_for_sql_parsing(
                models=[
                    model
                    for model in all_models
                    if self.config.model_patterns.allowed(model.name)
                ],
                syncs=filtered_syncs,
                registered_urns=self._registered_urns,
                get_source=self._get_source,
                get_model=self._get_model,
                get_destination=self._get_destination,
                get_platform_for_source=self._get_platform_for_source,
                get_aggregator_for_platform=self._get_aggregator_for_platform,
                extract_table_urns_fn=self._model_handler.extract_table_urns_from_sql,
                get_outlet_urn_for_sync=self._sync_handler.get_outlet_urn_for_sync,
            )

        if self.config.emit_models_as_datasets:
            yield from self._get_models_workunits(all_models, filtered_syncs)

        for sync in filtered_syncs:
            try:
                yield from self._sync_handler.get_sync_workunits(sync)
            except Exception as e:
                # A failure, not a warning: it keeps stale-entity removal from
                # soft-deleting this sync, which still exists in Hightouch.
                self.report.failure(
                    title="Failed to process sync",
                    message="The sync could not be ingested.",
                    context=f"sync_slug: {sync.slug} (sync_id: {sync.id})",
                    exc=e,
                )

        if self.config.include_contracts:
            self.report.report_api_call()
            contracts = self.api_client.get_contracts()
            yield from self._contract_handler.get_contract_workunits(
                contracts=contracts
            )

        active_aggregators = {
            cache_key: aggregator
            for cache_key, aggregator in self._sql_aggregators.items()
            if aggregator is not None
        }
        for cache_key, aggregator in active_aggregators.items():
            platform = cache_key[0]
            try:
                for mcp in aggregator.gen_metadata():
                    yield mcp.as_workunit()
            except Exception as e:
                self.report.report_lineage_resolution_failure(f"platform: {platform}")
                self.report.warning(
                    title="Could not generate SQL-parsed lineage",
                    message="The SQL parsing aggregator failed to emit metadata for "
                    "this platform, so SQL-derived lineage will be missing.",
                    context=f"platform: {platform}",
                    exc=e,
                )
            finally:
                aggregator.close()

    def _get_models_workunits(
        self, all_models: List[HightouchModel], syncs: List[HightouchSync]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Emit each model once, before the syncs that read it.

        A model's schema may come only from the columns its syncs reference, so it
        is built from the union of every sync's columns rather than per sync.
        """
        columns_by_model: Dict[str, List[str]] = {}
        for sync in syncs:
            columns = columns_by_model.setdefault(sync.model_id, [])
            for column in sync.referenced_columns or []:
                if column not in columns:
                    columns.append(column)

        model_ids = [model.id for model in all_models]
        listed = set(model_ids)
        model_ids += [m for m in columns_by_model if m not in listed]

        for model_id in model_ids:
            try:
                model = self._get_model(model_id)
                if not model:
                    continue
                if not self.config.model_patterns.allowed(model.name):
                    self.report.report_models_dropped(model.name)
                    continue
                self.report.report_models_scanned()
                source = self._get_source(model.source_id)
                yield from self._model_handler.emit_model(
                    model, source, referenced_columns=columns_by_model.get(model_id)
                )
            except Exception as e:
                # A failure for the same reason as a failed sync.
                self.report.failure(
                    title="Failed to process model",
                    message="The model could not be ingested.",
                    context=f"model_id: {model_id}",
                    exc=e,
                )

    def get_report(self) -> SourceReport:
        return self.report
