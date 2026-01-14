import json
import logging
from typing import Dict, Iterable, List, Optional, Tuple, Union

from datahub.api.entities.datajob import DataJob as DataJobV1
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    MetadataWorkUnitProcessor,
    SourceReport,
    StructuredLogCategory,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hightouch.config import (
    Constant,
    HightouchSourceConfig,
    HightouchSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.models import (
    HightouchContract,
    HightouchContractRun,
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
    HightouchSyncRun,
    HightouchUser,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import AssertionUrn, DataFlowUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)

CORPUSER_DATAHUB = "urn:li:corpuser:datahub"
HIGHTOUCH_PLATFORM = "hightouch"

# Mapping of Hightouch source types to DataHub platform names
# Reference: https://hightouch.com/docs/getting-started/concepts/#sources
KNOWN_SOURCE_PLATFORM_MAPPING = {
    # Data Warehouses
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "synapse": "mssql",
    "athena": "athena",
    # Databases
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mssql": "mssql",
    "sql_server": "mssql",
    "azure_sql": "mssql",
    "oracle": "oracle",
    "mongodb": "mongodb",
    "dynamodb": "dynamodb",
    # BI & Analytics Tools
    "looker": "looker",
    "tableau": "tableau",
    "metabase": "metabase",
    "mode": "mode",
    "sigma": "sigma",
    # Cloud Storage
    "s3": "s3",
    "gcs": "gcs",
    "azure_blob": "abs",
    "azure_blob_storage": "abs",
    "azure_storage": "abs",
    "adls": "abs",
    "adls_gen1": "abs",
    "adls_gen2": "abs",
    "azure_data_lake": "abs",
    "azure_data_lake_storage": "abs",
    # SaaS & Other
    "salesforce": "salesforce",
    "google_sheets": "google-sheets",
    "airtable": "airtable",
    "google_analytics": "google-analytics",
    "hubspot": "hubspot",
}

# Mapping of Hightouch destination types to DataHub platform names
# Reference: https://hightouch.com/docs/destinations/overview/
KNOWN_DESTINATION_PLATFORM_MAPPING = {
    # Data Warehouses & Databases
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "mssql": "mssql",
    # Cloud Storage
    "s3": "s3",
    "gcs": "gcs",
    "azure_blob": "abs",
    "azure_blob_storage": "abs",
    "azure_storage": "abs",
    "adls": "abs",
    "adls_gen1": "abs",
    "adls_gen2": "abs",
    "azure_data_lake": "abs",
    "azure_data_lake_storage": "abs",
    # CRM & Sales
    "salesforce": "salesforce",
    "hubspot": "hubspot",
    "zendesk": "zendesk",
    "pipedrive": "pipedrive",
    "outreach": "outreach",
    "salesloft": "salesloft",
    # Marketing Automation
    "braze": "braze",
    "iterable": "iterable",
    "customer_io": "customerio",
    "marketo": "marketo",
    "klaviyo": "klaviyo",
    "mailchimp": "mailchimp",
    "activecampaign": "activecampaign",
    "eloqua": "eloqua",
    "sendgrid": "sendgrid",
    # Analytics & Product
    "segment": "segment",
    "mixpanel": "mixpanel",
    "amplitude": "amplitude",
    "google_analytics": "google-analytics",
    "heap": "heap",
    "pendo": "pendo",
    "intercom": "intercom",
    # Advertising
    "facebook_ads": "facebook",
    "google_ads": "google-ads",
    "linkedin_ads": "linkedin",
    "snapchat_ads": "snapchat",
    "tiktok_ads": "tiktok",
    "pinterest_ads": "pinterest",
    "twitter_ads": "twitter",
    # Support & Customer Success (additional)
    "freshdesk": "freshdesk",
    "kustomer": "kustomer",
    # Collaboration & Productivity
    "google_sheets": "google-sheets",
    "airtable": "airtable",
    "slack": "slack",
    # Payment & Finance
    "stripe": "stripe",
    "chargebee": "chargebee",
    # Other
    "webhook": "http",
    "http": "http",
}


@platform_name("Hightouch")
@config_class(HightouchSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, showing data flow from source -> model -> sync -> destination",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default via configuration `include_column_lineage`",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
class HightouchSource(StatefulIngestionSourceBase):
    """
    This plugin extracts Hightouch reverse ETL metadata including sources,
    models, syncs, destinations, and sync run history.
    """

    config: HightouchSourceConfig
    report: HightouchSourceReport
    platform: str = "hightouch"

    def __init__(self, config: HightouchSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = HightouchSourceReport()
        self.api_client = HightouchAPIClient(self.config.api_config)

        self._sources_cache: Dict[str, HightouchSourceConnection] = {}
        self._models_cache: Dict[str, HightouchModel] = {}
        self._destinations_cache: Dict[str, HightouchDestination] = {}
        self._users_cache: Dict[str, HightouchUser] = {}

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

    def _get_user(self, user_id: str) -> Optional[HightouchUser]:
        if user_id not in self._users_cache:
            self.report.report_api_call()
            user = self.api_client.get_user_by_id(user_id)
            if user:
                self._users_cache[user_id] = user
        return self._users_cache.get(user_id)

    def _get_platform_for_source(
        self, source: HightouchSourceConnection
    ) -> PlatformDetail:
        """Get platform details for a source with fallback logic"""
        source_details = self.config.sources_to_platform_instance.get(
            source.id, PlatformDetail()
        )

        if source_details.platform is None:
            if source.type.lower() in KNOWN_SOURCE_PLATFORM_MAPPING:
                source_details.platform = KNOWN_SOURCE_PLATFORM_MAPPING[
                    source.type.lower()
                ]
            else:
                self.report.info(
                    title="Unknown source platform type",
                    message=f"Source type '{source.type}' is not in the known platform mapping. "
                    f"Using source type as platform name. Consider adding a mapping in "
                    f"sources_to_platform_instance config for source_id '{source.id}'.",
                    context=f"source_name: {source.name} (source_id: {source.id}, type: {source.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                source_details.platform = source.type.lower()

        if source_details.env is None:
            source_details.env = self.config.env

        return source_details

    def _get_platform_for_destination(
        self, destination: HightouchDestination
    ) -> PlatformDetail:
        """Get platform details for a destination with fallback logic"""
        destination_details = self.config.destinations_to_platform_instance.get(
            destination.id, PlatformDetail()
        )

        if destination_details.platform is None:
            if destination.type.lower() in KNOWN_DESTINATION_PLATFORM_MAPPING:
                destination_details.platform = KNOWN_DESTINATION_PLATFORM_MAPPING[
                    destination.type.lower()
                ]
            else:
                self.report.info(
                    title="Unknown destination platform type",
                    message=f"Destination type '{destination.type}' is not in the known platform mapping. "
                    f"Using destination type as platform name. Consider adding a mapping in "
                    f"destinations_to_platform_instance config for destination_id '{destination.id}'.",
                    context=f"destination_name: {destination.name} (destination_id: {destination.id}, type: {destination.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                destination_details.platform = destination.type.lower()

        if destination_details.env is None:
            destination_details.env = self.config.env

        return destination_details

    def _parse_model_sql_lineage(
        self,
        model: HightouchModel,
        source: HightouchSourceConnection,
    ) -> Optional[UpstreamLineageClass]:
        """Parse SQL from model to extract upstream table lineage."""
        if not model.raw_sql:
            return None

        self.report.sql_parsing_attempts += 1

        source_platform = self._get_platform_for_source(source)
        if not source_platform.platform:
            logger.debug(
                f"Skipping SQL parsing for model {model.id}: unknown source platform"
            )
            return None

        try:
            sql_result: SqlParsingResult = create_lineage_sql_parsed_result(
                query=model.raw_sql,
                default_db=source_platform.database,
                platform=source_platform.platform,
                platform_instance=source_platform.platform_instance,
                env=source_platform.env,
                graph=None,
                schema_aware=False,
            )

            if sql_result.debug_info.error:
                logger.debug(
                    f"Failed to parse SQL for model {model.id}: {sql_result.debug_info.error}"
                )
                self.report.sql_parsing_failures += 1
                self.report.warning(
                    title="SQL parsing failed for model",
                    message=f"Could not extract table lineage from model '{model.name}'",
                    context=f"model_id: {model.id}, error: {sql_result.debug_info.error}",
                )
                return None

            if not sql_result.in_tables:
                logger.debug(f"No upstream tables found for model {model.id}")
                self.report.sql_parsing_successes += 1
                return None

            upstreams = []
            for table_urn in sql_result.in_tables:
                upstreams.append(
                    UpstreamClass(
                        dataset=str(table_urn),
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                )

            self.report.sql_parsing_successes += 1
            logger.info(
                f"Extracted {len(upstreams)} upstream tables for model {model.id}"
            )
            return UpstreamLineageClass(upstreams=upstreams)

        except Exception as e:
            logger.debug(f"Error parsing SQL for model {model.id}: {e}")
            self.report.sql_parsing_failures += 1
            self.report.warning(
                title="SQL parsing error",
                message=f"Unexpected error while parsing SQL for model '{model.name}'",
                context=f"model_id: {model.id}",
                exc=e,
            )
            return None

    def _parse_model_schema(
        self, model: HightouchModel
    ) -> Optional[List[Tuple[str, str, Optional[str]]]]:
        """Parse schema from Hightouch model and convert to DataHub format.

        Handles multiple possible schema formats from the Hightouch API:
        - JSON string that needs parsing
        - List of column dictionaries
        - Dict with 'columns' or 'fields' key
        - Various field name variations (name/fieldName, type/dataType/data_type)

        Returns:
            List of tuples (field_name, field_type, description) or None if no schema available
        """
        if not model.query_schema:
            return None

        try:
            schema_data = model.query_schema

            if isinstance(schema_data, str):
                logger.debug(f"Model {model.id}: Parsing query_schema from JSON string")
                try:
                    schema_data = json.loads(schema_data)
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"Model {model.id}: query_schema is a string but not valid JSON: {e}"
                    )
                    self.report.report_model_schemas_skipped("invalid_json")
                    return None

            columns = None
            if isinstance(schema_data, list):
                columns = schema_data
                logger.debug(
                    f"Model {model.id}: Schema is a direct list with {len(columns)} columns"
                )
            elif isinstance(schema_data, dict):
                columns = (
                    schema_data.get("columns")
                    or schema_data.get("fields")
                    or schema_data.get("schema")
                    or schema_data.get("properties")
                )
                if columns:
                    logger.debug(
                        f"Model {model.id}: Extracted {len(columns) if isinstance(columns, list) else '?'} "
                        f"columns from dict"
                    )
                else:
                    logger.debug(
                        f"Model {model.id}: Schema dict keys: {list(schema_data.keys())}"
                    )
            else:
                logger.warning(
                    f"Model {model.id}: Unexpected query_schema type: {type(schema_data).__name__}"
                )
                return None

            if not columns:
                logger.debug(f"Model {model.id}: No columns found in schema")
                return None

            if not isinstance(columns, list):
                logger.warning(
                    f"Model {model.id}: Columns is not a list: {type(columns).__name__}"
                )
                return None

            schema_fields = []
            for idx, col in enumerate(columns):
                if not isinstance(col, dict):
                    logger.debug(
                        f"Model {model.id}: Skipping non-dict column at index {idx}"
                    )
                    continue

                name = (
                    col.get("name")
                    or col.get("fieldName")
                    or col.get("field_name")
                    or col.get("columnName")
                    or col.get("column_name")
                )

                data_type = (
                    col.get("type")
                    or col.get("dataType")
                    or col.get("data_type")
                    or col.get("fieldType")
                    or col.get("field_type")
                    or col.get("columnType")
                    or col.get("column_type")
                )

                description = (
                    col.get("description")
                    or col.get("comment")
                    or col.get("doc")
                    or None
                )

                if name and data_type:
                    schema_fields.append((str(name), str(data_type), description))
                else:
                    logger.debug(
                        f"Model {model.id}: Skipping incomplete column at index {idx} "
                        f"(name={name}, type={data_type})"
                    )

            if schema_fields:
                logger.info(
                    f"Model {model.id} ({model.name}): Successfully parsed {len(schema_fields)} schema fields"
                )
                self.report.report_model_schemas_emitted()
                return schema_fields
            else:
                logger.debug(f"Model {model.id}: No valid schema fields found")
                self.report.report_model_schemas_skipped("no_valid_fields")
                return None

        except Exception as e:
            logger.warning(
                f"Model {model.id}: Unexpected error parsing schema: {e}",
                exc_info=True,
            )
            self.report.report_model_schemas_skipped(f"parse_error: {str(e)}")
            return None

    def _generate_model_dataset(
        self, model: HightouchModel, source: Optional[HightouchSourceConnection]
    ) -> Dataset:
        custom_properties = {
            "model_id": model.id,
            "query_type": model.query_type,
            "is_schema": str(model.is_schema),
        }

        if model.primary_key:
            custom_properties["primary_key"] = model.primary_key

        if source:
            custom_properties["source_id"] = source.id
            custom_properties["source_name"] = source.name
            custom_properties["source_type"] = source.type

        if model.tags:
            for key, value in model.tags.items():
                custom_properties[f"tag_{key}"] = value

        # Include raw SQL in custom properties (truncate if very long)
        if model.raw_sql:
            sql_truncated = (
                model.raw_sql[:2000] if len(model.raw_sql) > 2000 else model.raw_sql
            )
            custom_properties["raw_sql"] = sql_truncated
            if len(model.raw_sql) > 2000:
                custom_properties["raw_sql_truncated"] = "true"
                custom_properties["raw_sql_length"] = str(len(model.raw_sql))

        dataset = Dataset(
            name=model.slug,
            platform=HIGHTOUCH_PLATFORM,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=model.name,
            description=model.description,
            created=model.created_at,
            last_modified=model.updated_at,
            custom_properties=custom_properties,
        )

        schema_fields = self._parse_model_schema(model)
        if schema_fields:
            # Convert to format expected by _set_schema (no None descriptions)
            formatted_fields = [
                (name, data_type)
                if description is None
                else (name, data_type, description)
                for name, data_type, description in schema_fields
            ]
            dataset._set_schema(formatted_fields)

        # Add upstream lineage based on model type
        if source:
            if (
                self.config.parse_model_sql
                and model.raw_sql
                and model.query_type == "raw_sql"
            ):
                # For raw_sql models, parse SQL to extract upstream tables
                upstream_lineage = self._parse_model_sql_lineage(model, source)
                if upstream_lineage:
                    dataset.set_upstreams(upstream_lineage)
                    custom_properties["sql_parsed"] = "true"
                    custom_properties["upstream_tables_count"] = str(
                        len(upstream_lineage.upstreams)
                    )
                else:
                    custom_properties["sql_parsed"] = "true"
                    custom_properties["upstream_tables_count"] = "0"
            elif model.query_type == "table" and model.name:
                # For table models, create direct lineage to the source table
                source_details = self._get_platform_for_source(source)
                if source_details.platform:
                    table_name = model.name

                    # Add database/schema prefix if available
                    if source.configuration:
                        database = source.configuration.get("database", "")
                        schema = source.configuration.get("schema", "")

                        if source_details.include_schema_in_urn and schema:
                            table_name = f"{database}.{schema}.{table_name}"
                        elif database:
                            table_name = f"{database}.{table_name}"

                    upstream_urn = DatasetUrn.create_from_ids(
                        platform_id=source_details.platform,
                        table_name=table_name,
                        env=source_details.env,
                        platform_instance=source_details.platform_instance,
                    )
                    dataset.set_upstreams([upstream_urn])
                    custom_properties["table_lineage"] = "true"
                    custom_properties["upstream_table"] = table_name

        # Update custom properties after adding lineage info
        dataset.set_custom_properties(custom_properties)

        return dataset

    def _generate_dataflow_from_sync(self, sync: HightouchSync) -> DataFlow:
        return DataFlow(
            platform=Constant.ORCHESTRATOR,
            name=sync.id,
            env=self.config.env,
            display_name=sync.slug,
            platform_instance=self.config.platform_instance,
        )

    def _get_inlet_urn_for_model(
        self, model: HightouchModel, sync: HightouchSync
    ) -> Union[str, DatasetUrn, None]:
        """Get the inlet URN for a model, either as Hightouch dataset or source platform dataset"""
        if self.config.emit_models_as_datasets:
            return DatasetUrn.create_from_ids(
                platform_id=HIGHTOUCH_PLATFORM,
                table_name=model.slug,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )

        source = self._get_source(model.source_id)
        if not source:
            self.report.warning(
                title="Failed to get source for model",
                message="Could not retrieve source information for lineage creation.",
                context=f"sync_slug: {sync.slug} (model_id: {model.source_id})",
            )
            return None

        source_details = self._get_platform_for_source(source)
        if not source_details.platform:
            return None

        model_table_name = model.slug
        if source_details.database:
            model_table_name = f"{source_details.database.lower()}.{model_table_name}"

        return DatasetUrn.create_from_ids(
            platform_id=source_details.platform,
            table_name=model_table_name,
            env=source_details.env,
            platform_instance=source_details.platform_instance,
        )

    def _get_outlet_urn_for_sync(
        self, sync: HightouchSync, destination: HightouchDestination
    ) -> Union[str, DatasetUrn, None]:
        """Get the outlet URN for a sync based on destination configuration"""
        dest_details = self._get_platform_for_destination(destination)
        if not dest_details.platform:
            return None

        dest_table = None
        if sync.configuration:
            # Try multiple common configuration keys for destination table name
            for key in [
                "destinationTable",
                "object",
                "tableName",
                "table",
                "objectName",
            ]:
                dest_table = sync.configuration.get(key)
                if dest_table:
                    break

        if not dest_table:
            # Last resort: use sync name with a prefix to distinguish from job name
            dest_table = f"{sync.slug}_destination"
            logger.warning(
                f"Could not find destination table name in sync configuration for sync {sync.slug} (id: {sync.id}). "
                f"Using fallback name: {dest_table}"
            )

        dest_table_name = dest_table
        if dest_details.database:
            dest_table_name = f"{dest_details.database.lower()}.{dest_table_name}"

        return DatasetUrn.create_from_ids(
            platform_id=dest_details.platform,
            table_name=dest_table_name,
            env=dest_details.env,
            platform_instance=dest_details.platform_instance,
        )

    def _generate_datajob_from_sync(self, sync: HightouchSync) -> DataJob:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=sync.id,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        datajob = DataJob(
            name=sync.id,
            flow_urn=dataflow_urn,
            platform_instance=self.config.platform_instance,
            display_name=sync.slug,
        )

        model = self._get_model(sync.model_id)
        destination = self._get_destination(sync.destination_id)

        inlets: List[Union[str, DatasetUrn]] = []
        if model:
            inlet_urn = self._get_inlet_urn_for_model(model, sync)
            if inlet_urn:
                inlets.append(inlet_urn)

        datajob.set_inlets(inlets)

        fine_grained_lineages = []
        outlet_urn = None

        if destination:
            outlet_urn = self._get_outlet_urn_for_sync(sync, destination)

        if self.config.include_column_lineage and model and destination and outlet_urn:
            field_mappings = self.api_client.extract_field_mappings(sync)
            inlet_urn = inlets[0] if inlets else None

            if field_mappings and inlet_urn:
                for mapping in field_mappings:
                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                make_schema_field_urn(
                                    str(inlet_urn), mapping.source_field
                                )
                            ],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            downstreams=[
                                make_schema_field_urn(
                                    str(outlet_urn), mapping.destination_field
                                )
                            ],
                        )
                    )

                if fine_grained_lineages:
                    datajob.set_fine_grained_lineages(fine_grained_lineages)

        if outlet_urn:
            datajob.set_outlets([outlet_urn])

        custom_properties: Dict[str, str] = {
            "sync_id": sync.id,
            "model_id": sync.model_id,
            "destination_id": sync.destination_id,
            "disabled": str(sync.disabled),
        }

        if sync.schedule:
            custom_properties["schedule"] = str(sync.schedule)

        if model:
            custom_properties["model_name"] = model.name
            custom_properties["model_slug"] = model.slug
            custom_properties["query_type"] = model.query_type
            if model.description:
                custom_properties["model_description"] = model.description

        if destination:
            custom_properties["destination_name"] = destination.name
            custom_properties["destination_type"] = destination.type

        datajob.set_custom_properties(custom_properties)

        return datajob

    def _generate_dpi_from_sync_run(
        self, sync_run: HightouchSyncRun, datajob: DataJob, sync_id: str
    ) -> DataProcessInstance:
        datajob_v1 = DataJobV1(
            id=datajob.name,
            flow_urn=datajob.flow_urn,
            platform_instance=self.config.platform_instance,
            name=datajob.name,
            inlets=datajob.inlets,
            outlets=datajob.outlets,
        )

        dpi = DataProcessInstance.from_datajob(
            datajob=datajob_v1,
            id=sync_run.id,
            clone_inlets=True,
            clone_outlets=True,
        )

        custom_props = {
            "sync_run_id": sync_run.id,
            "sync_id": sync_id,
            "status": sync_run.status,
        }
        if sync_run.planned_rows:
            custom_props["planned_rows_added"] = str(
                sync_run.planned_rows.get("added", 0)
            )
            custom_props["planned_rows_changed"] = str(
                sync_run.planned_rows.get("changed", 0)
            )
            custom_props["planned_rows_removed"] = str(
                sync_run.planned_rows.get("removed", 0)
            )
            custom_props["planned_rows_total"] = str(
                sum(sync_run.planned_rows.values())
            )

        if sync_run.successful_rows:
            custom_props["successful_rows_added"] = str(
                sync_run.successful_rows.get("added", 0)
            )
            custom_props["successful_rows_changed"] = str(
                sync_run.successful_rows.get("changed", 0)
            )
            custom_props["successful_rows_removed"] = str(
                sync_run.successful_rows.get("removed", 0)
            )
            custom_props["successful_rows_total"] = str(
                sum(sync_run.successful_rows.values())
            )

        if sync_run.failed_rows:
            failed_total = sum(sync_run.failed_rows.values())
            custom_props["failed_rows_total"] = str(failed_total)
            custom_props["failed_rows_added"] = str(
                sync_run.failed_rows.get("added", 0)
            )
            custom_props["failed_rows_changed"] = str(
                sync_run.failed_rows.get("changed", 0)
            )
            custom_props["failed_rows_removed"] = str(
                sync_run.failed_rows.get("removed", 0)
            )

        if sync_run.query_size:
            custom_props["query_size_bytes"] = str(sync_run.query_size)

        if sync_run.completion_ratio is not None:
            custom_props["completion_ratio"] = f"{sync_run.completion_ratio * 100:.1f}%"
        if sync_run.error:
            if isinstance(sync_run.error, str):
                custom_props["error_message"] = sync_run.error
            else:
                custom_props["error_message"] = sync_run.error.get(
                    "message", "Unknown error"
                )
                if "code" in sync_run.error:
                    custom_props["error_code"] = sync_run.error["code"]

        dpi.properties.update(custom_props)

        return dpi

    def _get_dpi_workunits(
        self, sync_run: HightouchSyncRun, dpi: DataProcessInstance
    ) -> Iterable[MetadataWorkUnit]:
        status_map = {
            "success": InstanceRunResult.SUCCESS,
            "failed": InstanceRunResult.FAILURE,
            "cancelled": InstanceRunResult.SKIPPED,
            "interrupted": InstanceRunResult.SKIPPED,
            "warning": InstanceRunResult.SUCCESS,
        }

        status = status_map.get(sync_run.status.lower(), InstanceRunResult.SUCCESS)

        start_timestamp_millis = int(sync_run.started_at.timestamp() * 1000)

        for mcp in dpi.generate_mcp(
            created_ts_millis=start_timestamp_millis, materialize_iolets=False
        ):
            yield mcp.as_workunit()

        for mcp in dpi.start_event_mcp(start_timestamp_millis):
            yield mcp.as_workunit()

        end_timestamp_millis = (
            int(sync_run.finished_at.timestamp() * 1000)
            if sync_run.finished_at
            else start_timestamp_millis
        )

        for mcp in dpi.end_event_mcp(
            end_timestamp_millis=end_timestamp_millis,
            result=status,
            result_type=Constant.ORCHESTRATOR,
        ):
            yield mcp.as_workunit()

    def _get_sync_workunits(
        self, sync: HightouchSync
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_syncs_scanned()

        if self.config.emit_models_as_datasets:
            model = self._get_model(sync.model_id)
            if model and self.config.model_patterns.allowed(model.name):
                source = self._get_source(model.source_id)
                model_dataset = self._generate_model_dataset(model, source)
                self.report.report_models_emitted()
                yield model_dataset

        dataflow = self._generate_dataflow_from_sync(sync)
        yield dataflow

        datajob = self._generate_datajob_from_sync(sync)
        yield datajob

        if self.config.include_sync_runs:
            self.report.report_api_call()
            sync_runs = self.api_client.get_sync_runs(
                sync.id, limit=self.config.max_sync_runs_per_sync
            )

            for sync_run in sync_runs:
                self.report.report_sync_runs_scanned()
                dpi = self._generate_dpi_from_sync_run(sync_run, datajob, sync.id)
                yield from self._get_dpi_workunits(sync_run, dpi)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _make_assertion_urn(self, contract_id: str) -> str:
        """Generate assertion URN from contract ID"""
        return AssertionUrn(f"hightouch_contract_{contract_id}").urn()

    def _get_assertion_dataset_urn(self, contract: HightouchContract) -> Optional[str]:
        """Get the dataset URN that this contract validates"""
        if not contract.model_id:
            return None

        model = self._get_model(contract.model_id)
        if not model:
            logger.debug(
                f"Model {contract.model_id} not found for contract {contract.id}"
            )
            return None

        source = self._get_source(model.source_id)
        if not source:
            logger.debug(f"Source {model.source_id} not found for model {model.id}")
            return None

        platform_detail = self.config.sources_to_platform_instance.get(source.id)
        if not platform_detail or not platform_detail.platform:
            logger.debug(
                f"No platform mapping found for source {source.id}. "
                f"Add to sources_to_platform_instance config to enable lineage."
            )
            return None

        platform = platform_detail.platform
        env = platform_detail.env
        database = platform_detail.database or source.configuration.get("database", "")
        schema = source.configuration.get("schema", "")
        table_name = model.name

        if platform_detail.include_schema_in_urn and schema:
            table = f"{database}.{schema}.{table_name}"
        else:
            table = f"{database}.{table_name}" if database else table_name

        return DatasetUrn.create_from_ids(
            platform_id=platform,
            table_name=table,
            env=env,
            platform_instance=platform_detail.platform_instance,
        ).urn()

    def _generate_assertion_from_contract(
        self, contract: HightouchContract
    ) -> Iterable[MetadataWorkUnit]:
        """Generate DataHub assertion from Hightouch contract"""
        assertion_urn = self._make_assertion_urn(contract.id)
        dataset_urn = self._get_assertion_dataset_urn(contract)

        if not dataset_urn:
            logger.warning(
                f"Could not determine dataset for contract {contract.id} ({contract.name}), skipping assertion"
            )
            return

        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.DATA_SCHEMA,
            datasetAssertion=DatasetAssertionInfoClass(
                dataset=dataset_urn,
                scope=DatasetAssertionScopeClass.DATASET_ROWS,
                operator=AssertionStdOperatorClass._NATIVE_,
                nativeType="hightouch_contract",
            ),
            description=contract.description
            or f"Hightouch Event Contract: {contract.name}",
            externalUrl=None,
            customProperties={
                "platform": "hightouch",
                "contract_id": contract.id,
                "contract_name": contract.name,
                "enabled": str(contract.enabled),
                "severity": contract.severity or "unknown",
                "workspace_id": contract.workspace_id,
            },
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=assertion_info,
        ).as_workunit()

        self.report.report_contracts_emitted()

    def _generate_assertion_results_from_contract_runs(
        self, contract: HightouchContract, runs: List[HightouchContractRun]
    ) -> Iterable[MetadataWorkUnit]:
        """Generate assertion run results from contract validation runs"""
        assertion_urn = self._make_assertion_urn(contract.id)

        for run in runs:
            if run.status == "passed":
                result_type = AssertionResultTypeClass.SUCCESS
                native_results = {"status": "passed"}
            elif run.status == "failed":
                result_type = AssertionResultTypeClass.FAILURE
                native_results = {"status": "failed"}
            else:
                result_type = AssertionResultTypeClass.ERROR
                native_results = {"status": run.status}

            if run.total_rows_checked is not None:
                native_results["total_rows_checked"] = str(run.total_rows_checked)
            if run.rows_passed is not None:
                native_results["rows_passed"] = str(run.rows_passed)
            if run.rows_failed is not None:
                native_results["rows_failed"] = str(run.rows_failed)

            if run.error:
                if isinstance(run.error, str):
                    native_results["error"] = run.error
                else:
                    native_results["error"] = str(run.error.get("message", run.error))

            assertion_result = AssertionRunEventClass(
                timestampMillis=int(run.created_at.timestamp() * 1000),
                assertionUrn=assertion_urn,
                asserteeUrn=self._get_assertion_dataset_urn(contract) or "",
                runId=run.id,
                status=AssertionRunStatusClass.COMPLETE,
                result=AssertionResultClass(
                    type=result_type,
                    nativeResults=native_results,
                ),
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=assertion_result,
            ).as_workunit()

            self.report.report_contract_runs_scanned()

    def _get_contract_workunits(
        self, contract: HightouchContract
    ) -> Iterable[MetadataWorkUnit]:
        """Get work units for a single contract and its runs"""
        self.report.report_contracts_scanned()

        yield from self._generate_assertion_from_contract(contract)

        if self.config.max_contract_runs_per_contract > 0:
            self.report.report_api_call()
            contract_runs = self.api_client.get_contract_runs(
                contract.id, limit=self.config.max_contract_runs_per_contract
            )
            yield from self._generate_assertion_results_from_contract_runs(
                contract, contract_runs
            )

    def _get_model_workunits(
        self, model: HightouchModel
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_models_scanned()

        source = self._get_source(model.source_id)
        model_dataset = self._generate_model_dataset(model, source)
        self.report.report_models_emitted()
        yield model_dataset

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        logger.info("Starting Hightouch metadata extraction")

        emitted_model_ids = set()

        self.report.report_api_call()
        syncs = self.api_client.get_syncs()
        logger.info(f"Found {len(syncs)} syncs")

        filtered_syncs = [
            sync for sync in syncs if self.config.sync_patterns.allowed(sync.slug)
        ]

        logger.info(f"Processing {len(filtered_syncs)} syncs after filtering")

        for sync in filtered_syncs:
            try:
                if self.config.emit_models_as_datasets:
                    emitted_model_ids.add(sync.model_id)

                yield from self._get_sync_workunits(sync)
            except Exception as e:
                self.report.warning(
                    title="Failed to process sync",
                    message=f"An error occurred while processing sync: {str(e)}",
                    context=f"sync_slug: {sync.slug} (sync_id: {sync.id})",
                    exc=e,
                )

        if self.config.emit_models_as_datasets:
            logger.info("Fetching standalone models")
            self.report.report_api_call()
            all_models = self.api_client.get_models()
            logger.info(f"Found {len(all_models)} models total")

            standalone_models = [
                model
                for model in all_models
                if model.id not in emitted_model_ids
                and self.config.model_patterns.allowed(model.name)
            ]

            logger.info(
                f"Processing {len(standalone_models)} standalone models after filtering"
            )

            for model in standalone_models:
                try:
                    yield from self._get_model_workunits(model)
                except Exception as e:
                    self.report.warning(
                        title="Failed to process model",
                        message=f"An error occurred while processing model: {str(e)}",
                        context=f"model_name: {model.name} (model_id: {model.id})",
                        exc=e,
                    )

        if self.config.include_contracts:
            logger.info("Fetching event contracts")
            self.report.report_api_call()
            contracts = self.api_client.get_contracts()
            logger.info(f"Found {len(contracts)} contracts")

            filtered_contracts = [
                contract
                for contract in contracts
                if self.config.contract_patterns.allowed(contract.name)
            ]

            logger.info(
                f"Processing {len(filtered_contracts)} contracts after filtering"
            )

            for contract in filtered_contracts:
                try:
                    yield from self._get_contract_workunits(contract)
                except Exception as e:
                    self.report.warning(
                        title="Failed to process contract",
                        message=f"An error occurred while processing contract: {str(e)}",
                        context=f"contract: {contract.name} (contract_id: {contract.id})",
                        exc=e,
                    )

    def get_report(self) -> SourceReport:
        return self.report
