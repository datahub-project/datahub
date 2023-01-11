import logging
import re
from dataclasses import dataclass, field as dataclass_field
from time import sleep
from typing import Any, Dict, Iterable, List, Optional, Type, Union
from xmlrpc.client import Boolean

import msal
import pydantic
import requests

import datahub.emitter.mce_builder as builder
import datahub.ingestion.source.powerbi_v2.powerbi_models as models
from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationError,
)
from datahub.emitter.mce_builder import make_container_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import PlatformKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.powerbi_v2.constants import APIEndpoints, Constant
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BrowsePathsClass,
    ChangeTypeClass,
    ChartInfoClass,
    ChartKeyClass,
    ContainerClass,
    CorpUserInfoClass,
    CorpUserKeyClass,
    DashboardInfoClass,
    DashboardKeyClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
)
from datahub.utilities.dedup_list import deduplicate_list

# Logger instance
LOGGER = logging.getLogger(__name__)

FIELD_TYPE_MAPPING = {
    "Int64": NumberTypeClass(),
    "Double": NumberTypeClass(),
    "Boolean": BooleanTypeClass(),
    "Datetime": DateTypeClass(),
    "DateTime": DateTypeClass(),
    "String": StringTypeClass(),
    "Decimal": NumberTypeClass(),
}


class WorkspaceKey(PlatformKey):
    workspace_id: str


def gen_workspace_key(platform_name: str, workspace_id: str) -> WorkspaceKey:
    return WorkspaceKey(
        platform=platform_name, instance=None, workspace_id=workspace_id
    )


class OwnershipMapping(ConfigModel):
    enabled: bool = pydantic.Field(
        default=True, description="Whether to ingest PowerBI ownership"
    )
    use_powerbi_email: bool = pydantic.Field(
        default=False,
        description="Use PowerBI User email to ingest, default is powerbi user identifier",
    )
    remove_email_suffix: bool = pydantic.Field(
        default=False,
        description="Remove PowerBI User email suffix for example, @abc.com",
    )


class PowerBIStatefulIngestionConfig(StatefulStaleMetadataRemovalConfig):
    """
    Specialization of basic StatefulStaleMetadataRemovalConfig to adding custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the SQLAlchemyConfig.
    """

    _entity_types: List[str] = pydantic.Field(
        default=["chart", "dataset", "dashboard", "container"]
    )


class PowerBiSourceConfig(StatefulIngestionConfigBase):
    tenant_id: str = pydantic.Field(description="PowerBI tenant identifier")
    client_id: str = pydantic.Field(description="Azure app client identifier")
    client_secret: str = pydantic.Field(description="Azure app client secret")
    scan_timeout: int = pydantic.Field(
        default=60, description="timeout for PowerBI metadata scanning"
    )
    # dataset_type_mapping: Dict[str, str] = pydantic.Field(
    #     description="Mapping of PowerBI datasource type to DataHub supported data-sources. Will be use for lineage"
    # )
    ownership: OwnershipMapping = pydantic.Field(
        default=OwnershipMapping(),
        description="Configure whether and how to ingest ownership from powerbi",
    )
    ingest_catalog_container: bool = pydantic.Field(
        default=False,
        description="Whether to ingest PowerBI platform catalog container, true / false",
    )
    extract_dataset: bool = pydantic.Field(
        default=False,
        description="Whether PowerBI dataset should be ingested, true / false",
    )
    extract_dashboard: bool = pydantic.Field(
        default=False,
        description="Whether PowerBI dashboard should be ingested, true / false",
    )
    extract_tile: bool = pydantic.Field(
        default=False,
        description="Whether PowerBI tile should be ingested, true / false",
    )
    extract_report: bool = pydantic.Field(
        default=False,
        description="Whether PowerBI Reports should be ingested, true / false",
    )
    modified_since: Optional[str] = pydantic.Field(
        description="Get only recently modified workspaces based on modified_since datetime, excludePersonalWorkspaces and excludeInActiveWorkspaces limit to last 30 days",
    )
    workspace_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter for PowerBI Workspace IDs",
    )
    stateful_ingestion: Optional[PowerBIStatefulIngestionConfig] = pydantic.Field(
        default=None, description="PowerBI Stateful Ingestion Config."
    )


class PowerBiAPI:
    # API endpoints of PowerBI Report Server to fetch reports, datasets

    def __init__(self, config: PowerBiSourceConfig) -> None:
        self.workspace_name: str = ""
        self.__config: PowerBiSourceConfig = config
        self.__msal_client = msal.ConfidentialClientApplication(
            self.__config.client_id,
            client_credential=self.__config.client_secret,
            authority=APIEndpoints.POWERBI_AUTHORITY_URL + self.__config.tenant_id,
        )
        self.__access_token: str = self.get_access_token()

    def get_access_token(self):
        auth_response = self.__msal_client.acquire_token_for_client(
            scopes=[APIEndpoints.POWERBI_SCOPE]
        )
        if not auth_response.get("access_token"):
            raise ConfigurationError(
                "Failed to generate the PowerBi access token, powerbi authorization failed . Please check your input configuration."
            )
        LOGGER.info("Generated PowerBi access token")

        __access_token = "Bearer {}".format(auth_response.get("access_token"))
        return __access_token

    def requests_get(self, url: str, params: Optional[Dict[str, str]] = None) -> Any:
        try:
            LOGGER.info("Request to Report URL={}".format(url))
            response = requests.get(
                url,
                headers={Constant.AUTHORIZATION: self.__access_token},
                params=params,
            )
        except ConnectionError:
            LOGGER.info("Retrying... Request to Report URL={}".format(url))
            response = requests.get(
                url=url,
                headers={Constant.AUTHORIZATION: self.__access_token},
                params=params,
            )
        # Check if we got response from PowerBi Report Server
        if response.status_code != 200:
            message: str = "Failed to fetch from API"
            LOGGER.warning(message)
            LOGGER.warning("url: {}".format(url))
            raise ValueError(message)

        return response.json()

    def get_mod_workspaces(self) -> List[str]:
        """
        Get list of mod workspaces
        """
        mod_workspace_endpoint = APIEndpoints.WORKSPACE_MODIFIED_LIST
        parameters: Dict[str, Any] = {
            "excludePersonalWorkspaces": True,
            "excludeInActiveWorkspaces": True,
        }
        if self.__config.modified_since:
            parameters["modifiedSince"] = self.__config.modified_since
        response_json = self.requests_get(mod_workspace_endpoint, parameters)
        workspace_ids = [row["id"] for row in response_json]
        LOGGER.info("workspace_ids: {}".format(workspace_ids))
        return workspace_ids

    def create_scan_job(self, workspace_ids: List[str]) -> str:
        """
        Create scan job on PowerBi for the workspace
        """
        scan_create_endpoint = APIEndpoints.SCAN_CREATE
        request_body = {"workspaces": workspace_ids}
        response = requests.post(
            scan_create_endpoint,
            data=request_body,
            params={
                "datasetExpressions": True,
                "datasetSchema": True,
                "datasourceDetails": True,
                "getArtifactUsers": True,
                "lineage": True,
            },
            headers={Constant.AUTHORIZATION: self.__access_token},
        )
        if response.status_code != 202:
            message: str = "Failed to create Scan"
            LOGGER.warning(message)
            LOGGER.warning("url: {}".format(response.url))
            raise ValueError(message)

        scan_id = response.json()["id"]
        LOGGER.info("Scan id({})".format(id))
        return scan_id

    def poll_for_scan_to_complete(self, scan_id: str, timeout: int) -> Boolean:
        """
        Poll the PowerBi service for workspace scan to complete
        """
        minimum_sleep = 3
        if timeout < minimum_sleep:
            LOGGER.info(
                f"Setting timeout to minimum_sleep time {minimum_sleep} seconds"
            )
            timeout = minimum_sleep

        max_retry = timeout // minimum_sleep
        LOGGER.info(f"Max retry {max_retry}")
        scan_get_endpoint = APIEndpoints.SCAN_GET.format(SCAN_ID=scan_id)

        retry = 1
        while True:
            LOGGER.info(f"Retry = {retry}")
            response_json = self.requests_get(scan_get_endpoint)

            if response_json["status"].upper() == "Succeeded".upper():
                LOGGER.info(f"Scan result is available for scan id({scan_id})")
                return True
            if retry == max_retry:
                return False
            LOGGER.info(f"Sleeping for {minimum_sleep} seconds")
            sleep(minimum_sleep)
            retry += 1

    def get_scan_result(self, scan_id: str) -> Dict[str, Any]:
        LOGGER.info("Fetching scan result")
        LOGGER.info(f"scan_id: {scan_id}")
        scan_result_get_endpoint = APIEndpoints.SCAN_RESULT_GET.format(SCAN_ID=scan_id)

        response_json = self.requests_get(scan_result_get_endpoint)

        return response_json

    def launch_powerbi_scan(self, workspace_ids):
        scan_id = self.create_scan_job(workspace_ids)
        is_scan_completed = self.poll_for_scan_to_complete(
            scan_id, self.__config.scan_timeout
        )
        if is_scan_completed:
            scan_result = self.get_scan_result(scan_id)
        else:
            raise ValueError(
                "Scan result wasn't returned, please increase scan_timeout"
            )
        return scan_result


class EquableMetadataWorkUnit(MetadataWorkUnit):
    """
    We can add EquableMetadataWorkUnit to set.
    This will avoid passing same MetadataWorkUnit to DataHub Ingestion framework.
    """

    def __eq__(self, instance):
        return self.id == instance.id

    def __hash__(self):
        return id(self.id)


@dataclass
class PowerBiDashboardSourceReport(StaleEntityRemovalSourceReport):
    dashboards_scanned: int = 0
    charts_scanned: int = 0
    datasets_scanned: int = 0
    reports_scanned: int = 0
    filtered_dashboards: List[str] = dataclass_field(default_factory=list)
    filtered_charts: List[str] = dataclass_field(default_factory=list)

    def report_datasets_scanned(self, count: int = 1) -> None:
        self.datasets_scanned += count

    def report_dashboards_scanned(self, count: int = 1) -> None:
        self.dashboards_scanned += count

    def report_reports_scanned(self, count: int = 1) -> None:
        self.reports_scanned += count

    def report_charts_scanned(self, count: int = 1) -> None:
        self.charts_scanned += count

    def report_dashboards_dropped(self, model: str) -> None:
        self.filtered_dashboards.append(model)

    def report_charts_dropped(self, view: str) -> None:
        self.filtered_charts.append(view)


class PowerBiStaleEntityRemovalHandler(StaleEntityRemovalHandler):
    def __init__(
        self,
        source: StatefulIngestionSourceBase,
        config: StatefulIngestionConfigBase,
        state_type_class: Type[StaleEntityCheckpointStateBase],
        pipeline_name: Optional[str],
        run_id: str,
    ):
        super().__init__(source, config, state_type_class, pipeline_name, run_id)

    def _init_job_id(self) -> JobId:
        job_name_suffix = "stale_entity_removal"
        platform: Optional[str] = getattr(self.source, "platform")
        if getattr(self.source, "current_workspace_id", None):
            job_name_suffix += "_{}".format(
                getattr(self.source, "current_workspace_id")
            )

        return JobId(f"{platform}_{job_name_suffix}" if platform else job_name_suffix)


@platform_name("PowerBI")
@config_class(PowerBiSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(
    SourceCapability.OWNERSHIP, "On by default but can disabled by configuration"
)
class PowerBiDashboardV2Source(StatefulIngestionSourceBase):
    platform = "powerbi"
    platform_urn: str = builder.make_data_platform_urn(platform=platform)
    source_config: PowerBiSourceConfig
    reporter: PowerBiDashboardSourceReport
    accessed_dashboards: int = 0

    @classmethod
    def create(cls, config_dict, ctx):
        config = PowerBiSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def __init__(self, config: PowerBiSourceConfig, ctx: PipelineContext):
        super().__init__(config=config, ctx=ctx)
        self.source_config = config
        self.reporter = PowerBiDashboardSourceReport()
        self.powerbi_client = PowerBiAPI(self.source_config)
        self.current_workspace_id = ""
        self.current_workspace_name = ""
        # Create and register the stateful ingestion stale entity removal handler.
        self.stale_entity_removal_handler = PowerBiStaleEntityRemovalHandler(
            source=self,
            config=self.source_config,
            state_type_class=GenericCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

    def reset_current_entity_urn_map(self) -> None:
        self.current_entity_urn_map: Dict[str, List[str]] = {
            "chart": [],
            "dataset": [],
            "dashboard": [],
            "container": [],
        }

    def get_platform_instance_id(self) -> str:
        if self.platform is None:
            raise ValueError("Unlikely but platform is not set")
        return self.platform

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:  # noqa: C901
        """
        Datahub Ingestion framework invoke this method
        """
        LOGGER.info("PowerBi plugin execution started")

        workspace_ids = self.powerbi_client.get_mod_workspaces()
        workspace_ids = [
            workspace_id
            for workspace_id in workspace_ids
            if self.source_config.workspace_pattern.allowed(workspace_id)
        ]

        LOGGER.info(f"Total of {len(workspace_ids)} workspaces to be ingested")
        # we can batch here if we want...
        scan_result_json = self.powerbi_client.launch_powerbi_scan(workspace_ids)
        scan_result = models.ScanResult.parse_obj(scan_result_json)
        if self.source_config.ingest_catalog_container:
            yield from self.gen_catalog_container()

        for workspace in scan_result.workspaces:
            # Fetch PowerBi workspace for given workspace identifier
            self.current_workspace_id = workspace.id
            self.current_workspace_name = workspace.name
            self.reset_current_entity_urn_map()
            """ TODO: this is getting hacky,
            Because the recipe is capable of only ingesting `modified_since` workspaces, hence the need for below...
            Because of `job_checkpoint_aspects[job_name] = checkpoint_aspect`
            When we prepare to commit the checkpoints, it actually only store 1 checkpoint to 1 job_name only
            Over here we make the `job_name` dynamic by suffixing it with `workspace_id` so we can store checkpoint for each workspace_id
            Hence the need for `PowerBiStaleEntityRemovalHandler` to facilitate this.
            We then `register_stateful_ingestion_usecase_handler` for each new `job_id` constructed
            """
            self.stale_entity_removal_handler._job_id = (
                self.stale_entity_removal_handler._init_job_id()
            )
            self.register_stateful_ingestion_usecase_handler(
                self.stale_entity_removal_handler
            )

            yield from self.gen_datahub_container(workspace)

            if self.source_config.extract_dashboard:
                for dashboard in workspace.dashboards:
                    try:
                        self.reporter.report_dashboards_scanned()
                        self.reporter.report_charts_scanned(count=len(dashboard.tiles))
                        workunits = self.to_datahub_work_units(dashboard)
                        for workunit in workunits:
                            self.reporter.report_workunit(workunit)
                            yield workunit
                    except Exception as e:
                        message = f"Error ({e}) occurred while loading dashboard {dashboard.display_name}(id={dashboard.id}) tiles."
                        LOGGER.exception(message, e)
                        self.reporter.report_warning(dashboard.id, message)

            if self.source_config.extract_dataset:
                for dataset in workspace.datasets:
                    if dataset:
                        try:
                            self.reporter.report_datasets_scanned()
                            workunits = self.to_datahub_work_units(dataset)
                            for workunit in workunits:
                                self.reporter.report_workunit(workunit)
                                yield workunit
                        except Exception as e:
                            message = f"Error ({e}) occurred while loading dataset {dataset.name}"
                            LOGGER.exception(message, e)
                            self.reporter.report_warning(dataset.id, message)

            if self.source_config.extract_report:
                for report in workspace.reports:
                    try:
                        self.reporter.report_reports_scanned()
                        workunits = self.to_datahub_work_units(report)
                        for workunit in workunits:
                            self.reporter.report_workunit(workunit)
                            yield workunit
                    except Exception as e:
                        message = (
                            f"Error ({e}) occurred while loading report {report.name}"
                        )
                        LOGGER.exception(message, e)
                        self.reporter.report_warning(report.id, message)

            # Record the urn for each entity type into Checkpoint State
            for entity_type, urn_list in self.current_entity_urn_map.items():
                for entity_urn in set(urn_list):
                    self.stale_entity_removal_handler.add_entity_to_state(
                        type=entity_type,
                        urn=entity_urn,
                    )

            yield from self.stale_entity_removal_handler.gen_removed_entity_workunits()

    def get_report(self) -> SourceReport:
        return self.reporter

    def close(self):
        self.prepare_for_commit()

    def create_mcp(
        self,
        entity_type,
        entity_urn,
        aspect_name,
        aspect,
        change_type=ChangeTypeClass.UPSERT,
    ):
        return MetadataChangeProposalWrapper(
            entityType=entity_type,
            changeType=change_type,
            entityUrn=entity_urn,
            aspectName=aspect_name,
            aspect=aspect,
        )

    def to_urn_set(self, mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
        return deduplicate_list(
            [
                mcp.entityUrn
                for mcp in mcps
                if mcp is not None and mcp.entityUrn is not None
            ]
        )

    def __to_work_unit(
        self, mcp: MetadataChangeProposalWrapper
    ) -> EquableMetadataWorkUnit:
        return EquableMetadataWorkUnit(
            id="{PLATFORM}-{ENTITY_URN}-{ASPECT_NAME}".format(
                PLATFORM=self.platform,
                ENTITY_URN=mcp.entityUrn,
                ASPECT_NAME=mcp.aspectName,
            ),
            mcp=mcp,
        )

    def __get_container_mcp(self, workspace_id, entity_type, entity_urn):
        # create container for each entity in this workspace (container)
        workspace_container_key = gen_workspace_key(self.platform, workspace_id)
        container_urn = make_container_urn(
            guid=workspace_container_key.guid(),
        )
        container_mcp = MetadataChangeProposalWrapper(
            entityType=entity_type,
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=entity_urn,
            aspect=ContainerClass(container=f"{container_urn}"),
        )
        return container_mcp

    def __get_schema_field(
        self,
        table: models.Table,
        field: Union[models.Column, models.Measure],
    ) -> SchemaFieldClass:
        if isinstance(field, models.Column):
            data_type = field.data_type
            type_class = FIELD_TYPE_MAPPING.get(data_type)
            description = None
            if type_class is None:
                LOGGER.warning(
                    field,
                    f"Unable to map type {field.data_type} to metadata schema",
                )
                type_class = NullTypeClass()

        elif isinstance(field, models.Measure):
            # hardcoded measure to nulltype class, unsure what is the best
            data_type = "measure"
            type_class = NullTypeClass()
            if getattr(field, "expression", None):
                description = field.expression
            else:
                description = None

        schema_field = SchemaFieldClass(
            fieldPath=f"{table.name} {field.name}",
            type=SchemaFieldDataTypeClass(type=type_class),  # type:ignore
            nativeDataType=data_type,
            description=description,
        )
        return schema_field

    def __get_dataplatform_instance_mcp(
        self, datasetUrn: str
    ) -> MetadataChangeProposalWrapper:
        dataplatform_instance = DataPlatformInstanceClass(
            platform=builder.make_data_platform_urn(self.platform),
            instance=None,
        )

        dataplatform_instance_mcp = MetadataChangeProposalWrapper(
            entityType=Constant.DATASET,
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=datasetUrn,
            aspectName=Constant.DATAPLATFORM_INSTANCE,
            aspect=dataplatform_instance,
        )
        return dataplatform_instance_mcp

    def __to_datahub_dataset(
        self,
        dataset: Optional[models.Dataset],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi dataset to datahub dataset. Here we are mapping each table of PowerBi Dataset to Datahub dataset.
        In PowerBi Tile would be having single dataset, However corresponding Datahub's chart might have many input sources.
        """

        dataset_mcps: List[MetadataChangeProposalWrapper] = []
        if dataset is None:
            return dataset_mcps

        LOGGER.info(
            f"Converting dataset={dataset.name}(id={dataset.id}) to datahub dataset"
        )
        # Create an URN for dataset
        ds_urn = self.__create_dataset_urn(self.current_workspace_id, dataset.id)
        self.current_entity_urn_map["dataset"].append(ds_urn)

        LOGGER.info(f"dataset_urn: {ds_urn}")
        ds_properties = DatasetPropertiesClass(
            name=dataset.name,
            description=dataset.name,
            externalUrl=models.Dataset.get_web_url(
                self.current_workspace_id, dataset.id
            ),
            customProperties={
                "workspaceName": self.current_workspace_name,
                "workspaceId": self.current_workspace_id,
                "datasetId": dataset.id,
            },
        )

        fields = []
        # Loop through PowerBI Dataset and compile into one Datahub Dataset
        for table in dataset.tables:
            table_fields = [
                self.__get_schema_field(table, column) for column in table.columns
            ]
            measure_fields = [
                self.__get_schema_field(table, measure) for measure in table.measures
            ]
            fields.extend(table_fields)
            fields.extend(measure_fields)

        schema_metadata = SchemaMetadataClass(
            schemaName=dataset.name,
            platform=self.platform_urn,
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

        schema_mcp = self.create_mcp(
            entity_type=Constant.DATASET,
            entity_urn=ds_urn,
            aspect_name=Constant.SCHEMA_METADATA,
            aspect=schema_metadata,
        )

        info_mcp = self.create_mcp(
            entity_type=Constant.DATASET,
            entity_urn=ds_urn,
            aspect_name=Constant.DATASET_PROPERTIES,
            aspect=ds_properties,
        )
        # Remove status mcp
        status_mcp = self.create_mcp(
            entity_type=Constant.DATASET,
            entity_urn=ds_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )
        container_mcp = self.__get_container_mcp(
            self.current_workspace_id, Constant.DATASET, ds_urn
        )
        dataplaform_instance_mcp = self.__get_dataplatform_instance_mcp(ds_urn)
        subtype_mcp = self.__create_subtype_mcp(
            Constant.DATASET, ds_urn, "powerbi_dataset"
        )

        # ingest dataset ownership
        entity_owner = dataset.configured_by or None
        if self.source_config.ownership.enabled and entity_owner:
            entity_owner_urn = self.create_datahub_owner_urn(entity_owner)

            owner_mcp = self.__create_ownership_mcp(
                Constant.DATASET,
                ds_urn,
                OwnershipTypeClass.NONE,
                [entity_owner_urn],
            )
            if owner_mcp:
                dataset_mcps.append(owner_mcp)

        dataset_mcps.extend(
            [
                info_mcp,
                status_mcp,
                schema_mcp,
                container_mcp,
                dataplaform_instance_mcp,
                subtype_mcp,
            ]
        )

        return dataset_mcps

    def __to_datahub_chart(
        self, tile: models.Tile
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi tile to datahub chart
        """
        LOGGER.info("Converting tile {}(id={}) to chart".format(tile.title, tile.id))
        # Create an URN for chart
        chart_urn = builder.make_chart_urn(self.platform, tile.get_urn_part())
        self.current_entity_urn_map["chart"].append(chart_urn)

        LOGGER.info("chart_urn: {}".format(chart_urn))
        if tile.dataset_id:
            ds_input: List[str] = [
                self.__create_dataset_urn(self.current_workspace_id, tile.dataset_id)
            ]
        # Create chartInfo mcp
        # Set chartUrl only if tile is created from Report
        if tile.dataset_id or tile.report_id:
            custom_properties = {
                "datasetId": tile.dataset_id if tile.dataset_id else "",
                "reportId": tile.report_id if tile.report_id else "",
                "datasetWebUrl": models.Dataset.get_web_url(
                    self.current_workspace_id, tile.dataset_id
                )
                if tile.dataset_id
                else "",
            }
        else:
            custom_properties = None

        chart_info_instance = ChartInfoClass(
            title=tile.title or "",
            description=tile.title or "",
            lastModified=ChangeAuditStamps(),
            inputs=ds_input,
            customProperties=custom_properties,
        )

        info_mcp = self.create_mcp(
            entity_type=Constant.CHART,
            entity_urn=chart_urn,
            aspect_name=Constant.CHART_INFO,
            aspect=chart_info_instance,
        )

        # removed status mcp
        status_mcp = self.create_mcp(
            entity_type=Constant.CHART,
            entity_urn=chart_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        # ChartKey status
        chart_key_instance = ChartKeyClass(
            dashboardTool=self.platform,
            chartId=Constant.CHART_ID.format(tile.id),
        )

        chartkey_mcp = self.create_mcp(
            entity_type=Constant.CHART,
            entity_urn=chart_urn,
            aspect_name=Constant.CHART_KEY,
            aspect=chart_key_instance,
        )
        container_mcp = self.__get_container_mcp(
            self.current_workspace_id, Constant.CHART, chart_urn
        )

        return [info_mcp, status_mcp, chartkey_mcp, container_mcp]

    def __create_dataset_urn(
        self,
        workspace_id: str,  # not used, but if we want we can
        dataset_id: str,
    ) -> str:
        dataset_urn = builder.make_dataset_urn(
            platform=self.platform,
            name=f"{dataset_id}",
            env=self.source_config.env,
        )
        return dataset_urn

    def __create_subtype_mcp(
        self,
        entity_type: str,
        entity_urn: str,
        subtype: str,
    ) -> MetadataChangeProposalWrapper:
        subtypes_dict = {
            "dashboard": "Dashboard",
            "report": "Report",
            "powerbi_dataset": "PowerBIDataset",
        }

        subtype_cls = SubTypesClass(typeNames=[subtypes_dict[subtype]])

        subtype_mcp = self.create_mcp(
            entity_type=entity_type,
            entity_urn=entity_urn,
            aspect_name=Constant.SUBTYPES,
            aspect=subtype_cls,
        )
        return subtype_mcp

    def __create_ownership_mcp(
        self,
        entity_type: str,
        entity_urn: str,
        ownership_type: str,
        user_urn_list: List[str],
    ) -> Optional[MetadataChangeProposalWrapper]:
        owners = [
            OwnerClass(owner=user_urn, type=ownership_type)
            for user_urn in user_urn_list
            if user_urn is not None
        ]

        owner_mcp = None
        if len(owners) > 0:
            ownership = OwnershipClass(owners=owners)
            owner_mcp = self.create_mcp(
                entity_type=entity_type,
                entity_urn=entity_urn,
                aspect_name=Constant.OWNERSHIP,
                aspect=ownership,
            )
        return owner_mcp

    def gen_catalog_key(self) -> PlatformKey:
        return PlatformKey(
            platform=self.platform,
        )

    def gen_catalog_container(self) -> Iterable[MetadataWorkUnit]:
        container_workunits = gen_containers(
            container_key=self.gen_catalog_key(),
            name="PowerBI",
            sub_types=["Catalog"],
            description="PowerBI Catalog to store all PowerBI datasets, tiles, dashboard and reports.",
        )

        for wu in container_workunits:
            self.reporter.report_workunit(wu)
            yield wu

    def gen_datahub_container(
        self,
        workspace: models.Workspace,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map PowerBi workspace to Datahub container
        """
        user_mcps = self.to_datahub_users(workspace.users)

        workspace_container_key = gen_workspace_key(self.platform, workspace.id)
        container_urn = make_container_urn(
            guid=workspace_container_key.guid(),
        )
        self.current_entity_urn_map["container"].append(container_urn)

        if self.source_config.ownership.enabled and user_mcps:
            user_urn_list: List[str] = self.to_urn_set(user_mcps)
            owner_mcp = self.__create_ownership_mcp(
                Constant.CONTAINER,
                container_urn,
                OwnershipTypeClass.NONE,
                user_urn_list,
            )
            if owner_mcp:
                yield MetadataWorkUnit(
                    id=f"container-ownership-{workspace.name}-{container_urn}",
                    mcp=owner_mcp,
                )

        container_workunits = gen_containers(
            container_key=workspace_container_key,
            name=workspace.name,
            sub_types=["Workspace"],
            parent_container_key=self.gen_catalog_key()
            if self.source_config.ingest_catalog_container
            else None,
        )

        for workunit in container_workunits:
            self.reporter.report_workunit(workunit)
            yield workunit

    def to_datahub_dashboard(
        self,
        entity: Union[models.Dashboard, models.Report],
        chart_mcps: Optional[List[MetadataChangeProposalWrapper]] = None,
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi dashboard to Datahub dashboard
        """
        dashboard_urn = builder.make_dashboard_urn(self.platform, entity.get_urn_part())
        self.current_entity_urn_map["dashboard"].append(dashboard_urn)
        dashboard_mcps: List[MetadataChangeProposalWrapper] = []

        custom_properties = {
            "workspaceName": self.current_workspace_name,
            "workspaceId": self.current_workspace_id,
        }

        if isinstance(entity, models.Dashboard):
            ds_urn_list = None
            chart_urn_list: Optional[List[str]] = (
                self.to_urn_set(chart_mcps) if chart_mcps else None
            )
            custom_properties["chartCount"] = (
                str(len(entity.tiles)) if entity.tiles else ""
            )
            custom_properties["dashboardId"] = entity.id
            dashboard_name = entity.display_name
            dashboard_key_id = Constant.DASHBOARD_ID.format(entity.id)
            entity_owner = None

        elif isinstance(entity, models.Report):
            # normally report will map to 1 powerbi dataset
            ds_urn_list = (
                [
                    self.__create_dataset_urn(
                        self.current_workspace_id, entity.dataset_id
                    )
                ]
                if entity.dataset_id
                else None
            )
            chart_urn_list = None
            dashboard_name = entity.name
            custom_properties["reportId"] = entity.id
            dashboard_key_id = Constant.REPORT_ID.format(entity.id)

            entity_owner = entity.modified_by or entity.created_by or None

        # DashboardInfo mcp
        dashboard_info_cls = DashboardInfoClass(
            description=dashboard_name or "",
            title=dashboard_name or "",
            charts=chart_urn_list,
            datasets=ds_urn_list,
            lastModified=ChangeAuditStamps(),
            dashboardUrl=entity.__class__.get_web_url(
                self.current_workspace_id, entity.id
            ),
            customProperties=custom_properties,
        )

        info_mcp = self.create_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.DASHBOARD_INFO,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = self.create_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.platform,
            dashboardId=dashboard_key_id,
        )

        # Dashboard key
        dashboard_key_mcp = self.create_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.DASHBOARD_KEY,
            aspect=dashboard_key_cls,
        )

        subtype_mcp = self.__create_subtype_mcp(
            Constant.DASHBOARD, dashboard_urn, entity.entity_type
        )
        # Dashboard Ownership
        if self.source_config.ownership.enabled and entity_owner:
            entity_owner_urn = self.create_datahub_owner_urn(entity_owner)
            owner_mcp = self.__create_ownership_mcp(
                Constant.DASHBOARD,
                dashboard_urn,
                OwnershipTypeClass.NONE,
                [entity_owner_urn],
            )
            if owner_mcp:
                dashboard_mcps.append(owner_mcp)

        # Dashboard browsePaths
        browse_path = BrowsePathsClass(
            paths=["/powerbi/{}".format(self.current_workspace_name)]
        )
        browse_path_mcp = self.create_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.BROWSERPATH,
            aspect=browse_path,
        )

        container_mcp = self.__get_container_mcp(
            self.current_workspace_id, Constant.DASHBOARD, dashboard_urn
        )

        dashboard_mcps.extend(
            [
                browse_path_mcp,
                info_mcp,
                removed_status_mcp,
                dashboard_key_mcp,
                subtype_mcp,
                container_mcp,
            ]
        )

        return dashboard_mcps

    def create_datahub_owner_urn(self, user: str) -> str:
        """
        Create corpuser urn from PowerBi (configured by| modified by| created by) user
        """
        if self.source_config.ownership.remove_email_suffix:
            return builder.make_user_urn(user.split("@")[0])
        elif self.source_config.ownership.remove_email_suffix is False:
            return builder.make_user_urn(user)
        else:
            return builder.make_user_urn(f"users.{user}")

    def to_datahub_user(self, user: models.User) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi user to datahub user
        """

        LOGGER.info(
            f"Converting user {user.display_name}(id={user.identifier}) to datahub's user"
        )

        # Create an URN for user
        if self.source_config.ownership.use_powerbi_email:
            remove_email_suffix = self.source_config.ownership.remove_email_suffix
            user_urn = builder.make_user_urn(user.get_urn_part(remove_email_suffix))
        else:
            user_urn = builder.make_user_urn(user.get_urn_part())

        user_info_instance = CorpUserInfoClass(
            displayName=user.display_name,
            email=user.email_address,
            title=user.display_name,
            active=True,
        )

        info_mcp = self.create_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.CORP_USER_INFO,
            aspect=user_info_instance,
        )

        # removed status mcp
        status_mcp = self.create_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        user_key = CorpUserKeyClass(username=user.identifier)

        user_key_mcp = self.create_mcp(
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.CORP_USER_KEY,
            aspect=user_key,
        )

        return [info_mcp, status_mcp, user_key_mcp]

    def to_datahub_users(
        self, users: List[Optional[models.User]]
    ) -> List[MetadataChangeProposalWrapper]:
        user_mcps = []

        for user in users:
            if user:
                if user.principal_type == "User" and (
                    user.dataset_user_access_right == "ReadWriteReshareExplore"
                    or user.report_user_access_right == "Owner"
                    or user.dashboard_user_access_right == "Owner"
                    or user.group_user_access_right == "Admin"
                ):
                    user_mcps.extend(self.to_datahub_user(user))
                else:
                    continue

        return user_mcps

    def to_datahub_chart(
        self, tiles: List[models.Tile]
    ) -> List[MetadataChangeProposalWrapper]:
        chart_mcps = []
        LOGGER.info(f"Converting tiles(count={len(tiles)}) to charts")

        for tile in tiles:
            # skip if tile data is missing, or title is full of blanks...
            if tile is None or not tile.title or re.match(r"^[ ]*$", tile.title):
                continue
            chart_mcp = self.__to_datahub_chart(tile)
            chart_mcps.extend(chart_mcp)

        return chart_mcps

    def to_datahub_work_units(
        self, entity: Union[models.Dashboard, models.Report, models.Dataset]
    ) -> List[EquableMetadataWorkUnit]:
        mcps = []

        if isinstance(entity, models.Dashboard):
            LOGGER.info(
                f"Converting dashboard={entity.display_name} to datahub dashboard"
            )

            # Convert tiles to charts
            chart_mcps = self.to_datahub_chart(entity.tiles) if entity.tiles else None
            # Lets convert dashboard to datahub dashboard
            dashboard_mcps = self.to_datahub_dashboard(
                entity=entity,
                chart_mcps=chart_mcps,
            )
            # Now add MCPs in sequence
            if self.source_config.extract_tile and chart_mcps:
                mcps.extend(chart_mcps)
            mcps.extend(dashboard_mcps)

        elif isinstance(entity, models.Report):
            LOGGER.info(f"Converting report={entity.name} to datahub dashboard")

            # Lets convert report to datahub dashboard
            dashboard_mcps = self.to_datahub_dashboard(
                entity=entity,
            )

            # Now add MCPs in sequence
            mcps.extend(dashboard_mcps)

        elif isinstance(entity, models.Dataset):
            LOGGER.info(f"Converting dataset={entity.name} to datahub dataset")

            # Lets convert dataset to datahub dataset
            dataset_mcps = self.__to_datahub_dataset(
                dataset=entity,
            )

            mcps.extend(dataset_mcps)

        # Convert MCP to work_units
        work_units = map(self.__to_work_unit, mcps)

        # Return set of work_unit
        return deduplicate_list([wu for wu in work_units if wu is not None])
