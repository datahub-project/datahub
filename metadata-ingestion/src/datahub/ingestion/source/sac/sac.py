import json
import logging
from dataclasses import dataclass
from functools import partial
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pyodata
import pyodata.v2.model
import pyodata.v2.service
from authlib.integrations.requests_client import OAuth2Session
from pydantic import Field, SecretStr, validator
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    DEFAULT_ENV,
    DatasetSourceConfigMixin,
    EnvConfigMixin,
)
from datahub.emitter.mce_builder import (
    dataset_urn_to_key,
    make_dashboard_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
    auto_incremental_lineage,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes, DatasetSubTypes
from datahub.ingestion.source.sac.sac_common import (
    ImportDataModelColumn,
    Resource,
    ResourceModel,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsClass,
    BrowsePathsV2Class,
    ChangeAuditStampsClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities import config_clean

logger = logging.getLogger(__name__)


class ConnectionMappingConfig(EnvConfigMixin):
    platform: Optional[str] = Field(
        default=None, description="The platform that this connection mapping belongs to"
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description="The instance of the platform that this connection mapping belongs to",
    )

    env: str = Field(
        default=DEFAULT_ENV,
        description="The environment that this connection mapping belongs to",
    )


class SACSourceConfig(
    StatefulIngestionConfigBase, DatasetSourceConfigMixin, IncrementalLineageConfigMixin
):
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion related configs",
    )

    tenant_url: str = Field(description="URL of the SAP Analytics Cloud tenant")
    token_url: str = Field(
        description="URL of the OAuth token endpoint of the SAP Analytics Cloud tenant"
    )
    client_id: str = Field(description="Client ID for the OAuth authentication")
    client_secret: SecretStr = Field(
        description="Client secret for the OAuth authentication"
    )

    ingest_stories: bool = Field(
        default=True,
        description="Controls whether Stories should be ingested",
    )

    ingest_applications: bool = Field(
        default=True,
        description="Controls whether Analytic Applications should be ingested",
    )

    ingest_import_data_model_schema_metadata: bool = Field(
        default=True,
        description="Controls whether schema metadata of Import Data Models should be ingested (ingesting schema metadata of Import Data Models significantly increases overall ingestion time)",
    )

    resource_id_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting resource ids that are to be included",
    )

    resource_name_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting resource names that are to be included",
    )

    folder_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting folders that are to be included",
    )

    connection_mapping: Dict[str, ConnectionMappingConfig] = Field(
        default={}, description="Custom mappings for connections"
    )

    query_name_template: Optional[str] = Field(
        default="QUERY/{name}",
        description="Template for generating dataset urns of consumed queries, the placeholder {query} can be used within the template for inserting the name of the query",
    )

    @validator("tenant_url", "token_url")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)


@dataclass
class SACSourceReport(StaleEntityRemovalSourceReport):
    pass


@platform_name("SAP Analytics Cloud", id="sac")
@config_class(SACSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default (only for Live Data Models)",
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Enabled by default (only for Import Data Models)",
)
class SACSource(StatefulIngestionSourceBase, TestableSource):
    config: SACSourceConfig
    report: SACSourceReport
    platform = "sac"

    session: OAuth2Session
    client: pyodata.Client

    ingested_dataset_entities: Set[str] = set()
    ingested_upstream_dataset_keys: Set[str] = set()

    def __init__(self, config: SACSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = SACSourceReport()

        self.session, self.client = SACSource.get_sac_connection(self.config)

    def close(self) -> None:
        self.session.close()
        super().close()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SACSource":
        config = SACSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()

        try:
            config = SACSourceConfig.parse_obj(config_dict)

            # when creating the pyodata.Client, the metadata is automatically parsed and validated
            session, _ = SACSource.get_sac_connection(config)

            # test the Data Import Service separately here, because it requires specific properties when configuring the OAuth client
            response = session.get(url=f"{config.tenant_url}/api/v1/dataimport/models")
            response.raise_for_status()

            session.close()

            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"{e}"
            )

        return test_report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            partial(
                auto_incremental_lineage,
                self.config.incremental_lineage,
            ),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        if self.config.ingest_stories or self.config.ingest_applications:
            resources = self.get_resources()

            for resource in resources:
                datasets = []

                for resource_model in resource.resource_models:
                    dataset_urn = make_dataset_urn_with_platform_instance(
                        platform=self.platform,
                        name=f"{resource_model.namespace}:{resource_model.model_id}",
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )

                    if dataset_urn not in datasets:
                        datasets.append(dataset_urn)

                    if dataset_urn in self.ingested_dataset_entities:
                        continue

                    self.ingested_dataset_entities.add(dataset_urn)

                    yield from self.get_model_workunits(dataset_urn, resource_model)

                yield from self.get_resource_workunits(resource, datasets)

    def get_report(self) -> SACSourceReport:
        return self.report

    def get_resource_workunits(
        self, resource: Resource, datasets: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        dashboard_urn = make_dashboard_urn(
            platform=self.platform,
            name=resource.resource_id,
            platform_instance=self.config.platform_instance,
        )

        if resource.ancestor_path:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=BrowsePathsClass(
                    paths=[
                        f"/{self.platform}/{resource.ancestor_path}",
                    ],
                ),
            )

            yield mcp.as_workunit()

            mcp = MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=BrowsePathsV2Class(
                    path=[
                        BrowsePathEntryClass(id=folder_name)
                        for folder_name in resource.ancestor_path.split("/")
                    ],
                ),
            )

            yield mcp.as_workunit()

        if self.config.platform_instance is not None:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            )

            yield mcp.as_workunit()

        mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=DashboardInfoClass(
                title=resource.name,
                description=resource.description,
                lastModified=ChangeAuditStampsClass(
                    created=AuditStampClass(
                        time=round(resource.created_time.timestamp() * 1000),
                        actor=(
                            make_user_urn(resource.created_by)
                            if resource.created_by
                            else "urn:li:corpuser:unknown"
                        ),
                    ),
                    lastModified=AuditStampClass(
                        time=round(resource.modified_time.timestamp() * 1000),
                        actor=(
                            make_user_urn(resource.modified_by)
                            if resource.modified_by
                            else "urn:li:corpuser:unknown"
                        ),
                    ),
                ),
                customProperties={
                    "resourceType": resource.resource_type,
                    "resourceSubtype": resource.resource_subtype,
                    "storyId": resource.story_id,
                    "isMobile": str(resource.is_mobile),
                },
                datasets=sorted(datasets) if datasets else None,
                externalUrl=f"{self.config.tenant_url}{resource.open_url}",
            ),
        )

        yield mcp.as_workunit()

        type_name: Optional[str] = None
        if resource.resource_subtype == "":
            type_name = BIAssetSubTypes.SAC_STORY
        elif resource.resource_subtype == "APPLICATION":
            type_name = BIAssetSubTypes.SAC_APPLICATION

        if type_name:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=SubTypesClass(
                    typeNames=[type_name],
                ),
            )

            yield mcp.as_workunit()

    def get_model_workunits(
        self, dataset_urn: str, model: ResourceModel
    ) -> Iterable[MetadataWorkUnit]:
        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=model.name,
                description=model.description,
                customProperties={
                    "namespace": model.namespace,
                    "modelId": model.model_id,
                    "isImport": "true" if model.is_import else "false",
                },
                externalUrl=f"{self.config.tenant_url}/sap/fpa/ui/tenants/3c44c#view_id=model;model_id={model.namespace}:{model.model_id}",
            ),
        )

        yield mcp.as_workunit()

        if model.is_import and self.config.ingest_import_data_model_schema_metadata:
            primary_fields: List[str] = []
            schema_fields: List[SchemaFieldClass] = []

            columns = self.get_import_data_model_columns(model_id=model.model_id)
            for column in columns:

                schema_field = SchemaFieldClass(
                    fieldPath=column.name,
                    type=self.get_schema_field_data_type(column),
                    nativeDataType=self.get_schema_field_native_data_type(column),
                    description=column.description,
                    isPartOfKey=column.is_key,
                )

                schema_fields.append(schema_field)

                if column.is_key:
                    primary_fields.append(column.name)

            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName=model.model_id,
                    platform=make_data_platform_urn(self.platform),
                    version=0,
                    hash="",
                    platformSchema=SchemalessClass(),
                    fields=schema_fields,
                    primaryKeys=primary_fields,
                ),
            )

            yield mcp.as_workunit()

        if model.system_type in ("BW", "HANA") and model.external_id is not None:
            upstream_dataset_name: Optional[str] = None

            if model.system_type == "BW" and model.external_id.startswith(
                "query:"
            ):  # query:[][][query]
                query = model.external_id[11:-1]
                upstream_dataset_name = self.get_query_name(query)
            elif model.system_type == "HANA" and model.external_id.startswith(
                "view:"
            ):  # view:[schema][schema.namespace][view]
                schema, namespace_with_schema, view = model.external_id.split("][", 2)
                schema = schema[6:]
                namespace: Optional[str] = None
                if len(schema) < len(namespace_with_schema):
                    namespace = namespace_with_schema[len(f"{schema}.") :]
                view = view[:-1]
                upstream_dataset_name = self.get_view_name(schema, namespace, view)

            if upstream_dataset_name is not None:
                if model.connection_id in self.config.connection_mapping:
                    connection = self.config.connection_mapping[model.connection_id]
                    platform = (
                        connection.platform
                        if connection.platform
                        else model.system_type.lower()
                    )
                    platform_instance = connection.platform_instance
                    env = connection.env
                else:
                    platform = model.system_type.lower()
                    platform_instance = model.connection_id
                    env = DEFAULT_ENV

                    logger.info(
                        f"No connection mapping found for connection with id {model.connection_id}, connection id will be used as platform instance"
                    )

                upstream_dataset_urn = make_dataset_urn_with_platform_instance(
                    platform=platform,
                    name=upstream_dataset_name,
                    platform_instance=platform_instance,
                    env=env,
                )

                if upstream_dataset_urn not in self.ingested_upstream_dataset_keys:
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=upstream_dataset_urn,
                        aspect=dataset_urn_to_key(upstream_dataset_urn),
                    )

                    yield mcp.as_workunit(is_primary_source=False)

                    self.ingested_upstream_dataset_keys.add(upstream_dataset_urn)

                mcp = MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=upstream_dataset_urn,
                                type=DatasetLineageTypeClass.COPY,
                            ),
                        ],
                    ),
                )

                yield mcp.as_workunit()
            else:
                self.report.report_warning(
                    "unknown-upstream-dataset",
                    f"Unknown upstream dataset for model with id {model.namespace}:{model.model_id} and external id {model.external_id}",
                )
        elif model.system_type is not None:
            self.report.report_warning(
                "unknown-system-type",
                f"Unknown system type {model.system_type} for model with id {model.namespace}:{model.model_id} and external id {model.external_id}",
            )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(
                removed=False,
            ),
        )

        yield mcp.as_workunit()

        if model.external_id and model.connection_id and model.system_type:
            type_name = DatasetSubTypes.SAC_LIVE_DATA_MODEL
        elif model.is_import:
            type_name = DatasetSubTypes.SAC_IMPORT_DATA_MODEL
        else:
            type_name = DatasetSubTypes.SAC_MODEL

        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(
                typeNames=[type_name],
            ),
        )

        yield mcp.as_workunit()

        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=self.config.platform_instance,
            ),
        )

        yield mcp.as_workunit()

    @staticmethod
    def get_sac_connection(
        config: SACSourceConfig,
    ) -> Tuple[OAuth2Session, pyodata.Client]:
        session = OAuth2Session(
            client_id=config.client_id,
            client_secret=config.client_secret.get_secret_value(),
            token_endpoint=config.token_url,
            token_endpoint_auth_method="client_secret_post",
            grant_type="client_credentials",
        )

        retries = 3
        backoff_factor = 10
        status_forcelist = (500,)

        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.register_compliance_hook(
            "protected_request", _add_sap_sac_custom_auth_header
        )
        session.fetch_token()

        client = pyodata.Client(
            url=f"{config.tenant_url}/api/v1",
            connection=session,
            config=pyodata.v2.model.Config(retain_null=True),
        )

        return session, client

    def get_resources(self) -> Iterable[Resource]:
        import_data_model_ids = self.get_import_data_model_ids()

        filter = "isTemplate eq 0 and isSample eq 0 and isPublic eq 1"
        if self.config.ingest_stories and self.config.ingest_applications:
            filter += " and ((resourceType eq 'STORY' and resourceSubtype eq '') or (resourceType eq 'STORY' and resourceSubtype eq 'APPLICATION'))"
        elif self.config.ingest_stories and not self.config.ingest_applications:
            filter += " and resourceType eq 'STORY' and resourceSubtype eq ''"
        elif not self.config.ingest_stories and self.config.ingest_applications:
            filter += (
                " and resourceType eq 'STORY' and resourceSubtype eq 'APPLICATION'"
            )

        select = "resourceId,resourceType,resourceSubtype,storyId,name,description,createdTime,createdBy,modifiedBy,modifiedTime,openURL,ancestorPath,isMobile"

        entities: pyodata.v2.service.ListWithTotalCount = (
            self.client.entity_sets.Resources.get_entities()
            .custom("$format", "json")
            .filter(filter)
            .select(select)
            .execute()
        )
        entity: pyodata.v2.service.EntityProxy
        for entity in entities:
            resource_id: str = entity.resourceId
            name: str = entity.name.strip()

            if not self.config.resource_id_pattern.allowed(
                resource_id
            ) or not self.config.resource_name_pattern.allowed(name):
                continue

            ancestor_path: Optional[str] = None

            try:
                ancestors = json.loads(entity.ancestorPath)
                ancestor_path = "/".join(
                    ancestor.replace("/", "%2F") for ancestor in ancestors
                )
            except json.JSONDecodeError:
                pass

            if ancestor_path and not self.config.folder_pattern.allowed(ancestor_path):
                continue

            resource_models: Set[ResourceModel] = set()

            select = "modelId,name,description,externalId,connectionId,systemType"

            nav_entities: pyodata.v2.service.EntitySetProxy = (
                entity.nav("resourceModels")
                .get_entities()
                .custom("$format", "json")
                .select(select)
                .execute()
            )
            nav_entity: pyodata.v2.service.EntityProxy
            for nav_entity in nav_entities:
                # the model id can have a different structure, commonly all model ids have a namespace (the part before the colon) and the model id itself
                # t.4.sap.fpa.services.userFriendlyPerfLog:ACTIVITY_LOG is a builtin model without a possiblity to get more metadata about the model
                # t.4.YV67EM4QBRU035A7TVKERZ786N:YV67EM4QBRU035A7TVKERZ786N is a model id where the model id itself also appears as part of the namespace
                # t.4:C76tt2j402o1e69wnvrwfcl79c is a model id without the model id itself as part of the namespace
                model_id: str = nav_entity.modelId
                namespace, _, model_id = model_id.partition(":")

                resource_models.add(
                    ResourceModel(
                        namespace=namespace,
                        model_id=model_id,
                        name=nav_entity.name.strip(),
                        description=nav_entity.description.strip(),
                        system_type=nav_entity.systemType,  # BW or HANA
                        connection_id=nav_entity.connectionId,
                        external_id=nav_entity.externalId,  # query:[][][query] or view:[schema][schema.namespace][view]
                        is_import=model_id in import_data_model_ids,
                    )
                )

            created_by: Optional[str] = entity.createdBy
            if created_by in ("SYSTEM", "$DELETED_USER$"):
                created_by = None

            modified_by: Optional[str] = entity.modifiedBy
            if modified_by in ("SYSTEM", "$DELETED_USER$"):
                modified_by = None

            yield Resource(
                resource_id=resource_id,
                resource_type=entity.resourceType,
                resource_subtype=entity.resourceSubtype,
                story_id=entity.storyId,
                name=name,
                description=entity.description.strip(),
                created_time=entity.createdTime,
                created_by=created_by,
                modified_time=entity.modifiedTime,
                modified_by=modified_by,
                open_url=entity.openURL,
                ancestor_path=ancestor_path,
                is_mobile=entity.isMobile,
                resource_models=frozenset(resource_models),
            )

    def get_import_data_model_ids(self) -> Set[str]:
        response = self.session.get(
            url=f"{self.config.tenant_url}/api/v1/dataimport/models"
        )
        response.raise_for_status()

        import_data_model_ids = set(
            model["modelID"] for model in response.json()["models"]
        )
        return import_data_model_ids

    def get_import_data_model_columns(
        self, model_id: str
    ) -> List[ImportDataModelColumn]:
        response = self.session.get(
            url=f"{self.config.tenant_url}/api/v1/dataimport/models/{model_id}/metadata"
        )
        response.raise_for_status()

        model_metadata = response.json()

        columns: List[ImportDataModelColumn] = []
        for column in model_metadata["factData"]["columns"]:
            columns.append(
                ImportDataModelColumn(
                    name=column["columnName"].strip(),
                    description=column["descriptionName"].strip(),
                    property_type=column["propertyType"],
                    data_type=column["columnDataType"],
                    max_length=column.get("maxLength"),
                    precision=column.get("precision"),
                    scale=column.get("scale"),
                    is_key=column["isKey"],
                )
            )

        return columns

    def get_query_name(self, query: str) -> str:
        if not self.config.query_name_template:
            return query

        query_name = self.config.query_name_template
        query_name = query_name.replace("{name}", query)

        return query_name

    def get_view_name(self, schema: str, namespace: Optional[str], view: str) -> str:
        if namespace:
            return f"{schema}.{namespace}::{view}"

        return f"{schema}.{view}"

    def get_schema_field_data_type(
        self, column: ImportDataModelColumn
    ) -> SchemaFieldDataTypeClass:
        if column.property_type == "DATE":
            return SchemaFieldDataTypeClass(type=DateTypeClass())
        else:
            if column.data_type == "string":
                return SchemaFieldDataTypeClass(type=StringTypeClass())
            elif column.data_type in ("decimal", "int32"):
                return SchemaFieldDataTypeClass(type=NumberTypeClass())
            else:
                self.report.report_warning(
                    "unknown-data-type",
                    f"Unknown data type {column.data_type} found",
                )

                return SchemaFieldDataTypeClass(type=NullTypeClass())

    def get_schema_field_native_data_type(self, column: ImportDataModelColumn) -> str:
        native_data_type = column.data_type
        if column.data_type == "decimal":
            native_data_type = f"{column.data_type}({column.precision}, {column.scale})"
        elif column.data_type == "int32":
            native_data_type = f"{column.data_type}({column.precision})"
        elif column.max_length is not None:
            native_data_type = f"{column.data_type}({column.max_length})"

        return native_data_type


def _add_sap_sac_custom_auth_header(
    url: str, headers: Dict[str, str], body: Any
) -> Tuple[str, Dict[str, str], Any]:
    headers["x-sap-sac-custom-auth"] = "true"
    return url, headers, body
