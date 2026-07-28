import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

from authlib.integrations.requests_client import OAuth2Session
from pydantic import Field, SecretStr, field_validator
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin, EnvConfigMixin
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    dataset_urn_to_key,
    make_dashboard_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
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
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes, DatasetSubTypes
from datahub.ingestion.source.sac.data_export_metadata import parse_data_export_metadata
from datahub.ingestion.source.sac.sac_common import (
    ImportDataModelColumn,
    Resource,
    ResourceModel,
)
from datahub.ingestion.source.sap_common.models import EdmxParseResult
from datahub.ingestion.source.state.stale_entity_removal_handler import (
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
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
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

# SAP Analytics Cloud serializes dates in OData verbose-JSON as "/Date(<ms-since-epoch>[±<offset>])/".
_SAC_JSON_DATE_PATTERN = re.compile(r"^/Date\((?P<ms>-?\d+)(?P<offset>[+-]\d+)?\)/$")

# SAP Datasphere is surfaced to SAC as a live "Data Warehouse Cloud" (DWC) connection.
# DWC models carry an empty externalId, so their upstream urn is built from the model
# name plus the per-connection datasphere_space rather than a parsed external id.
_DWC_SYSTEM_TYPE = "DWC"
_DATASPHERE_PLATFORM = "sap-datasphere"


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

    datasphere_space: Optional[str] = Field(
        default=None,
        description=(
            "For SAP Datasphere ('DWC') connections only: the Datasphere space id that "
            "backs this connection (e.g. `bdap_sac`). SAC does not expose the space for "
            "Datasphere-backed live models, so it must be supplied here to build the "
            "upstream sap-datasphere dataset urn (`<space>.<model_name>`). Leave unset "
            "for non-Datasphere connections."
        ),
    )

    convert_urns_to_lowercase: bool = Field(
        default=True,
        description=(
            "Whether to lower-case identifiers when constructing the upstream dataset "
            "urn for this connection. Must match the `convert_urns_to_lowercase` setting "
            "used by the corresponding upstream connector recipe so the urns stitch. "
            "Currently applied to SAP Datasphere ('DWC') upstreams only; BW/HANA "
            "upstreams preserve case as before. Defaults to True (matching the SAP "
            "Datasphere connector default)."
        ),
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

    ingest_acquired_data_model_schema_metadata: bool = Field(
        default=True,
        description=(
            "Controls whether schema metadata of acquired (non-import) Data Models is ingested "
            "via the Data Export Service. Live Data Models keep their schema in the source system "
            "and are skipped. Ingesting this schema adds one metadata request per acquired model."
        ),
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

    resolve_datasphere_lineage: bool = Field(
        default=True,
        description=(
            "For SAC Live Data Models backed by SAP Datasphere (Data Warehouse Cloud / "
            "'DWC' connections), emit upstream lineage to the backing SAP Datasphere "
            "dataset. The Datasphere object's technical name is derived from the SAC "
            "model name; the Datasphere space is not exposed by SAC and must be supplied "
            "via `connection_mapping.<connection_id>.datasphere_space`. The upstream urn "
            "is built deterministically (`<space>.<model_name>`) with no DataHub graph "
            "lookup. Models on connections without a configured `datasphere_space` are "
            "skipped with a warning."
        ),
    )

    resolve_datasphere_column_lineage: bool = Field(
        default=True,
        description=(
            "In addition to table-level DWC lineage, emit column-level lineage to the "
            "backing SAP Datasphere dataset. SAC does not expose columns for Live Data "
            "Models, so the field list is resolved from the upstream Datasphere dataset's "
            "schema in DataHub (requires a `datahub_api`/graph connection) and mirrored "
            "onto the SAC dataset, since the live model is a passthrough. Falls back to "
            "table-level lineage when the graph or upstream schema is unavailable. "
            "No effect unless `resolve_datasphere_lineage` is also enabled."
        ),
    )

    @field_validator("tenant_url", "token_url", mode="after")
    @classmethod
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)


@dataclass
class SACSourceReport(StaleEntityRemovalSourceReport):
    acquired_model_schema_resolved: int = 0
    acquired_model_schema_skipped_live: int = 0
    acquired_model_schema_failed: int = 0
    # SAC Live Data Models backed by SAP Datasphere (DWC connections).
    dwc_models_scanned: int = 0
    dwc_lineage_resolved: int = 0
    dwc_lineage_unresolved: int = 0
    dwc_lineage_skipped_no_space: int = 0
    dwc_column_lineage_resolved: int = 0
    dwc_column_lineage_unresolved: int = 0


@platform_name("SAP Analytics Cloud", id="sac")
@config_class(SACSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default (only for Live Data Models)",
)
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Enabled by default (for Import Data Models and acquired Data Models)",
)
class SACSource(StatefulIngestionSourceBase, TestableSource):
    config: SACSourceConfig
    report: SACSourceReport
    platform = "sac"

    session: OAuth2Session

    ingested_dataset_entities: Set[str]
    ingested_upstream_dataset_keys: Set[str]

    def __init__(self, config: SACSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = SACSourceReport()

        self.ingested_dataset_entities = set()
        self.ingested_upstream_dataset_keys = set()

        self.session = SACSource.get_sac_connection(self.config)

    def close(self) -> None:
        self.session.close()
        super().close()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SACSource":
        config = SACSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()

        try:
            config = SACSourceConfig.model_validate(config_dict)

            session = SACSource.get_sac_connection(config)

            # test the Resources API and the Data Import Service separately here, because the Data
            # Import Service requires specific properties when configuring the OAuth client
            response = session.get(
                url=f"{config.tenant_url}/api/v1/Resources",
                params={"$format": "json", "$top": "1"},
            )
            response.raise_for_status()

            response = session.get(url=f"{config.tenant_url}/api/v1/dataimport/models")
            response.raise_for_status()

            session.close()

            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"{e}"
            )

        return test_report

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
                description=resource.description
                if resource.description is not None
                else "",
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

        if (
            not model.is_import
            and model.system_type is None
            and self.config.ingest_acquired_data_model_schema_metadata
        ):
            yield from self._emit_acquired_model_schema(dataset_urn, model)

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
                self.report.warning(
                    message="Unknown upstream dataset for model",
                    context=f"{model.namespace}:{model.model_id} (external_id={model.external_id})",
                    log=False,
                )
        elif model.system_type == _DWC_SYSTEM_TYPE:
            # DWC is a known type; when resolution is disabled we skip it quietly rather
            # than falling through to the "Unknown system type" warning below.
            if self.config.resolve_datasphere_lineage:
                self.report.dwc_models_scanned += 1
                datasphere_upstream_urn = self._resolve_datasphere_upstream(model)
                if datasphere_upstream_urn is not None:
                    self.report.dwc_lineage_resolved += 1
                    fine_grained, schema_workunit = (
                        self._resolve_datasphere_column_lineage(
                            dataset_urn, datasphere_upstream_urn
                        )
                    )
                    if schema_workunit is not None:
                        yield schema_workunit
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=UpstreamLineageClass(
                            upstreams=[
                                UpstreamClass(
                                    dataset=datasphere_upstream_urn,
                                    type=DatasetLineageTypeClass.COPY,
                                ),
                            ],
                            fineGrainedLineages=fine_grained or None,
                        ),
                    ).as_workunit()
        elif model.system_type is not None:
            self.report.warning(
                message="Unknown system type for model",
                context=f"{model.namespace}:{model.model_id} (external_id={model.external_id}, system_type={model.system_type})",
                log=False,
            )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(
                removed=False,
            ),
        )

        yield mcp.as_workunit()

        if (
            model.connection_id
            and model.system_type
            and (model.external_id or model.system_type == _DWC_SYSTEM_TYPE)
        ):
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
    ) -> OAuth2Session:
        session = OAuth2Session(
            client_id=config.client_id,
            client_secret=config.client_secret.get_secret_value(),
            token_endpoint=config.token_url,
            token_endpoint_auth_method="client_secret_post",
            grant_type="client_credentials",
        )

        retries = 3
        backoff_factor = 10

        # The Resources and Data Import Service APIs of SAP Analytics Cloud can be somewhat unstable, occasionally
        # returning HTTP errors for some requests, even though the APIs are generally operational. Therefore, we must
        # retry these requests to increase the likelihood that the ingestion is successful. For the same reason we
        # should also retry requests that receive a 401 HTTP status; however, this status also legitimately indicates
        # that the provided OAuth credentials are invalid or that the OAuth client does not have the correct
        # permissions assigned, therefore requests that receive a 401 HTTP status must not be retried.
        status_forcelist = (400, 500, 503)

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

        return session

    def _query_odata_entities(
        self, path: str, select: str, filter: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        # We query the OData endpoints directly instead of going through a metadata-driven OData
        # client. The "Resources" data endpoints are stable across SAC tenant generations, whereas
        # the $metadata document is not: newer (CAP-based) tenants no longer advertise the
        # "Resources" EntitySet there (it is replaced by a non-queryable "*_INDEX" catalog), which
        # would break any client that resolves endpoints from $metadata. See ING-2650.
        query: Dict[str, str] = {"$format": "json", "$select": select}
        if filter is not None:
            query["$filter"] = filter

        url: Optional[str] = f"{self.config.tenant_url}/api/v1/{path}"
        params: Optional[Dict[str, str]] = query

        while url is not None:
            response = self.session.get(url=url, params=params)
            response.raise_for_status()

            # OData verbose JSON always wraps the payload in a top-level "d"; a missing key means an
            # unexpected response, which we want to surface rather than silently ingest nothing.
            payload = response.json()["d"]
            if isinstance(payload, dict):
                yield from payload.get("results", [])
                # follow server-driven paging; "__next" is an absolute URL with the query baked in
                url = payload.get("__next")
            else:
                yield from payload
                url = None

            params = None

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

        for entity in self._query_odata_entities(
            "Resources", select=select, filter=filter
        ):
            resource_id: str = entity["resourceId"]
            entity_name = entity.get("name")
            name: str = entity_name.strip() if entity_name is not None else resource_id

            if not self.config.resource_id_pattern.allowed(
                resource_id
            ) or not self.config.resource_name_pattern.allowed(name):
                continue

            ancestor_path: Optional[str] = None

            ancestor_path_raw = entity.get("ancestorPath")
            if ancestor_path_raw:
                try:
                    ancestors = json.loads(ancestor_path_raw)
                    ancestor_path = "/".join(
                        ancestor.replace("/", "%2F") for ancestor in ancestors
                    )
                except json.JSONDecodeError:
                    pass

            if ancestor_path and not self.config.folder_pattern.allowed(ancestor_path):
                continue

            resource_models: Set[ResourceModel] = set()

            models_select = (
                "modelId,name,description,externalId,connectionId,systemType"
            )

            # OData string keys escape a single quote by doubling it
            escaped_resource_id = resource_id.replace("'", "''")
            for nav_entity in self._query_odata_entities(
                f"Resources('{escaped_resource_id}')/resourceModels",
                select=models_select,
            ):
                # the model id can have a different structure, commonly all model ids have a namespace (the part before the colon) and the model id itself
                # t.4.sap.fpa.services.userFriendlyPerfLog:ACTIVITY_LOG is a builtin model without a possiblity to get more metadata about the model
                # t.4.YV67EM4QBRU035A7TVKERZ786N:YV67EM4QBRU035A7TVKERZ786N is a model id where the model id itself also appears as part of the namespace
                # t.4:C76tt2j402o1e69wnvrwfcl79c is a model id without the model id itself as part of the namespace
                model_id: str = nav_entity["modelId"]
                namespace, _, model_id = model_id.partition(":")

                nav_name = nav_entity.get("name")
                nav_description = nav_entity.get("description")

                resource_models.add(
                    ResourceModel(
                        namespace=namespace,
                        model_id=model_id,
                        name=nav_name.strip()
                        if nav_name is not None
                        else f"{namespace}:{model_id}",
                        description=nav_description.strip()
                        if nav_description is not None
                        else None,
                        system_type=nav_entity.get("systemType"),  # BW or HANA
                        connection_id=nav_entity.get("connectionId"),
                        external_id=nav_entity.get(
                            "externalId"
                        ),  # query:[][][query] or view:[schema][schema.namespace][view]
                        is_import=model_id in import_data_model_ids,
                    )
                )

            created_by: Optional[str] = entity.get("createdBy")
            if created_by in ("SYSTEM", "$DELETED_USER$"):
                created_by = None

            modified_by: Optional[str] = entity.get("modifiedBy")
            if modified_by in ("SYSTEM", "$DELETED_USER$"):
                modified_by = None

            description = entity.get("description")

            yield Resource(
                resource_id=resource_id,
                resource_type=entity["resourceType"],
                resource_subtype=entity["resourceSubtype"],
                story_id=entity["storyId"],
                name=name,
                description=description.strip() if description is not None else None,
                created_time=_parse_sac_datetime(entity["createdTime"]),
                created_by=created_by,
                modified_time=_parse_sac_datetime(entity["modifiedTime"]),
                modified_by=modified_by,
                open_url=entity["openURL"],
                ancestor_path=ancestor_path,
                is_mobile=entity["isMobile"],
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
                    description=(
                        column["descriptionName"].strip()
                        if column.get("descriptionName") is not None
                        else None
                    ),
                    property_type=column["propertyType"],
                    data_type=column["columnDataType"],
                    max_length=column.get("maxLength"),
                    precision=column.get("precision"),
                    scale=column.get("scale"),
                    is_key=column["isKey"],
                )
            )

        return columns

    def _emit_acquired_model_schema(
        self, dataset_urn: str, model: ResourceModel
    ) -> Iterable[MetadataWorkUnit]:
        parse_result = self._get_data_export_schema(model)
        if (
            parse_result is None
            or parse_result.error is not None
            or not parse_result.fields
        ):
            return

        self.report.acquired_model_schema_resolved += 1
        primary_fields = [f.fieldPath for f in parse_result.fields if f.isPartOfKey]

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SchemaMetadataClass(
                schemaName=model.model_id,
                platform=make_data_platform_urn(self.platform),
                version=0,
                hash="",
                platformSchema=SchemalessClass(),
                fields=parse_result.fields,
                primaryKeys=primary_fields,
            ),
        ).as_workunit()

    def _get_data_export_schema(
        self, model: ResourceModel
    ) -> Optional[EdmxParseResult]:
        # The Data Export Service returns EDMX for acquired models but 412
        # ("Requested ProviderID is not supported") for Live Data Models, whose
        # schema lives in the source system. Treat 412 as an expected skip.
        try:
            response = self.session.get(
                url=f"{self.config.tenant_url}/api/v1/dataexport/providers/sac/{model.model_id}/$metadata",
                headers={"Accept": "application/xml"},
            )
        except RequestException as e:
            # A per-model transport failure (e.g. the retry adapter exhausting on
            # repeated 5xx) must not abort the whole run: the model is still emitted,
            # just without a DES-derived schema.
            self.report.acquired_model_schema_failed += 1
            self.report.warning(
                title="Failed to fetch acquired model schema",
                message="The Data Export Service metadata request failed; the model is emitted without a schema.",
                context=f"{model.namespace}:{model.model_id}: {e}",
            )
            return None
        if response.status_code == 412:
            self.report.acquired_model_schema_skipped_live += 1
            return None
        if not response.ok:
            self.report.acquired_model_schema_failed += 1
            self.report.warning(
                title="Failed to fetch acquired model schema",
                message="The Data Export Service metadata request failed; the model is emitted without a schema.",
                context=f"{model.namespace}:{model.model_id} (status={response.status_code})",
            )
            return None

        parse_result = parse_data_export_metadata(response.text)
        if parse_result.error is not None:
            self.report.acquired_model_schema_failed += 1
            self.report.warning(
                title="Failed to parse acquired model schema",
                message="The Data Export Service metadata could not be parsed; the model is emitted without a schema.",
                context=f"{model.namespace}:{model.model_id}: {parse_result.error}",
            )
        return parse_result

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

    def _resolve_datasphere_upstream(self, model: ResourceModel) -> Optional[str]:
        # SAC exposes the Datasphere object's technical name (the model name) but not its
        # space, so the space comes from connection_mapping and the urn is built directly.
        object_name = (model.name or "").strip()
        if not object_name:
            self.report.dwc_lineage_unresolved += 1
            self.report.warning(
                title="SAP Datasphere model has no name",
                message=(
                    "Cannot link a DWC-backed SAC model to its SAP Datasphere source "
                    "because the model has no name to derive the object from."
                ),
                context=f"{model.connection_id}: {model.namespace}:{model.model_id}",
            )
            return None

        connection = self.config.connection_mapping.get(model.connection_id or "")
        if connection is None or not connection.datasphere_space:
            self.report.dwc_lineage_skipped_no_space += 1
            self.report.warning(
                title="SAP Datasphere space not configured",
                message=(
                    "Cannot link a DWC-backed SAC model to its SAP Datasphere source "
                    "because no datasphere_space is set for the connection. Add "
                    "connection_mapping.<connection_id>.datasphere_space (the Datasphere "
                    "space id), or set resolve_datasphere_lineage=false to silence this."
                ),
                context=f"{model.connection_id}: {model.name}",
            )
            return None

        platform = connection.platform or _DATASPHERE_PLATFORM

        # Match the Datasphere connector's urn casing so the upstream stitches.
        dataset_name = f"{connection.datasphere_space}.{object_name}"
        if connection.convert_urns_to_lowercase:
            dataset_name = dataset_name.lower()

        return make_dataset_urn_with_platform_instance(
            platform=platform,
            name=dataset_name,
            platform_instance=connection.platform_instance,
            env=connection.env,
        )

    def _resolve_datasphere_column_lineage(
        self, dataset_urn: str, upstream_urn: str
    ) -> Tuple[List[FineGrainedLineageClass], Optional[MetadataWorkUnit]]:
        # SAC exposes no columns for Live Data Models, so the field list is taken from
        # the upstream SAP Datasphere dataset's schema in the DataHub graph. The live
        # model is a passthrough, so that schema is mirrored onto the SAC dataset and
        # each field is mapped to itself. Best-effort: an unavailable graph or upstream
        # schema degrades to table-level lineage only.
        if not self.config.resolve_datasphere_column_lineage:
            return [], None

        graph = self.ctx.graph
        if graph is None:
            self.report.dwc_column_lineage_unresolved += 1
            self.report.warning(
                title="SAP Datasphere column lineage needs a DataHub graph",
                message=(
                    "Column-level lineage for DWC-backed models resolves the upstream "
                    "schema from DataHub, which requires a datahub_api/graph connection. "
                    "Emitting table-level lineage only; set "
                    "resolve_datasphere_column_lineage=false to silence this."
                ),
                context=upstream_urn,
            )
            return [], None

        upstream_schema = graph.get_aspect(upstream_urn, SchemaMetadataClass)
        if upstream_schema is None or not upstream_schema.fields:
            self.report.dwc_column_lineage_unresolved += 1
            self.report.warning(
                title="SAP Datasphere upstream schema not found",
                message=(
                    "The upstream SAP Datasphere dataset has no schema in DataHub "
                    "(ingest SAP Datasphere first). Emitting table-level lineage only."
                ),
                context=upstream_urn,
            )
            return [], None

        # Mirror the upstream schema onto the SAC dataset so downstream field urns
        # resolve, and map each field to its identical upstream counterpart.
        schema_workunit = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SchemaMetadataClass(
                schemaName=upstream_schema.schemaName,
                platform=make_data_platform_urn(self.platform),
                version=0,
                hash="",
                platformSchema=SchemalessClass(),
                fields=upstream_schema.fields,
                primaryKeys=upstream_schema.primaryKeys,
            ),
        ).as_workunit()

        fine_grained = [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[make_schema_field_urn(upstream_urn, field.fieldPath)],
                downstreams=[make_schema_field_urn(dataset_urn, field.fieldPath)],
            )
            for field in upstream_schema.fields
        ]
        self.report.dwc_column_lineage_resolved += 1
        return fine_grained, schema_workunit

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
                self.report.warning(
                    message="Unknown data type found",
                    context=f"data_type={column.data_type}",
                    log=False,
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


def _parse_sac_datetime(value: str) -> datetime:
    match = _SAC_JSON_DATE_PATTERN.match(value)
    if match is None:
        raise ValueError(f"Unexpected SAP Analytics Cloud date format: {value!r}")

    # The millisecond value is an absolute instant (epoch-relative); an optional ±offset only
    # affects the displayed wall-clock time, not the instant, so it does not change the UTC value.
    return datetime.fromtimestamp(int(match.group("ms")) / 1000, tz=timezone.utc)
