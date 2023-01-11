from typing import Any, List, Optional

from pydantic import BaseModel, Field

from datahub.ingestion.source.powerbi_v2.constants import Constant


class User(BaseModel):
    display_name: Optional[str] = Field(alias="displayName")
    identifier: str = Field(alias="identifier")
    principal_type: str = Field(alias="principalType")
    user_type: Optional[str] = Field(alias="userType")
    graph_id: str = Field(alias="graphId")
    dashboard_user_access_right: Optional[str] = Field(alias="dashboardUserAccessRight")
    dataset_user_access_right: Optional[str] = Field(alias="datasetUserAccessRight")
    report_user_access_right: Optional[str] = Field(alias="reportUserAccessRight")
    group_user_access_right: Optional[str] = Field(alias="groupUserAccessRight")
    datamart_user_access_right: Optional[str] = Field(alias="datamartUserAccessRight")
    email_address: Optional[str] = Field(alias="emailAddress")

    def get_urn_part(self, remove_email_suffix: Optional[bool] = None) -> str:
        if self.email_address:
            if remove_email_suffix:
                return self.email_address.split("@")[0]
            elif remove_email_suffix is False:
                return self.email_address
        return f"users.{self.identifier}"


class Tile(BaseModel):
    id: str
    title: Optional[str]
    sub_title: Optional[str] = Field(alias="subTitle")
    report_id: Optional[str] = Field(alias="reportId")
    dataset_id: Optional[str] = Field(alias="datasetId")

    def get_urn_part(self):
        return f"charts.{self.id}"


class Dashboard(BaseModel):
    entity_type: str = "dashboard"
    id: str
    display_name: str = Field(alias="displayName")
    is_read_only: bool = Field(alias="isReadOnly")
    tiles: List[Tile]
    sensitivity_label: Any = Field(alias="sensitivityLabel")
    users: List[Optional[User]]

    def get_urn_part(self):
        return f"dashboards.{self.id}"

    @staticmethod
    def get_web_url(workspace_id: str, dashboard_id: str) -> str:
        return f"{Constant.WEB_URL_BASE}/{workspace_id}/dashboards/{dashboard_id}"


class Report(BaseModel):
    entity_type: str = "report"
    report_type: str = Field(alias="reportType")
    id: str
    name: str
    dataset_id: Optional[str] = Field(alias="datasetId")
    created_date_time: Optional[str] = Field(alias="createdDateTime")
    modified_date_time: Optional[str] = Field(alias="modifiedDateTime")
    users: Optional[List[Optional[User]]]
    modified_by: Optional[str] = Field(alias="modifiedBy")
    modified_by_id: Optional[str] = Field(alias="modifiedById")
    dataset_workspace_id: Optional[str] = Field(alias="datasetWorkspaceId")
    created_by: Optional[str] = Field(alias="createdBy")
    created_by_id: Optional[str] = Field(alias="createdById")

    def get_urn_part(self):
        return f"reports.{self.id}"

    @staticmethod
    def get_web_url(workspace_id: str, report_id: str) -> str:
        return f"{Constant.WEB_URL_BASE}/{workspace_id}/reports/{report_id}"


class Column(BaseModel):
    name: str
    data_type: str = Field(alias="dataType")
    is_hidden: bool = Field(alias="isHidden")
    column_type: Optional[str] = Field(alias="columnType")
    expression: Optional[str] = None


class Measure(BaseModel):
    name: str
    expression: str
    is_hidden: bool = Field(alias="isHidden")


class TableSource(BaseModel):
    expression: str


class Table(BaseModel):
    name: str = Field(alias="name")
    is_hidden: bool = Field(alias="isHidden")
    columns: List[Column] = Field(alias="columns")
    measures: List[Measure] = Field(alias="measures")
    source: Optional[List[TableSource]] = Field(alias="source")


class DatasourceUsage(BaseModel):
    data_source_instance_id: Optional[str]


class UpstreamDataset(BaseModel):
    targetDatasetId: str
    groupId: str


class Dataset(BaseModel):
    entity_type: str = "powerbi_dataset"
    id: str
    name: str
    tables: List[Table]
    is_effective_identity_required: bool = Field(alias="isEffectiveIdentityRequired")
    is_effective_identity_roles_required: bool = Field(
        alias="isEffectiveIdentityRolesRequired"
    )
    target_storage_mode: str = Field(alias="targetStorageMode")
    created_date: str = Field(alias="createdDate")
    content_provider_type: str = Field(alias="contentProviderType")
    users: List[Optional[User]] = Field(alias="users")
    configured_by: Optional[str] = Field(alias="configuredBy")
    configured_by_id: Optional[str] = Field(alias="configuredById")
    datasource_usages: Optional[List[DatasourceUsage]] = Field(alias="datasourceUsages")
    schema_may_not_be_up_to_date: Optional[bool] = Field(alias="schemaMayNotBeUpToDate")
    upstream_datasets: Optional[List[UpstreamDataset]] = Field(alias="upstreamDatasets")

    # def get_urn_part(self):
    #     return f"datasets.{self.id}"
    @staticmethod
    def get_web_url(workspace_id: str, dataset_id: str) -> str:
        return f"{Constant.WEB_URL_BASE}/{workspace_id}/datasets/{dataset_id}/details"


class Workspace(BaseModel):
    id: str
    name: str
    type: str
    state: str
    is_on_dedicated_capacity: bool = Field(alias="isOnDedicatedCapacity")
    capacity_id: Optional[str] = Field(alias="capacityId")
    dashboards: List[Dashboard]
    reports: List[Report]
    datasets: List[Optional[Dataset]]
    dataflows: List[Any]
    datamarts: List[Any]
    users: List[Optional[User]]


class ConnectionDetails(BaseModel):
    server: Optional[str] = None
    database: Optional[str] = None
    path: Optional[str] = None
    url: Optional[str] = None
    extension_data_source_kind: Optional[str] = Field(alias="extensionDataSourceKind")
    extension_data_source_path: Optional[str] = Field(alias="extensionDataSourcePath")


class DatasourceInstance(BaseModel):
    datasource_type: str = Field(alias="datasourceType")
    connection_details: ConnectionDetails = Field(alias="connectionDetails")
    datasource_id: str = Field(alias="datasourceId")
    gateway_id: str = Field(alias="gatewayId")


class ScanResult(BaseModel):
    workspaces: List[Workspace]
    datasource_instances: Optional[List[DatasourceInstance]] = Field(
        alias="datasourceInstances"
    )
