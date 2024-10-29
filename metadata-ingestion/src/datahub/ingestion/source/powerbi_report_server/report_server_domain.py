from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator

from datahub.ingestion.source.powerbi_report_server.constants import (
    RelationshipDirection,
)
from datahub.metadata.schema_classes import OwnerClass


class CatalogItem(BaseModel):
    id: str = Field(alias="Id")
    name: str = Field(alias="Name")
    description: Optional[str] = Field(None, alias="Description")
    path: str = Field(alias="Path")
    type: Any = Field(None, alias="Type")
    hidden: bool = Field(alias="Hidden")
    size: int = Field(alias="Size")
    modified_by: Optional[str] = Field(None, alias="ModifiedBy")
    modified_date: Optional[datetime] = Field(None, alias="ModifiedDate")
    created_by: Optional[str] = Field(None, alias="CreatedBy")
    created_date: Optional[datetime] = Field(None, alias="CreatedDate")
    parent_folder_id: Optional[str] = Field(None, alias="ParentFolderId")
    content_type: Optional[str] = Field(None, alias="ContentType")
    content: str = Field(alias="Content")
    is_favorite: bool = Field(alias="IsFavorite")
    user_info: Any = Field(None, alias="UserInfo")
    display_name: Optional[str] = Field(None, alias="DisplayName")
    has_data_sources: bool = Field(default=False, alias="HasDataSources")
    data_sources: Optional[List["DataSource"]] = Field(
        default_factory=list, alias="DataSources"
    )

    @validator("display_name", always=True)
    def validate_diplay_name(cls, value, values):  # noqa: N805
        if values["created_by"]:
            return values["created_by"].split("\\")[-1]
        return ""

    def get_urn_part(self):
        return f"reports.{self.id}"

    def get_web_url(self, base_reports_url: str) -> str:
        return f"{base_reports_url}powerbi{self.path}"

    def get_browse_path(
        self, base_folder: str, workspace: str, env: str, report_directory: str
    ) -> str:
        return "/{}/{}/{}/{}{}".format(
            base_folder, env.lower(), workspace, report_directory, self.path
        )


class DataSet(CatalogItem):
    has_parameters: bool = Field(alias="HasParameters")
    query_execution_time_out: int = Field(alias="QueryExecutionTimeOut")

    def get_urn_part(self):
        return f"datasets.{self.id}"

    def __members(self):
        return (self.id,)

    def __eq__(self, instance):
        return (
            isinstance(instance, DataSet) and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


class DataModelDataSource(BaseModel):
    auth_type: Optional[str] = Field(None, alias="AuthType")
    supported_auth_types: List[Optional[str]] = Field(alias="SupportedAuthTypes")
    kind: str = Field(alias="Kind")
    model_connection_name: str = Field(alias="ModelConnectionName")
    secret: str = Field(alias="Secret")
    type: Optional[str] = Field(None, alias="Type")
    username: str = Field(alias="Username")


class CredentialsByUser(BaseModel):
    display_text: str = Field(alias="DisplayText")
    use_as_windows_credentials: bool = Field(alias="UseAsWindowsCredentials")


class CredentialsInServer(BaseModel):
    username: str = Field(alias="UserName")
    password: str = Field(alias="Password")
    use_as_windows_credentials: bool = Field(alias="UseAsWindowsCredentials")
    impersonate_authenticated_user: bool = Field(alias="ImpersonateAuthenticatedUser")


class ParameterValue(BaseModel):
    name: str = Field(alias="Name")
    value: str = Field(alias="Value")
    is_value_field_reference: str = Field(alias="IsValueFieldReference")


class ExtensionSettings(BaseModel):
    extension: str = Field(alias="Extension")
    parameter_values: ParameterValue = Field(alias="ParameterValues")


class Subscription(BaseModel):
    id: str = Field(alias="Id")
    owner: str = Field(alias="Owner")
    is_data_driven: bool = Field(alias="IsDataDriven")
    description: str = Field(alias="Description")
    report: str = Field(alias="Report")
    is_active: bool = Field(alias="IsActive")
    event_type: str = Field(alias="EventType")
    schedule_description: str = Field(alias="ScheduleDescription")
    last_run_time: datetime = Field(alias="LastRunTime")
    last_status: str = Field(alias="LastStatus")
    extension_settings: ExtensionSettings = Field(alias="ExtensionSettings")
    delivery_extension: str = Field(alias="DeliveryExtension")
    localized_delivery_extension_name: str = Field(
        alias="LocalizedDeliveryExtensionName"
    )
    modified_by: str = Field(alias="ModifiedBy")
    modified_date: datetime = Field(alias="ModifiedDate")
    parameter_values: ParameterValue = Field(alias="ParameterValues")


class MetaData(BaseModel):
    is_relational: bool


class DataSource(CatalogItem):
    name: str = Field(default="", alias="Name")
    path: str = Field(default="", alias="Path")
    is_enabled: bool = Field(alias="IsEnabled")
    connection_string: str = Field(alias="ConnectionString")
    data_model_data_source: Optional[DataModelDataSource] = Field(
        None, alias="DataModelDataSource"
    )
    data_source_sub_type: Optional[str] = Field(None, alias="DataSourceSubType")
    data_source_type: Optional[str] = Field(None, alias="DataSourceType")
    is_original_connection_string_expression_based: bool = Field(
        alias="IsOriginalConnectionStringExpressionBased"
    )
    is_connection_string_overridden: bool = Field(alias="IsConnectionStringOverridden")
    credentials_by_user: Optional[CredentialsByUser] = Field(
        None, alias="CredentialsByUser"
    )
    credentials_in_server: Optional[CredentialsInServer] = Field(
        None, alias="CredentialsInServer"
    )
    is_reference: bool = Field(alias="IsReference")
    subscriptions: Optional[Subscription] = Field(None, alias="Subscriptions")
    meta_data: Optional[MetaData] = Field(None, alias="MetaData")

    def __members(self):
        return (self.id,)

    def __eq__(self, instance):
        return (
            isinstance(instance, DataSource)
            and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


class Comment(BaseModel):
    id: str = Field(alias="Id")
    item_id: str = Field(alias="ItemId")
    username: str = Field(alias="UserName")
    thread_id: str = Field(alias="ThreadId")
    attachment_path: str = Field(alias="AttachmentPath")
    text: str = Field(alias="Text")
    created_date: datetime = Field(alias="CreatedDate")
    modified_date: datetime = Field(alias="ModifiedDate")


class ExcelWorkbook(CatalogItem):
    comments: Comment = Field(alias="Comments")


class Role(BaseModel):
    name: str = Field(alias="Name")
    description: str = Field(alias="Description")


class SystemPolicies(BaseModel):
    group_user_name: str = Field(alias="GroupUserName")
    roles: List[Role] = Field(alias="Roles")


class Report(CatalogItem):
    has_data_sources: bool = Field(alias="HasDataSources")
    has_shared_data_sets: bool = Field(alias="HasSharedDataSets")
    has_parameters: bool = Field(alias="HasParameters")


class PowerBiReport(CatalogItem):
    has_data_sources: bool = Field(alias="HasDataSources")


class Extension(BaseModel):
    extension_type: str = Field(alias="ExtensionType")
    name: str = Field(alias="Name")
    localized_name: str = Field(alias="LocalizedName")
    Visible: bool = Field(alias="Visible")


class Folder(CatalogItem):
    """Folder"""


class DrillThroughTarget(BaseModel):
    drill_through_target_type: str = Field(alias="DrillThroughTargetType")


class Value(BaseModel):
    value: str = Field(alias="Value")
    goal: int = Field(alias="Goal")
    status: int = Field(alias="Status")
    trend_set: List[int] = Field(alias="TrendSet")


class Kpi(CatalogItem):
    value_format: str = Field(alias="ValueFormat")
    visualization: str = Field(alias="Visualization")
    drill_through_target: DrillThroughTarget = Field(alias="DrillThroughTarget")
    currency: str = Field(alias="Currency")
    values: Value = Field(alias="Values")
    data: Dict[str, str] = Field(alias="Data")


class LinkedReport(CatalogItem):
    has_parameters: bool = Field(alias="HasParameters")
    link: str = Field(alias="Link")


class Manifest(BaseModel):
    resources: List[Dict[str, List]] = Field(alias="Resources")


class PowerBIReport(CatalogItem):
    has_data_sources: bool = Field(alias="HasDataSources")


class Resources(CatalogItem):
    """Resources"""


class System(BaseModel):
    report_server_absolute_url: str = Field(alias="ReportServerAbsoluteUrl")
    report_server_relative_url: str = Field(alias="ReportServerRelativeUrl")
    web_portal_relative_url: str = Field(alias="WebPortalRelativeUrl")
    product_name: str = Field(alias="ProductName")
    product_version: str = Field(alias="ProductVersion")
    product_type: str = Field(alias="ProductType")
    time_zone: str = Field(alias="TimeZone")


class Owner(BaseModel):
    owner: str
    type: str

    def __members(self):
        return self.owner, self.type

    def __eq__(self, instance):
        return isinstance(instance, Owner) and self.__members() == instance.__members()

    def __hash__(self):
        return hash(self.__members())


class CorpUserEditableInfo(BaseModel):
    display_name: str = Field(alias="displayName")
    title: str
    about_me: Optional[str] = Field(None, alias="aboutMe")
    teams: Optional[List[str]] = None
    skills: Optional[List[str]] = None
    picture_link: Optional[str] = Field(None, alias="pictureLink")


class CorpUserEditableProperties(CorpUserEditableInfo):
    slack: Optional[str] = None
    phone: Optional[str] = None
    email: str


class CorpUserStatus(BaseModel):
    active: bool


class GlobalTags(BaseModel):
    tags: List[str]


class EntityRelationship(BaseModel):
    type: str
    direction: RelationshipDirection
    entity: str
    created: datetime


class EntityRelationshipsResult(BaseModel):
    start: int
    count: int
    total: int
    relationships: Optional[EntityRelationship] = None


class CorpUserProperties(BaseModel):
    active: bool
    display_name: str = Field(alias="displayName")
    email: str
    title: Optional[str] = None
    manager: Optional["CorpUser"] = None
    department_id: Optional[int] = Field(None, alias="departmentId")
    department_name: Optional[str] = Field(None, alias="departmentName")
    first_name: Optional[str] = Field(None, alias="firstName")
    last_name: Optional[str] = Field(None, alias="lastName")
    full_name: Optional[str] = Field(None, alias="fullName")
    country_code: Optional[str] = Field(None, alias="countryCode")


class CorpUser(BaseModel):
    urn: str
    type: str
    username: str
    properties: CorpUserProperties
    editable_properties: Optional[CorpUserEditableProperties] = Field(
        None, alias="editableProperties"
    )
    status: Optional[CorpUserStatus] = None
    tags: Optional[GlobalTags] = None
    relationships: Optional[EntityRelationshipsResult] = None
    editableInfo: Optional[CorpUserEditableInfo] = Field(None, alias="editableInfo")
    global_tags: Optional[GlobalTags] = Field(None, alias="globalTags")

    def get_urn_part(self):
        return f"{self.username}"

    def __members(self):
        return (self.username,)

    def __eq__(self, instance):
        return (
            isinstance(instance, CorpUser) and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


class OwnershipData(BaseModel):
    existing_owners: Optional[List[OwnerClass]] = []
    owner_to_add: Optional[CorpUser] = None

    class Config:
        arbitrary_types_allowed = True


CatalogItem.update_forward_refs()
CorpUserProperties.update_forward_refs()
