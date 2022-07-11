from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, validator


class CatalogItem(BaseModel):
    Id: str
    Name: str
    Description: Optional[str]
    Path: str
    Type: Any
    Hidden: bool
    Size: int
    ModifiedBy: Optional[str]
    ModifiedDate: Optional[datetime]
    CreatedBy: Optional[str]
    CreatedDate: Optional[datetime]
    ParentFolderId: Optional[str]
    ContentType: Optional[str]
    Content: str
    IsFavorite: bool
    UserInfo: Any
    DisplayName: Optional[str]
    HasDataSources: bool = False
    DataSources: Optional[List["DataSource"]] = []

    @validator("DisplayName", always=True)
    def validate_diplay_name(cls, value, values):  # noqa: N805
        if values["CreatedBy"]:
            return values["CreatedBy"].split("\\")[-1]
        return ""

    def get_urn_part(self):
        return "reports.{}".format(self.Id)

    def get_web_url(self, base_reports_url: str):
        return "{}powerbi{}".format(base_reports_url, self.Path)

    def get_browse_path(self, base_folder: str, workspace: str):
        return "/{}/{}{}".format(base_folder, workspace, self.Path)


class DataSet(CatalogItem):
    HasParameters: bool
    QueryExecutionTimeOut: int

    def get_urn_part(self):
        return "datasets.{}".format(self.Id)

    def __members(self):
        return (self.Id,)

    def __eq__(self, instance):
        return (
            isinstance(instance, DataSet) and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


class DataModelDataSource(BaseModel):
    AuthType: Optional[str]
    SupportedAuthTypes: List[Optional[str]]
    Kind: str
    ModelConnectionName: str
    Secret: str
    Type: Optional[str]
    Username: str


class CredentialsByUser(BaseModel):
    DisplayText: str
    UseAsWindowsCredentials: bool


class CredentialsInServer(BaseModel):
    UserName: str
    Password: str
    UseAsWindowsCredentials: bool
    ImpersonateAuthenticatedUser: bool


class ParameterValue(BaseModel):
    Name: str
    Value: str
    IsValueFieldReference: str


class ExtensionSettings(BaseModel):
    Extension: str
    ParameterValues: ParameterValue


class Subscription(BaseModel):
    Id: str
    Owner: str
    IsDataDriven: bool
    Description: str
    Report: str
    IsActive: bool
    EventType: str
    ScheduleDescription: str
    LastRunTime: datetime
    LastStatus: str
    ExtensionSettings: ExtensionSettings
    DeliveryExtension: str
    LocalizedDeliveryExtensionName: str
    ModifiedBy: str
    ModifiedDate: datetime
    ParameterValues: ParameterValue


class MetaData(BaseModel):
    is_relational: bool


class DataSource(CatalogItem):
    Name: str = ""
    Path: str = ""
    IsEnabled: bool
    ConnectionString: str
    DataModelDataSource: Optional[DataModelDataSource]
    DataSourceSubType: Optional[str]
    DataSourceType: Optional[str]
    IsOriginalConnectionStringExpressionBased: bool
    IsConnectionStringOverridden: bool
    CredentialsByUser: Optional[CredentialsByUser]
    CredentialsInServer: Optional[CredentialsInServer]
    IsReference: bool
    Subscriptions: Optional[Subscription]
    MetaData: Optional[MetaData]

    def __members(self):
        return (self.Id,)

    def __eq__(self, instance):
        return (
            isinstance(instance, DataSource)
            and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


class Comment(BaseModel):
    Id: str
    ItemId: str
    UserName: str
    ThreadId: str
    AttachmentPath: str
    Text: str
    CreatedDate: datetime
    ModifiedDate: datetime


class ExcelWorkbook(CatalogItem):
    Comments: Comment


class Role(BaseModel):
    Name: str
    Description: str


class SystemPolicies(BaseModel):
    GroupUserName: str
    Roles: List[Role]


class Report(CatalogItem):
    HasDataSources: bool
    HasSharedDataSets: bool
    HasParameters: bool


class PowerBiReport(CatalogItem):
    HasDataSources: bool


class Extension(BaseModel):
    ExtensionType: str
    Name: str
    LocalizedName: str
    Visible: bool


class Folder(CatalogItem):
    """Folder"""


class DrillThroughTarget(BaseModel):
    DrillThroughTargetType: str


class Value(BaseModel):
    Value: str
    Goal: int
    Status: int
    TrendSet: List[int]


class Kpi(CatalogItem):
    ValuerFormat: str
    Visualization: str
    DrillThroughTarget: DrillThroughTarget
    Currency: str
    Values: Value
    Data: Dict[str, str]


class LinkedReport(CatalogItem):
    HasParemeters: bool
    Link: str


class Manifest(BaseModel):
    Resorces: List[Dict[str, List]]


class MobileReport(CatalogItem):
    AllowCaching: bool
    Manifest: Manifest


class PowerBIReport(CatalogItem):
    HasDataSources: bool


class Resources(CatalogItem):
    """Resources"""


class System(BaseModel):
    ReportServerAbsoluteUrl: str
    ReportServerRelativeUrl: str
    WebPortalRelativeUrl: str
    ProductName: str
    ProductVersion: str
    ProductType: str
    TimeZone: str


CatalogItem.update_forward_refs()
