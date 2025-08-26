from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Iterable, List, Optional

import pydantic
from pydantic import BaseModel, validator


class DataHubProductGlossary:
    """
    This class defines the DataHub Product Glossary.
    Eventually we can move it to a real business glossary implementation in
    DataHub
    For now, we will have classes (representing product concerns) - like
    GlossaryNodes
    Within those classes, we will have terms (representing product features,
    issue categories etc.). These would be the glossary terms conceptually.

    Any time we change this class (or internal classes), we should increment
    the version number to ensure that the changes are accounted for in the
    summarization process.

    """

    VERSION = "0.0.1rc2"

    class IntegrationPlatform(str, Enum):
        ADLS_GEN1 = "adlsGen1"
        ADLS_GEN2 = "adlsGen2"
        AIRFLOW = "airflow"
        AMAZON_S3 = "s3"
        ATHENA = "athena"
        BIGQUERY = "bigquery"
        CLICKHOUSE = "clickhouse"
        DATABRICKS = "databricks"
        DBT = "dbt"
        DELTA_LAKE = "delta-lake"
        DRUID = "druid"
        DYNAMODB = "dynamodb"
        ELASTICSEARCH = "elasticsearch"
        FIVETRAN = "fivetran"
        GLUE = "glue"
        GOOGLE_CLOUD_STORAGE = "gcs"
        GREAT_EXPECTATIONS = "great-expectations"
        HIVE = "hive"
        KAFKA = "kafka"
        LOOKER = "looker"
        METABASE = "metabase"
        MLFLOW = "mlflow"
        MONGODB = "mongodb"
        MYSQL = "mysql"
        NIFI = "nifi"
        ORACLE = "oracle"
        POSTGRES = "postgres"
        POWERBI = "powerbi"
        PRESTO = "presto"
        PULSAR = "pulsar"
        REDASH = "redash"
        REDSHIFT = "redshift"
        SAGEMAKER = "sagemaker"
        SALESFORCE = "salesforce"
        SNOWFLAKE = "snowflake"
        SPARK = "spark"
        SQL_SERVER = "mssql"
        SUPERSET = "superset"
        TABLEAU = "tableau"
        TRINO = "trino"
        PROTOBUF = "protobuf"
        AVRO = "avro"
        THRIFT = "thrift"
        HEX = "hex"
        ACRYL = "acryl"
        DATAHUB = "datahub"
        KAFKA_CONNECT = "kafka-connect"
        OKTA = "okta"
        DATADOG = "datadog"
        MODE = "mode"

    @dataclass
    class FeatureArea:
        name: str
        description: str
        examples: List[str]

    FEATURE_AREAS = [
        FeatureArea(
            name="SEARCH",
            description="Functionality related to searching and finding information within the system.",
            examples=[
                "Improving search result relevance",
                "Adding filters to search interface",
                "Implementing autocomplete in search bar",
            ],
        ),
        FeatureArea(
            name="HOMEPAGE",
            description="Issues related to the main landing page or dashboard of the application.",
            examples=[
                "Redesigning homepage layout",
                "Adding customizable widgets to homepage",
                "Fixing broken links on homepage",
            ],
        ),
        FeatureArea(
            name="LINEAGE",
            description="Features dealing with data lineage, tracing data origins and transformations.",
            examples=[
                "Visualizing data lineage graphs",
                "Adding ability to track column-level lineage",
                "Implementing lineage across different systems",
            ],
        ),
        FeatureArea(
            name="IMPACT_ANALYSIS",
            description="Tools and features for assessing the impact of changes in the data ecosystem.",
            examples=[
                "Creating impact analysis reports",
                "Implementing real-time impact notifications",
                "Adding what-if scenarios for impact analysis",
            ],
        ),
        FeatureArea(
            name="API",
            description="Issues related to the application programming interface.",
            examples=[
                "Adding new API endpoints",
                "Improving API documentation",
                "Implementing rate limiting for API calls",
            ],
        ),
        FeatureArea(
            name="INGESTION",
            description="Features related to data ingestion processes and pipelines.",
            examples=[
                "Adding support for new data sources",
                "Optimizing ingestion performance",
                "Implementing data validation during ingestion",
            ],
        ),
        FeatureArea(
            name="PROFILING",
            description="Tools and features for analyzing and understanding data characteristics.",
            examples=[
                "Generating data quality reports",
                "Implementing automated data profiling",
                "Adding custom profiling rules",
            ],
        ),
        FeatureArea(
            name="BUSINESS_GLOSSARY",
            description="Features related to defining and managing business terms and definitions.",
            examples=[
                "Creating a hierarchical glossary structure",
                "Implementing term relationships and cross-references",
                "Adding approval workflow for new glossary terms",
            ],
        ),
        FeatureArea(
            name="DATA_PRODUCTS",
            description="Features related to packaging and managing data as products.",
            examples=[
                "Creating data product templates",
                "Implementing version control for data products",
                "Adding data product discovery features",
            ],
        ),
        FeatureArea(
            name="DOMAINS",
            description="Features for organizing and managing data domains or subject areas.",
            examples=[
                "Implementing domain-based access control",
                "Creating domain hierarchy visualizations",
                "Adding domain-specific metadata fields",
            ],
        ),
        FeatureArea(
            name="ANALYTICS",
            description="Features related to the analytics section of the product.",
            examples=[
                "Measuring ownership coverage over time",
                "Understanding glossary term usage patterns",
                "Understanding documentation coverage",
                "Understanding weekly active users",
            ],
        ),
        FeatureArea(
            name="FORMS",
            description="Features related to metadata verification forms and user interfaces for data entry.",
            examples=[
                "Support for creating forms from the UI",
                "Adding support for bulk editing in forms",
                "Support for forms to fill out owners, domains, etc.",
            ],
        ),
        FeatureArea(
            name="STRUCTURED_PROPERTIES",
            description="Features for managing and utilizing structured metadata properties.",
            examples=[
                "Implementing custom property types",
                "Adding bulk editing for structured properties",
                "Creating property inheritance mechanisms",
            ],
        ),
        FeatureArea(
            name="COLUMNS_TAB",
            description="Features specific to the columns view or tab in the application.",
            examples=[
                "Showing the type of each column in the table",
                "Adding column descriptions",
                "Seeing nested fields in complex types",
                "Creating custom column tagging features",
                "Browsing column descriptions",
                "Schema evolution tracking",
                "This looker explore is not loading the columns in Acryl.",
            ],
        ),
        FeatureArea(
            name="OWNERSHIP",
            description="Features related to assigning and managing data ownership.",
            examples=[
                "Implementing ownership transfer workflows",
                "Adding team-based ownership models",
                "Creating ownership audit trails",
                "Supporting multiple owners for a dataset",
                "Supporting custom ownership roles",
            ],
        ),
        FeatureArea(
            name="AUTH",
            description="Features related to authentication and authorization mechanisms.",
            examples=[
                "Implementing single sign-on (SSO)",
                "Adding multi-factor authentication",
                "Creating role-based access control systems",
                "Supporting fine-grained access control policies",
                "OKTA, SAML, LDAP, etc.",
            ],
        ),
        FeatureArea(
            name="METADATA_TESTS",
            description="Features related to metadata quality and validation tests.",
            examples=[
                "Checking for ownership information and setting tags",
                "Auto tiering of assets based on metadata rules",
                "Implementing automated metadata validation",
                "Adding test scheduling and notifications",
                "Tagging assets automatically as gold, silver, bronze",
                "Supporting cost analysis based on metadata",
                "Detecting unused assets based on metadata",
            ],
        ),
        FeatureArea(
            name="PROPAGATION",
            description="Features related to metadata propagation and inheritance.",
            examples=[
                "Implementing metadata inheritance",
                "Adding propagation rules for metadata changes",
                "Supporting custom propagation policies",
                "Supporting propagation of tags and classifications to donwstream assets",
                "Supporting propagation of ownership information",
                "Propagating tags to Snowflake, Bigquery, etc.",
            ],
        ),
    ]

    class IssueCategory(Enum):
        BUG = "bug"
        FEATURE_REQUEST = "feature_request"


class ExternalUser(BaseModel):
    platform: str
    id: str
    name: str
    email: Optional[str] = None
    image: Optional[str] = None


class TicketComment(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    id: Optional[str] = None
    created_at: datetime
    public: bool
    body: str
    platform: str
    author: ExternalUser


# class Conversation(BaseModel):
#     id: str
#     body: str
#     user: ExternalUser
#     created_at: datetime


class LinkedResource(BaseModel):
    platform: str
    url: str
    name: str
    description: Optional[str] = None


class Project(BaseModel):
    class ProjectId(BaseModel):
        platform: str
        id: str

        @validator("id", pre=True)
        def id_must_be_coerced_to_string(cls, v: Any) -> str:
            if not v:
                raise ValueError("id must be present")
            if not isinstance(v, str):
                return str(v)
            return v

    name: str
    type: str
    project_id: ProjectId
    description: Optional[str] = None
    external_url: Optional[str] = None


class Issue(BaseModel):
    class IssueId(BaseModel):
        platform: str
        id: str

        @validator("id", pre=True)
        def id_must_be_coerced_to_string(cls, v: Any) -> str:
            if not v:
                raise ValueError("id must be present")
            if not isinstance(v, str):
                return str(v)
            return v

    project: Optional["Project"] = None
    issue_id: IssueId
    customer_id: str
    status: str = "open"
    subject: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    description: Optional[str] = None
    creator: Optional[ExternalUser] = None
    assignee: Optional[ExternalUser] = None
    other_assignees: Optional[List[ExternalUser]] = pydantic.Field(default_factory=list)
    conversation_list: List[TicketComment] = pydantic.Field(default_factory=list)
    external_url: Optional[str] = None

    feature_area: Optional[DataHubProductGlossary.FeatureArea] = None
    category: Optional[DataHubProductGlossary.IssueCategory] = None
    platforms: List[DataHubProductGlossary.IntegrationPlatform] = pydantic.Field(
        default_factory=list
    )
    linked_resources: List[LinkedResource] = pydantic.Field(default_factory=list)
    last_commented_at: Optional[datetime] = None

    @pydantic.validator("created_at", "updated_at", pre=True)
    def ts_must_be_coerced_to_datetime(cls, v: Any) -> Optional[datetime]:
        if not v:
            return v
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v)
            except Exception:
                return datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
        return v

    @property
    def last_activity(self) -> Optional[datetime]:
        """
        Returns the last activity time on the issue. This can be the last time
        the issue was updated or the last time a comment was made on the issue.
        """
        if self.last_commented_at and self.updated_at:
            return max(self.last_commented_at, self.updated_at)
        return self.updated_at or self.last_commented_at

    def conversations(self) -> Iterable["TicketComment"]:
        return self.conversation_list
