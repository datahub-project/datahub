import logging
import re
from enum import Enum
from typing import Optional

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)


class ItemType(Enum):
    DASHBOARD = "Dashboard"
    DATA_PIPELINE = "DataPipeline"
    DATAMART = "Datamart"
    ENVIRONMENT = "Environment"
    EVENTHOUSE = "Eventhouse"
    EVENTSTREAM = "Eventstream"
    GRAPHQL_API = "GraphQLApi"
    KQL_DASHBOARD = "KQLDashboard"
    KQL_DATABASE = "KQLDatabase"
    KQL_QUERYSET = "KQLQueryset"
    LAKEHOUSE = "Lakehouse"
    ML_EXPERIMENT = "MLExperiment"
    ML_MODEL = "MLModel"
    MIRRORED_DATABASE = "MirroredDatabase"
    MIRRORED_WAREHOUSE = "MirroredWarehouse"
    NOTEBOOK = "Notebook"
    PAGINATED_REPORT = "PaginatedReport"
    REFLEX = "Reflex"
    REPORT = "Report"
    SQL_ENDPOINT = "SQLEndpoint"
    SEMANTIC_MODEL = "SemanticModel"
    SPARK_JOB_DEFINITION = "SparkJobDefinition"
    WAREHOUSE = "Warehouse"
    WORKSPACE = "Workspace"


class WorkspaceState(Enum):
    ACTIVE = "Active"
    DELETED = "Deleted"


class SqlEndpointProvisioningStatus(Enum):
    FAILED = "Failed"
    IN_PROGRESS = "InProgress"
    SUCCESS = "Success"


class LakehouseTableType(Enum):
    EXTERNAL = "External"
    MANAGED = "Managed"


class SchemaFieldTypeMapper:
    FIELD_TYPE_MAPPING = {
        # Bool
        "boolean": BooleanTypeClass,
        # Numbers
        "decimal": NumberTypeClass,
        "integer": NumberTypeClass,
        "int64": NumberTypeClass,
        "double": NumberTypeClass,
        # Dates and times
        "datetime": DateTypeClass,
        "date": DateTypeClass,
        "time": TimeTypeClass,
        # Strings
        "string": StringTypeClass,
        # Default
        "": NullTypeClass,
    }

    @classmethod
    def get_field_type(cls, data_type: str) -> SchemaFieldDataTypeClass:
        """Maps a PowerBI data type to a DataHub SchemaFieldDataTypeClass"""
        if not data_type:
            type_class = NullTypeClass
        else:
            data_type = data_type.lower()
            type_class = cls.FIELD_TYPE_MAPPING.get(data_type, StringTypeClass)

        return SchemaFieldDataTypeClass(type=type_class())


class PowerBiSourceTypeMapper:
    """Maps PowerBI M-query source patterns to DataHub platform codes"""

    SOURCE_TYPE_PATTERNS = {
        # SQL Databases
        "Sql.Database": "mssql",
        "SqlServer.Database": "mssql",
        "PostgreSQL.Database": "postgres",
        "Oracle.Database": "oracle",
        "MySql.Database": "mysql",
        "Snowflake.Databases": "snowflake",
        "Teradata.DataSource": "teradata",
        "Db2.Database": "db2",
        "ClickHouse.Database": "clickhouse",
        "Redshift": "redshift",
        "Sybase.Database": "sybase",
        # Cloud Data Warehouses
        "BigQuery": "bigquery",
        "AzureSynapse.Databases": "synapse",
        # Cloud Storage
        "AzureStorage": "azure-storage",
        "Azure.Storage": "azure-storage",
        "AzureDataLake.Storage": "adls",
        "AwsS3.Contents": "s3",
        "GoogleCloudStorage": "gcs",
        # Files
        "Excel.Workbook": "excel",
        "Parquet": "parquet",
        "Csv": "csv",
        "Json.Document": "json",
        # Analytics Platforms
        "DremioCloud": "dremio",
        "Databricks": "databricks",
        "Spark": "spark",
        "Presto": "presto",
        "Trino": "trino",
        # SaaS/Applications
        "Salesforce": "salesforce",
        "Dynamics": "dynamics-365",
        "SharePoint": "sharepoint",
        "SAPBusinessWarehouse": "sap",
        "AmazonRedshift": "redshift",
        # Apache Products
        "Hive": "hive",
        "Kafka": "kafka",
        "Cassandra": "cassandra",
        "MongoDB": "mongodb",
        # Azure Specific
        "AzureML.Model": "ml-model",
        "AzureKusto.Database": "kusto",
        "AzureCosmosDB": "cosmosdb",
        "MicrosoftGraphSecurity": "microsoft-graph",
        "AzureFabric": "azure-fabric",
        # Other Common Sources
        "OData.Feed": "rest",
        "Web.Contents": "rest",
        "ActiveDirectory": "azure-ad",
        "Xml.Tables": "xml",
    }

    SOURCE_QUERY_PATTERNS = {
        # Analytics Platforms
        r"DremioCloud\.DatabasesByServer[^\(]*\([^)]*\)": "dremio",
        r"Databricks[^\(]*\([^)]*\)": "databricks",
        # SQL Databases
        r"PostgreSQL\.Database[^\(]*\([^)]*\)": "postgres",
        r"Sql\.Database[^\(]*\([^)]*\)": "mssql",
        r"SqlServer\.Database[^\(]*\([^)]*\)": "mssql",
        r"Oracle\.Database[^\(]*\([^)]*\)": "oracle",
        r"MySql\.Database[^\(]*\([^)]*\)": "mysql",
        r"Snowflake\.Databases[^\(]*\([^)]*\)": "snowflake",
        # Cloud Storage
        r"AzureStorage[^\(]*\([^)]*\)": "azure-storage",
        r"AzureDataLake\.Storage[^\(]*\([^)]*\)": "adls",
        r"AwsS3\.Contents[^\(]*\([^)]*\)": "s3",
    }

    @classmethod
    def get_source_type(cls, source_query: str) -> str:
        """
        Maps a PowerBI M-query source to a DataHub platform code.

        Args:
            source_query: The M-query containing source information

        Returns:
            DataHub platform code for the source system
        """
        # Try regex patterns for matching
        for pattern, platform in cls.SOURCE_QUERY_PATTERNS.items():
            if re.search(pattern, source_query, re.IGNORECASE | re.MULTILINE):
                logger.debug(f"Found source type {platform} using pattern {pattern}")
                return platform

        logger.debug("No source type pattern matched for query, defaulting to powerbi")
        return "powerbi"

    @classmethod
    def extract_source_database(cls, source_query: str) -> Optional[str]:
        """
        Extract the source database name from the M-query if available.

        Args:
            source_query: The M-query containing source information

        Returns:
            Database name if found, None otherwise
        """
        for pattern in cls.SOURCE_QUERY_PATTERNS.keys():
            match = re.search(pattern, source_query, re.IGNORECASE)
            if match and match.groups():
                return match.group(1)
        return None

    @classmethod
    def get_source_pattern(cls, source_type: str) -> Optional[str]:
        """
        Get the corresponding M-query pattern for a source type.

        Args:
            source_type: The source type to look up

        Returns:
            The corresponding M-query pattern if found, None otherwise
        """
        # Check simple patterns
        for pattern, platform in cls.SOURCE_TYPE_PATTERNS.items():
            if platform == source_type:
                logger.debug(
                    f"Found source type {platform} for query pattern {pattern}"
                )
                return pattern

        # Check regex patterns
        for pattern, platform in cls.SOURCE_QUERY_PATTERNS.items():
            if platform == source_type:
                logger.debug(
                    f"Found source type {platform} for query pattern {pattern}"
                )
                return pattern

        return None
