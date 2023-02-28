from enum import Enum


class DatasetSubTypes(str, Enum):
    # Generic SubTypes
    TABLE = "Table"
    VIEW = "View"
    TOPIC = "Topic"
    SCHEMA = "Schema"
    # System-Specific SubTypes
    LOOKER_EXPLORE = "Explore"
    ELASTIC_INDEX_TEMPLATE = "Index Template"
    ELASTIC_INDEX = "Index"
    ELASTIC_DATASTREAM = "Datastream"
    SALESFORCE_CUSTOM_OBJECT = "Custom Object"
    SALESFORCE_STANDARD_OBJECT = "Object"


class DatasetContainerSubTypes(str, Enum):
    # Generic SubTypes
    DATABASE = "Database"
    SCHEMA = "Schema"
    # System-Specific SubTypes
    PRESTO_CATALOG = "Catalog"
    BIGQUERY_PROJECT = "Project"
    BIGQUERY_DATASET = "Dataset"
    DATABRICKS_METASTORE = "Metastore"
    S3_FOLDER = "Folder"
    S3_BUCKET = "S3 bucket"


class BIContainerSubTypes(str, Enum):
    LOOKER_FOLDER = "Folder"
    TABLEAU_WORKBOOK = "Workbook"
    POWERBI_WORKSPACE = "Workspace"


class BIAssetSubTypes(str, Enum):
    # Generic SubTypes
    REPORT = "Report"
