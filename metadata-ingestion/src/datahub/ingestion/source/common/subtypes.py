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
    POWERBI_DATASET_TABLE = "PowerBI Dataset Table"
    QLIK_DATASET = "Qlik Dataset"
    BIGQUERY_TABLE_SNAPSHOT = "Bigquery Table Snapshot"
    SIGMA_DATASET = "Sigma Dataset"

    # TODO: Create separate entity...
    NOTEBOOK = "Notebook"


class DatasetContainerSubTypes(str, Enum):
    # Generic SubTypes
    DATABASE = "Database"
    SCHEMA = "Schema"
    # System-Specific SubTypes
    CATALOG = "Catalog"  # Presto or Unity Catalog
    BIGQUERY_PROJECT = "Project"
    BIGQUERY_DATASET = "Dataset"
    DATABRICKS_METASTORE = "Metastore"
    FOLDER = "Folder"
    S3_BUCKET = "S3 bucket"
    GCS_BUCKET = "GCS bucket"


class BIContainerSubTypes(str, Enum):
    LOOKER_FOLDER = "Folder"
    LOOKML_PROJECT = "LookML Project"
    LOOKML_MODEL = "LookML Model"
    TABLEAU_WORKBOOK = "Workbook"
    POWERBI_WORKSPACE = "Workspace"
    POWERBI_DATASET = "PowerBI Dataset"
    QLIK_SPACE = "Qlik Space"
    QLIK_APP = "Qlik App"
    SIGMA_WORKSPACE = "Sigma Workspace"
    SIGMA_WORKBOOK = "Sigma Workbook"
    MODE_COLLECTION = "Collection"


class JobContainerSubTypes(str, Enum):
    NIFI_PROCESS_GROUP = "Process Group"


class BIAssetSubTypes(str, Enum):
    # Generic SubTypes
    REPORT = "Report"

    # Looker
    LOOKER_LOOK = "Look"

    # PowerBI
    POWERBI_TILE = "PowerBI Tile"
    POWERBI_PAGE = "PowerBI Page"

    # Mode
    MODE_REPORT = "Report"
    MODE_QUERY = "Query"
    MODE_CHART = "Chart"
