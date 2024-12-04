from datahub.utilities.str_enum import StrEnum


class DatasetSubTypes(StrEnum):
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
    QLIK_DATASET = "Qlik Dataset"
    BIGQUERY_TABLE_SNAPSHOT = "Bigquery Table Snapshot"
    SHARDED_TABLE = "Sharded Table"
    EXTERNAL_TABLE = "External Table"
    SIGMA_DATASET = "Sigma Dataset"
    SAC_MODEL = "Model"
    SAC_IMPORT_DATA_MODEL = "Import Data Model"
    SAC_LIVE_DATA_MODEL = "Live Data Model"
    NEO4J_NODE = "Neo4j Node"
    NEO4J_RELATIONSHIP = "Neo4j Relationship"

    # TODO: Create separate entity...
    NOTEBOOK = "Notebook"


class DatasetContainerSubTypes(StrEnum):
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
    ABS_CONTAINER = "ABS container"
    KEYSPACE = "Keyspace"  # Cassandra


class BIContainerSubTypes(StrEnum):
    LOOKER_FOLDER = "Folder"
    LOOKML_PROJECT = "LookML Project"
    LOOKML_MODEL = "LookML Model"
    TABLEAU_WORKBOOK = "Workbook"
    POWERBI_DATASET = "Semantic Model"
    POWERBI_DATASET_TABLE = "Table"
    QLIK_SPACE = "Qlik Space"
    QLIK_APP = "Qlik App"
    SIGMA_WORKSPACE = "Sigma Workspace"
    SIGMA_WORKBOOK = "Sigma Workbook"
    MODE_COLLECTION = "Collection"


class JobContainerSubTypes(StrEnum):
    NIFI_PROCESS_GROUP = "Process Group"


class BIAssetSubTypes(StrEnum):
    # Generic SubTypes
    REPORT = "Report"

    # Looker
    LOOKER_LOOK = "Look"

    # PowerBI
    POWERBI_TILE = "PowerBI Tile"
    POWERBI_PAGE = "PowerBI Page"
    POWERBI_APP = "App"

    # Mode
    MODE_REPORT = "Report"
    MODE_DATASET = "Dataset"
    MODE_QUERY = "Query"
    MODE_CHART = "Chart"

    # SAP Analytics Cloud
    SAC_STORY = "Story"
    SAC_APPLICATION = "Application"
