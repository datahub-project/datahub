from enum import Enum


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
