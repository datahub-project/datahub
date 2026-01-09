import logging
from typing import Any, Dict

from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


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
    GRAFANA_DATASET = "Grafana Dataset"
    BIGQUERY_TABLE_SNAPSHOT = "Bigquery Table Snapshot"
    SHARDED_TABLE = "Sharded Table"
    EXTERNAL_TABLE = "External Table"
    SIGMA_DATASET = "Sigma Dataset"
    SAC_MODEL = "Model"
    SAC_IMPORT_DATA_MODEL = "Import Data Model"
    SAC_LIVE_DATA_MODEL = "Live Data Model"
    NEO4J_NODE = "Neo4j Node"
    NEO4J_RELATIONSHIP = "Neo4j Relationship"
    SNOWFLAKE_STREAM = "Snowflake Stream"
    SEMANTIC_VIEW = "Semantic View"
    DYNAMIC_TABLE = "Dynamic Table"
    API_ENDPOINT = "API Endpoint"
    SLACK_CHANNEL = "Slack Channel"
    PROJECTIONS = "Projections"
    GOOGLE_SHEETS = "Google Sheets"
    GOOGLE_SHEETS_NAMED_RANGE = "Google Sheets Named Range"

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
    NAMESPACE = "Namespace"  # Iceberg
    DREMIO_SPACE = "Dremio Space"
    DREMIO_SOURCE = "Dremio Source"


class BIContainerSubTypes(StrEnum):
    LOOKER_FOLDER = "Folder"
    LOOKML_PROJECT = "LookML Project"
    LOOKML_MODEL = "LookML Model"
    TABLEAU_SITE = "Site"
    TABLEAU_PROJECT = "Project"
    TABLEAU_WORKBOOK = "Workbook"
    POWERBI_WORKSPACE = "Workspace"
    POWERBI_DATASET = "Semantic Model"
    POWERBI_DATASET_TABLE = "Table"
    QLIK_SPACE = "Qlik Space"
    QLIK_APP = "Qlik App"
    SIGMA_WORKSPACE = "Sigma Workspace"
    SIGMA_WORKBOOK = "Sigma Workbook"
    MODE_COLLECTION = "Collection"
    GRAFANA_FOLDER = "Folder"
    GRAFANA_DASHBOARD = "Dashboard"


class FlowContainerSubTypes(StrEnum):
    MSSQL_JOB = "Job"
    MSSQL_PROCEDURE_CONTAINER = "Procedures Container"
    ADF_DATA_FACTORY = "Data Factory"


class JobContainerSubTypes(StrEnum):
    NIFI_PROCESS_GROUP = "Process Group"
    MSSQL_JOBSTEP = "Job Step"
    STORED_PROCEDURE = "Stored Procedure"


class BIAssetSubTypes(StrEnum):
    DASHBOARD = "Dashboard"
    CHART = "Chart"

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

    # Hex
    HEX_PROJECT = "Project"
    HEX_COMPONENT = "Component"

    # Streamlit
    STREAMLIT = "Streamlit"


class MLAssetSubTypes(StrEnum):
    MLFLOW_TRAINING_RUN = "ML Training Run"
    MLFLOW_EXPERIMENT = "ML Experiment"
    VERTEX_EXPERIMENT = "Experiment"
    VERTEX_EXPERIMENT_RUN = "Experiment Run"
    VERTEX_EXECUTION = "Execution"

    VERTEX_MODEL = "ML Model"
    VERTEX_MODEL_GROUP = "ML Model Group"
    VERTEX_TRAINING_JOB = "Training Job"
    VERTEX_ENDPOINT = "Endpoint"
    VERTEX_DATASET = "Dataset"
    VERTEX_PROJECT = "Project"
    VERTEX_PIPELINE = "Pipeline Job"
    VERTEX_PIPELINE_TASK = "Pipeline Task"
    VERTEX_PIPELINE_TASK_RUN = "Pipeline Task Run"


class DataJobSubTypes(StrEnum):
    # ADF Activity Types
    ADF_COPY_ACTIVITY = "Copy Activity"
    ADF_DATA_FLOW_ACTIVITY = "Data Flow Activity"
    ADF_EXECUTE_PIPELINE = "Execute Pipeline"
    ADF_LOOKUP_ACTIVITY = "Lookup Activity"
    ADF_GET_METADATA_ACTIVITY = "Get Metadata Activity"
    ADF_STORED_PROCEDURE_ACTIVITY = "Stored Procedure Activity"
    ADF_SCRIPT_ACTIVITY = "Script Activity"
    ADF_WEB_ACTIVITY = "Web Activity"
    ADF_WEBHOOK_ACTIVITY = "Webhook Activity"
    ADF_IF_CONDITION = "If Condition"
    ADF_FOREACH_LOOP = "ForEach Loop"
    ADF_UNTIL_LOOP = "Until Loop"
    ADF_WAIT_ACTIVITY = "Wait Activity"
    ADF_SET_VARIABLE = "Set Variable"
    ADF_APPEND_VARIABLE = "Append Variable"
    ADF_SWITCH_ACTIVITY = "Switch Activity"
    ADF_FILTER_ACTIVITY = "Filter Activity"
    ADF_VALIDATION_ACTIVITY = "Validation Activity"
    ADF_DATABRICKS_NOTEBOOK = "Databricks Notebook"
    ADF_DATABRICKS_SPARK_JAR = "Databricks Spark Jar"
    ADF_DATABRICKS_SPARK_PYTHON = "Databricks Spark Python"
    ADF_HDINSIGHT_HIVE = "HDInsight Hive"
    ADF_HDINSIGHT_PIG = "HDInsight Pig"
    ADF_HDINSIGHT_SPARK = "HDInsight Spark"
    ADF_HDINSIGHT_MAPREDUCE = "HDInsight MapReduce"
    ADF_HDINSIGHT_STREAMING = "HDInsight Streaming"
    ADF_AZURE_FUNCTION_ACTIVITY = "Azure Function Activity"
    ADF_AZURE_ML_BATCH = "Azure ML Batch"
    ADF_AZURE_ML_UPDATE = "Azure ML Update"
    ADF_AZURE_ML_PIPELINE = "Azure ML Pipeline"
    ADF_CUSTOM_ACTIVITY = "Custom Activity"
    ADF_DELETE_ACTIVITY = "Delete Activity"
    ADF_SYNAPSE_NOTEBOOK = "Synapse Notebook"
    ADF_SPARK_JOB = "Spark Job"
    ADF_SYNAPSE_SPARK_JOB = "Synapse Spark Job"
    ADF_SQL_POOL_STORED_PROCEDURE = "SQL Pool Stored Procedure"
    ADF_FAIL_ACTIVITY = "Fail Activity"


def create_source_capability_modifier_enum():
    all_values: Dict[str, Any] = {}
    source_enums = [
        DatasetSubTypes,
        DatasetContainerSubTypes,
        BIContainerSubTypes,
        FlowContainerSubTypes,
        JobContainerSubTypes,
        BIAssetSubTypes,
        MLAssetSubTypes,
        DataJobSubTypes,
    ]

    for enum_class in source_enums:
        for member in enum_class:  # type: ignore[var-annotated]
            if member.name in all_values:
                logger.debug(
                    f"Warning: {member.name} already exists with value {all_values[member.name]}, skipping {member.value}"
                )
                continue
            all_values[member.name] = member.value

    enum_code = "class SourceCapabilityModifier(StrEnum):\n"
    for name, value in all_values.items():
        enum_code += f'    {name} = "{value}"\n'

    exec(enum_code, globals())
    return globals()["SourceCapabilityModifier"]


# This will have all values from the enums above
SourceCapabilityModifier = create_source_capability_modifier_enum()
