"""Fabric-wide constants shared across all Fabric ingestion sources."""

from typing import Dict

# Fabric web UI base URL for building external links
FABRIC_APP_BASE_URL = "https://app.fabric.microsoft.com"

# Source: https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/list-supported-connection-types?tabs=HTTP
# Unmapped connection types fall back to using the connection type string
# itself as the platform name (e.g. "SapHana" → platform "SapHana").
FABRIC_CONNECTION_PLATFORM_MAP: Dict[str, str] = {
    # --- Fabric OneLake (data-bearing items ingested by the OneLake connector) ---
    "Lakehouse": "fabric-onelake",
    "Warehouse": "fabric-onelake",
    "FabricSql": "fabric-onelake",
    "DataLake": "fabric-onelake",
    "SqlAnalyticsEndpoint": "fabric-onelake",
    "FabricSqlEndpointMetadata": "fabric-onelake",
    # --- Fabric non-data items ---
    "CopyJob": "fabric-data-factory",
    "FabricDataPipelines": "fabric-data-factory",
    "FabricMaps": "fabric",
    "FabricMaterializedLakeView": "fabric",
    "SparkJobDefinition": "fabric",
    "UserDataFunctions": "fabric",
    "Notebook": "fabric",
    # Power BI / Power Platform
    "PowerBI": "powerbi",
    "PowerBIDatasets": "powerbi",
    "PowerBIDatamarts": "powerbi",
    "PowerPlatformDataflows": "powerbi",
    "AutoPremium": "powerbi",
    "CapacityMetricsCES": "powerbi",
    "MetricsDataConnector": "powerbi",
    "UsageMetricsDataConnector": "powerbi",
    "UsageMetricsCES": "powerbi",
    # --- SQL Server / Azure SQL ---
    "SQL": "mssql",
    "AzureSqlMI": "mssql",
    "AmazonRdsForSqlServer": "mssql",
    "Synapse": "mssql",
    "AzureSynapseWorkspace": "mssql",
    # --- MySQL ---
    "MySql": "mysql",
    "AzureDatabaseForMySQL": "mysql",
    "MariaDBForPipeline": "mysql",
    # --- Oracle ---
    "Oracle": "oracle",
    "AmazonRdsForOracle": "oracle",
    # --- PostgreSQL ---
    "PostgreSQL": "postgres",
    "AzurePostgreSQL": "postgres",
    # --- Azure Blob / Data Lake Storage ---
    "AzureBlobs": "abs",
    "AzureDataLakeStorage": "abs",
    "AdlsGen2CosmosStructuredStream": "abs",
    "AzureDataLakeStoreCosmosStructuredStream": "abs",
    # --- Google BigQuery ---
    "GoogleBigQuery": "bigquery",
    "GoogleBigQueryAad": "bigquery",
    # --- Amazon Redshift ---
    "AmazonRedshift": "redshift",
    # --- Snowflake ---
    "Snowflake": "snowflake",
    # --- Hive ---
    "ApacheHive": "hive",
    "AzureHive": "hive",
    # --- Spark ---
    "Spark": "spark",
    # --- Databricks ---
    "Databricks": "databricks",
    "DatabricksMultiCloud": "databricks",
    "AzureDatabricksWorkspace": "databricks",
    # --- Dremio ---
    "Dremio": "dremio",
    "DremioCloud": "dremio",
    "DremioIcebergCatalog": "dremio",
    # --- Salesforce ---
    "Salesforce": "salesforce",
    "SalesforceServiceCloud": "salesforce",
    # --- Vertica ---
    "Vertica": "vertica",
    # --- Azure Data Explorer (Kusto) ---
    "AzureDataExplorer": "kusto",
    # --- Apache Cassandra ---
    "Cassandra": "cassandra",
    # --- Kafka ---
    "ConfluentCloud": "kafka",
    # --- Google Cloud Storage ---
    "GoogleCloudStorage": "gcs",
    "SAPDatasphereGoogleCloudStorage": "gcs",
    # --- HDFS ---
    "HdfsForPipeline": "hdfs",
    # --- Amazon S3 ---
    "AmazonS3": "s3",
    "AmazonS3Compatible": "s3",
    "SAPDatasphereAmazonS3": "s3",
    # --- MongoDB ---
    "MongoDBAtlasForPipeline": "mongodb",
    "MongoDBForPipeline": "mongodb",
    # --- Presto ---
    "Presto": "presto",
    # --- dbt ---
    "DataBuildToolJob": "dbt",
    # --- Elasticsearch ---
    "ElasticSearch": "elasticsearch",
    # --- Looker ---
    "Looker": "looker",
    # --- Delta Lake / Delta Sharing ---
    "DeltaSharing": "delta-lake",
}

# ADF LinkedService type → DataHub platform.
# Used as a fallback when the connection type matches ADF naming conventions
# rather than Fabric connection types (e.g. in pipelines migrated from ADF).
# Source: azure-mgmt-datafactory LinkedService types.
ADF_LINKED_SERVICE_PLATFORM_MAP: Dict[str, str] = {
    # Azure Storage
    "AzureBlobStorage": "abs",
    "AzureBlobFS": "abs",
    "AzureDataLakeStore": "abs",
    "AzureDataLakeStoreCosmosStructuredStream": "abs",
    "AzureFileStorage": "abs",
    # Azure Databases
    "AzureSqlDatabase": "mssql",
    "AzureSqlDW": "mssql",
    "AzureSynapseAnalytics": "mssql",
    "AzureSqlMI": "mssql",
    "SqlServer": "mssql",
    "AzurePostgreSql": "postgres",
    "AzureMySql": "mysql",
    # Databricks
    "AzureDatabricks": "databricks",
    "AzureDatabricksDeltaLake": "databricks",
    # Cloud Platforms
    "AmazonS3": "s3",
    "AmazonS3Compatible": "s3",
    "GoogleCloudStorage": "gcs",
    "AmazonRedshift": "redshift",
    "GoogleBigQuery": "bigquery",
    "Snowflake": "snowflake",
    # Traditional Databases
    "PostgreSql": "postgres",
    "MySql": "mysql",
    "Oracle": "oracle",
    "OracleServiceCloud": "oracle",
    "Db2": "db2",
    "Teradata": "teradata",
    "Vertica": "vertica",
    # Data Warehouses
    "Hive": "hive",
    "Spark": "spark",
    "Hdfs": "hdfs",
    # SaaS Applications
    "Salesforce": "salesforce",
    "SalesforceServiceCloud": "salesforce",
    "SalesforceMarketingCloud": "salesforce",
}
