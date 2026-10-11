"""Shared constants for Azure-based ingestion sources (ADF, Fabric, etc.)."""

from typing import Dict

# Mapping of ADF linked service types to DataHub platforms.
# Platform identifiers must match those defined in:
# metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml
# Used by both the Azure Data Factory and Fabric Data Factory connectors.
ADF_LINKED_SERVICE_PLATFORM_MAP: Dict[str, str] = {
    # Azure Storage - all Azure storage types map to "abs" (Azure Blob Storage)
    "AzureBlobStorage": "abs",
    "AzureBlobFS": "abs",  # Azure Data Lake Storage Gen2 (uses abfs:// protocol)
    "AzureDataLakeStore": "abs",  # Azure Data Lake Storage Gen1
    "AzureDataLakeStoreCosmosStructuredStream": "abs",
    "AzureFileStorage": "abs",
    # Azure Databases - Synapse uses mssql protocol
    "AzureSqlDatabase": "mssql",
    "AzureSqlDW": "mssql",  # Azure Synapse (formerly SQL DW)
    "AzureSynapseAnalytics": "mssql",  # Azure Synapse Analytics
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
