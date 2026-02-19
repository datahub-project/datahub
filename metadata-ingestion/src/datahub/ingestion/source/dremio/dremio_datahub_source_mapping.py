from typing import Optional


class DremioToDataHubSourceTypeMapping:
    """
    Dremio source type to the Datahub source type mapping.
    """

    SOURCE_TYPE_MAPPING = {
        # Dremio source types from https://docs.dremio.com/current/reference/api/catalog/source/container-source-config
        "ADL": "abfs",
        "AMAZONELASTIC": "elasticsearch",
        "AWSGLUE": "glue",
        "AZURE_STORAGE": "abfs",
        "BIGQUERY": "bigquery",
        "DB2": "db2",
        "DREMIOTODREMIO": "dremio",
        "ELASTIC": "elasticsearch",
        "EXTERNAL": "iceberg",
        "GCS": "gcs",
        "HANA": "hana",
        "HDFS": "s3",
        "HIVE": "hive",
        "HIVE3": "hive",
        "ICEBERG": "iceberg",
        "KUSTO": "azure-data-explorer",
        "MONGO": "mongodb",
        "MSSQL": "mssql",
        "MYSQL": "mysql",
        "NAS": "s3",
        "NESSIE": "iceberg",
        "ORACLE": "oracle",
        "POSTGRES": "postgres",
        "REDSHIFT": "redshift",
        "S3": "s3",
        "SNOWFLAKE": "snowflake",
        "SNOWFLAKE_ICEBERG": "snowflake",
        "SYNAPSE": "mssql",
        "TERADATA": "teradata",
        "UNITY": "unity-catalog",
        "VERTICA": "vertica",
    }

    DATABASE_SOURCE_TYPES = {
        "AMAZONELASTIC",
        "AWSGLUE",
        "AZURE_STORAGE",
        "BIGQUERY",
        "DB2",
        "DREMIOTODREMIO",
        "ELASTIC",
        "EXTERNAL",
        "HANA",
        "HIVE",
        "HIVE3",
        "ICEBERG",
        "KUSTO",
        "MONGO",
        "MSSQL",
        "MYSQL",
        "NESSIE",
        "ORACLE",
        "POSTGRES",
        "REDSHIFT",
        "SNOWFLAKE",
        "SNOWFLAKE_ICEBERG",
        "SYNAPSE",
        "TERADATA",
        "UNITY",
        "VERTICA",
    }

    FILE_OBJECT_STORAGE_TYPES = {
        "ADL",
        "AZURE_STORAGE",
        "GCS",
        "HDFS",
        "NAS",
        "S3",
    }

    @staticmethod
    def get_datahub_platform(dremio_source_type: str) -> str:
        """
        Return the DataHub source type.
        """
        return DremioToDataHubSourceTypeMapping.SOURCE_TYPE_MAPPING.get(
            dremio_source_type.upper(), dremio_source_type.lower()
        )

    @staticmethod
    def get_category(source_type: str) -> str:
        """
        Define whether the source uses dot notation (DB) or slash notation (Object storage).
        """
        if (
            source_type.upper()
            in DremioToDataHubSourceTypeMapping.DATABASE_SOURCE_TYPES
        ):
            return "database"
        if (
            source_type.upper()
            in DremioToDataHubSourceTypeMapping.FILE_OBJECT_STORAGE_TYPES
        ):
            return "file_object_storage"
        return "unknown"

    @staticmethod
    def add_mapping(
        dremio_source_type: str,
        datahub_source_type: str,
        category: Optional[str] = None,
    ) -> None:
        """
        Add a new source type if not in the map (e.g., Dremio ARP).
        """
        dremio_source_type = dremio_source_type.upper()
        DremioToDataHubSourceTypeMapping.SOURCE_TYPE_MAPPING[dremio_source_type] = (
            datahub_source_type
        )

        if category:
            if category.lower() == "file_object_storage":
                DremioToDataHubSourceTypeMapping.FILE_OBJECT_STORAGE_TYPES.add(
                    dremio_source_type
                )
            else:
                DremioToDataHubSourceTypeMapping.DATABASE_SOURCE_TYPES.add(
                    dremio_source_type
                )
