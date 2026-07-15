from typing import TYPE_CHECKING, Dict, Optional, Set

if TYPE_CHECKING:
    from datahub.ingestion.source.dremio.dremio_config import (
        DremioSourceTypeOverride,
    )


class DremioToDataHubSourceTypeMapping:
    """Dremio source type to DataHub platform mapping. The `lookup_*` instance
    methods honor per-recipe overrides from `DremioSourceConfig.source_type_mappings`;
    the `get_*` staticmethods expose the built-in mapping only."""

    # Keys are the `type` field returned by Dremio's Catalog API
    # (https://docs.dremio.com/current/reference/api/catalog/source/).
    SOURCE_TYPE_MAPPING: Dict[str, str] = {
        "ADL": "abfs",
        "AMAZONELASTIC": "elasticsearch",
        "AWSGLUE": "glue",
        "AZURE_STORAGE": "abfs",
        "BIGQUERY": "bigquery",
        "DB2": "db2",
        "DREMIOTODREMIO": "dremio",
        "ELASTIC": "elasticsearch",
        "GCS": "gcs",
        "HDFS": "s3",
        "HIVE": "hive",
        "HIVE3": "hive",
        "MONGO": "mongodb",
        "MSSQL": "mssql",
        "MYSQL": "mysql",
        "NAS": "s3",
        "NESSIE": "iceberg",
        "ORACLE": "oracle",
        "POSTGRES": "postgres",
        "REDSHIFT": "redshift",
        # Polaris OSS, Nessie+REST, Glue Iceberg REST, S3 Tables,
        # Tableflow, OneLake — all surface as Iceberg tables.
        "RESTCATALOG": "iceberg",
        "S3": "s3",
        "SAPHANA": "hana",
        "SNOWFLAKE": "snowflake",
        # Snowflake Open Catalog (managed Polaris) serves Iceberg, not Snowflake tables.
        "SNOWFLAKEOPENCATALOG": "iceberg",
        "SYNAPSE": "mssql",
        "TERADATA": "teradata",
        "UNITY": "databricks",
        "VERTICA": "vertica",
    }

    DATABASE_SOURCE_TYPES: Set[str] = {
        "AMAZONELASTIC",
        "AWSGLUE",
        "AZURE_STORAGE",
        "BIGQUERY",
        "DB2",
        "DREMIOTODREMIO",
        "ELASTIC",
        "HIVE",
        "HIVE3",
        "MONGO",
        "MSSQL",
        "MYSQL",
        "NESSIE",
        "ORACLE",
        "POSTGRES",
        "REDSHIFT",
        "RESTCATALOG",
        "SAPHANA",
        "SNOWFLAKE",
        "SNOWFLAKEOPENCATALOG",
        "SYNAPSE",
        "TERADATA",
        "UNITY",
        "VERTICA",
    }

    FILE_OBJECT_STORAGE_TYPES: Set[str] = {
        "ADL",
        # AZURE_STORAGE also lives in DATABASE_SOURCE_TYPES. get_category
        # picks "database" first (matching the dot-notation path users
        # actually hit); membership here keeps "is object storage?" honest.
        "AZURE_STORAGE",
        "GCS",
        "HDFS",
        "NAS",
        "S3",
    }

    def __init__(
        self,
        extra_mappings: Optional[Dict[str, "DremioSourceTypeOverride"]] = None,
    ) -> None:
        self._platform_overrides: Dict[str, str] = {}
        self._database_overrides: Set[str] = set()
        self._file_overrides: Set[str] = set()
        for source_type, override in (extra_mappings or {}).items():
            key = source_type.upper()
            self._platform_overrides[key] = override.platform
            if override.category == "database":
                self._database_overrides.add(key)
            elif override.category == "file_object_storage":
                self._file_overrides.add(key)

    @staticmethod
    def get_datahub_platform(dremio_source_type: str) -> str:
        """Built-in mapping lookup. Prefer `lookup_datahub_platform` to honor recipe overrides."""
        return DremioToDataHubSourceTypeMapping.SOURCE_TYPE_MAPPING.get(
            dremio_source_type.upper(), dremio_source_type.lower()
        )

    @staticmethod
    def get_category(source_type: str) -> str:
        """Built-in category lookup. Prefer `lookup_category` to honor recipe overrides."""
        key = source_type.upper()
        if key in DremioToDataHubSourceTypeMapping.DATABASE_SOURCE_TYPES:
            return "database"
        if key in DremioToDataHubSourceTypeMapping.FILE_OBJECT_STORAGE_TYPES:
            return "file_object_storage"
        return "unknown"

    def lookup_datahub_platform(self, dremio_source_type: str) -> str:
        key = dremio_source_type.upper()
        if key in self._platform_overrides:
            return self._platform_overrides[key]
        return self.SOURCE_TYPE_MAPPING.get(key, dremio_source_type.lower())

    def lookup_category(self, source_type: str) -> str:
        key = source_type.upper()
        if key in self._database_overrides or key in self.DATABASE_SOURCE_TYPES:
            return "database"
        if key in self._file_overrides or key in self.FILE_OBJECT_STORAGE_TYPES:
            return "file_object_storage"
        return "unknown"
