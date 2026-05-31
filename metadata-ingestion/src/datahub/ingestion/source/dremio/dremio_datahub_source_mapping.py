from typing import TYPE_CHECKING, Dict, Optional, Set

if TYPE_CHECKING:
    from datahub.ingestion.source.dremio.dremio_config import (
        DremioSourceTypeOverride,
    )


class DremioToDataHubSourceTypeMapping:
    """Dremio source type to DataHub platform mapping.

    The `lookup_*` instance methods honor per-recipe overrides registered via
    `DremioSourceConfig.source_type_mappings`; the `get_*` `@staticmethod`s
    expose the built-in mapping only and are kept for callers that don't have
    a config in hand. Overrides live on the instance and never mutate the
    class-level mapping.
    """

    SOURCE_TYPE_MAPPING: Dict[str, str] = {
        # Dremio source types -> DataHub platform names.
        # Source type strings come from Dremio's Catalog API (the `type`
        # field on a source object). See
        # https://docs.dremio.com/current/reference/api/catalog/source/ and
        # https://docs.dremio.com/dremio-cloud/api/catalog/source/.
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
        # Iceberg REST Catalog source: covers Apache Polaris OSS, Nessie
        # with Iceberg REST, AWS Glue Iceberg REST, S3 Tables, Confluent
        # Tableflow, and Microsoft OneLake (all served as Iceberg tables).
        "RESTCATALOG": "iceberg",
        "S3": "s3",
        "SAPHANA": "hana",
        "SNOWFLAKE": "snowflake",
        # Snowflake Open Catalog (managed Polaris): also serves Iceberg
        # tables, so map to the iceberg platform rather than snowflake to
        # match how the data is physically materialised.
        "SNOWFLAKEOPENCATALOG": "iceberg",
        "SYNAPSE": "mssql",
        "TERADATA": "teradata",
        # Databricks Unity Catalog source.
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
        # AZURE_STORAGE is also listed in DATABASE_SOURCE_TYPES above.
        # get_category checks DATABASE_SOURCE_TYPES first, so AZURE_STORAGE
        # always resolves to "database" — which is the dot-notation path
        # users hit in practice with Dremio's Azure Storage source
        # (container/database/table). It's kept in this set so any future
        # call site that asks "is this object storage?" still says yes.
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
