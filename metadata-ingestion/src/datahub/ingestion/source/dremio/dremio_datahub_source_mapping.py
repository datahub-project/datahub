from typing import Dict, Optional


class DremioToDataHubSourceTypeMapping:
    """
    Dremio source type to the Datahub source type mapping.
    """

    SOURCE_TYPE_MAPPING = {
        # Dremio source types
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
        "SAPHANA": "hana",
        "S3": "s3",
        "SNOWFLAKE": "snowflake",
        "SYNAPSE": "mssql",
        "TERADATA": "teradata",
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
        "HIVE",
        "HIVE3",
        "MONGO",
        "MSSQL",
        "MYSQL",
        "NESSIE",
        "ORACLE",
        "POSTGRES",
        "REDSHIFT",
        "SAPHANA",
        "SNOWFLAKE",
        "SYNAPSE",
        "TERADATA",
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

    @staticmethod
    def get_platform_from_format(
        format_type: Optional[str],
        source_type: str,
        format_mapping: Dict[str, str],
        use_format_based_platforms: bool = False,
    ) -> str:
        """
        Determine the appropriate DataHub platform based on table format and source type.

        This method implements intelligent platform detection that can assign platform-specific
        URNs based on the underlying storage format of datasets. This enables better lineage
        tracking and metadata organization for modern data lake formats.

        Platform Detection Logic:
        1. If format-based platforms are disabled, fall back to source-based mapping
        2. If format_type is None or empty, fall back to source-based mapping
        3. Check format_type against format_mapping keys (case-insensitive substring matching)
        4. Return the first matching platform from format_mapping
        5. If no format matches, fall back to source-based mapping

        Format Matching:
        - Uses case-insensitive substring matching for flexibility
        - "DELTA_LAKE" format_type matches "delta" mapping key
        - "apache_iceberg" format_type matches "iceberg" mapping key
        - First match wins if multiple keys match the same format_type

        Args:
            format_type: The storage format from Dremio metadata. Common values include:
                        "DELTA", "ICEBERG", "HUDI", "PARQUET", "ORC", "AVRO", etc.
                        Can be None for datasets without format information.
            source_type: The Dremio source type for fallback mapping. Examples:
                        "S3", "HDFS", "AZURE_STORAGE", "GCS", "SNOWFLAKE", etc.
            format_mapping: Dictionary mapping format substrings to DataHub platforms.
                           Keys are case-insensitive substrings to match in format_type.
                           Values are DataHub platform names.
                           Example: {"delta": "delta-lake", "iceberg": "iceberg"}
            use_format_based_platforms: Whether to enable format-based detection.
                                       If False, always uses source-based mapping.

        Returns:
            DataHub platform name to use for URN generation. Examples:
            - "delta-lake" for Delta Lake tables
            - "iceberg" for Apache Iceberg tables
            - "hudi" for Apache Hudi tables
            - "s3" for generic S3 data or when format detection fails
            - Platform from SOURCE_TYPE_MAPPING for other source types

        Example:
            >>> # Delta Lake detection
            >>> platform = get_platform_from_format(
            ...     format_type="DELTA",
            ...     source_type="S3",
            ...     format_mapping={"delta": "delta-lake", "iceberg": "iceberg"},
            ...     use_format_based_platforms=True
            ... )
            >>> print(platform)  # "delta-lake"

            >>> # Fallback to source mapping
            >>> platform = get_platform_from_format(
            ...     format_type="UNKNOWN_FORMAT",
            ...     source_type="SNOWFLAKE",
            ...     format_mapping={"delta": "delta-lake"},
            ...     use_format_based_platforms=True
            ... )
            >>> print(platform)  # "snowflake"

            >>> # Disabled format detection
            >>> platform = get_platform_from_format(
            ...     format_type="DELTA",
            ...     source_type="S3",
            ...     format_mapping={"delta": "delta-lake"},
            ...     use_format_based_platforms=False
            ... )
            >>> print(platform)  # "s3"
        """
        if use_format_based_platforms and format_type:
            format_lower = format_type.lower()

            # Check for format matches in the mapping
            for format_key, platform in format_mapping.items():
                if format_key.lower() in format_lower:
                    return platform

        # Fallback to source-based mapping
        return DremioToDataHubSourceTypeMapping.get_datahub_platform(source_type)
