"""
    Dremio source type to Datahub source type.
"""
class DremioToDataHubSourceTypeMapping:
    """
    Dremio source type to the Datahub source type mapping.
    """
    def __init__(self):
        self.mapping = {
            # Dremio source types
            "ADL": "abfs",
            "AMAZONELASTIC": "elasticsearch",
            "AWSGLUE": "glue",
            "AZURE_STORAGE": "abfs",
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
            "S3": "s3",
            "SNOWFLAKE": "snowflake",
            "SYNAPSE": "mssql",
            "TERADATA": "teradata",

        }

        self.database_types = {
            "AMAZONELASTIC", "AWSGLUE", "AZURE_STORAGE", "DB2", "DREMIOTODREMIO", "ELASTIC", "HIVE", "HIVE3", "MONGO",
            "MSSQL", "MYSQL", "NESSIE", "ORACLE", "POSTGRES", "REDSHIFT", "SNOWFLAKE", "SYNAPSE", "TERADATA",
        }

        self.file_object_storage_types = {
            "ADL", "AZURE_STORAGE", "GCS", "HDFS", "NAS", "S3",
        }

    def get_datahub_source_type(self, dremio_source_type):
        """
            Return the datahub source type.
        """
        return self.mapping.get(dremio_source_type.upper(), dremio_source_type.lower())

    def get_category(self, source_type):
        """
            Define whether source uses dot notation (DB) or slash notation (Object storage)
        """
        if source_type.upper() in self.database_types:
            return "database"
        if source_type.upper() in self.file_object_storage_types:
            return "file_object_storage"
        return "unknown"

    def add_mapping(self, dremio_source_type, datahub_source_type, category=None):
        """
            Add new source type if not in map (e.g. Dremio ARP)
        """
        dremio_source_type = dremio_source_type.upper()
        self.mapping[dremio_source_type] = datahub_source_type
        if category:
            if category.lower() == "file_object_storage":
                self.file_object_storage_types.add(dremio_source_type)
            else:
                self.database_types.add(dremio_source_type)
