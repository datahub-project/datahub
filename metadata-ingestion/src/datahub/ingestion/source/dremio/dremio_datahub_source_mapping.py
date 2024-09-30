class DremioToDataHubSourceTypeMapping:
    def __init__(self):
        self.mapping = {
            # Dremio source types
            "NAS": "file",
            "S3": "s3",
            "ADLS": "abfs",
            "HDFS": "hdfs",
            "ORACLE": "oracle",
            "POSTGRES": "postgres",
            "MYSQL": "mysql",
            "MSSQL": "mssql",
            "REDSHIFT": "redshift",
            "SNOWFLAKE": "snowflake",
            "MONGO": "mongodb",
            "ELASTIC": "elasticsearch",
            "HIVE": "hive",
            "AZURE_STORAGE": "abfs",
            "GOOGLE_CLOUD_STORAGE": "gcs",
            "AWSGLUE": "glue",
        }

        self.database_types = {
            "ORACLE", "POSTGRES", "MYSQL", "MSSQL", "REDSHIFT", "SNOWFLAKE",
            "MONGO", "HIVE", "ELASTIC", "AWSGLUE"
        }

        self.file_object_storage_types = {
            "NAS", "S3", "ADLS", "HDFS", "AZURE_STORAGE", "GOOGLE_CLOUD_STORAGE"
        }

    def get_datahub_source_type(self, dremio_source_type):
        return self.mapping.get(dremio_source_type.upper(), dremio_source_type.lower())

    def get_category(self, source_type):
        if source_type.upper() in self.database_types:
            return "database"
        elif source_type.upper() in self.file_object_storage_types:
            return "file_object_storage"
        else:
            return "unknown"

    def add_mapping(self, dremio_source_type, datahub_source_type, category=None):
        dremio_source_type = dremio_source_type.upper()
        self.mapping[dremio_source_type] = datahub_source_type
        if category:
            if category.lower() == "file_object_storage":
                self.file_object_storage_types.add(dremio_source_type)
            else:
                self.database_types.add(dremio_source_type)

