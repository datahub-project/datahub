from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.source import Source

from .bigquery import BigQuerySource
from .hive import HiveSource
from .kafka import KafkaSource
from .ldap import LDAPSource
from .mce_file import MetadataFileSource
from .mssql import SQLServerSource
from .mysql import MySQLSource
from .postgres import PostgresSource
from .snowflake import SnowflakeSource

source_registry = Registry[Source]()

# Add some defaults to source registry.
source_registry.register("file", MetadataFileSource)
source_registry.register("mssql", SQLServerSource)
source_registry.register("mysql", MySQLSource)
source_registry.register("hive", HiveSource)
source_registry.register("postgres", PostgresSource)
source_registry.register("snowflake", SnowflakeSource)
source_registry.register("bigquery", BigQuerySource)
source_registry.register("kafka", KafkaSource)
source_registry.register("ldap", LDAPSource)
