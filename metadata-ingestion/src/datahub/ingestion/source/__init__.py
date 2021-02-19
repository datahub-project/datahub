from typing import Dict, Type

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

source_class_mapping: Dict[str, Type[Source]] = {
    "mssql": SQLServerSource,
    "mysql": MySQLSource,
    "hive": HiveSource,
    "postgres": PostgresSource,
    "snowflake": SnowflakeSource,
    "bigquery": BigQuerySource,
    "kafka": KafkaSource,
    "ldap": LDAPSource,
    "file": MetadataFileSource,
}
