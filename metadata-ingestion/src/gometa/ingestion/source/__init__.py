from typing import Dict, Type

from gometa.ingestion.api.source import Source

from .kafka import KafkaSource

# from .ldap import LDAPSource
from .mce_file import MetadataFileSource
from .mssql import SQLServerSource
from .mysql import MySQLSource
from .hive import HiveSource
from .postgres import PostgresSource

source_class_mapping: Dict[str, Type[Source]] = {
    "mssql": SQLServerSource,
    "mysql": MySQLSource,
    "hive": HiveSource,
    "postgres": PostgresSource,
    "kafka": KafkaSource,
    # "ldap": LDAPSource,
    "file": MetadataFileSource,
}
