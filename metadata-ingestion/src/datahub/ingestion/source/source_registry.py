from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.source import Source

from .mce_file import MetadataFileSource

source_registry = Registry[Source]()

# This source is always enabled.
source_registry.register("file", MetadataFileSource)

try:
    from .athena import AthenaSource

    source_registry.register("athena", AthenaSource)
except ImportError as e:
    source_registry.register_disabled("athena", e)

try:
    from .bigquery import BigQuerySource

    source_registry.register("bigquery", BigQuerySource)
except ImportError as e:
    source_registry.register_disabled("bigquery", e)

try:
    from .hive import HiveSource

    source_registry.register("hive", HiveSource)
except ImportError as e:
    source_registry.register_disabled("hive", e)

try:
    from .mssql import SQLServerSource

    source_registry.register("mssql", SQLServerSource)
except ImportError as e:
    source_registry.register_disabled("mssql", e)

try:
    from .mysql import MySQLSource

    source_registry.register("mysql", MySQLSource)
except ImportError as e:
    source_registry.register_disabled("mysql", e)

try:
    from .postgres import PostgresSource

    source_registry.register("postgres", PostgresSource)
except ImportError as e:
    source_registry.register_disabled("postgres", e)

try:
    from .snowflake import SnowflakeSource

    source_registry.register("snowflake", SnowflakeSource)
except ImportError as e:
    source_registry.register_disabled("snowflake", e)

try:
    from .druid import DruidSource

    source_registry.register("druid", DruidSource)
except ImportError as e:
    source_registry.register_disabled("druid", e)

try:
    from .kafka import KafkaSource

    source_registry.register("kafka", KafkaSource)
except ImportError as e:
    source_registry.register_disabled("kafka", e)

try:
    from .dbt import DBTSource

    source_registry.register("dbt", DBTSource)
except ImportError as e:
    source_registry.register_disabled("dbt", e)

try:
    from .ldap import LDAPSource

    source_registry.register("ldap", LDAPSource)
except ImportError as e:
    source_registry.register_disabled("ldap", e)


try:
    from .mongodb import MongoDBSource

    source_registry.register("mongodb", MongoDBSource)
except ImportError as e:
    source_registry.register_disabled("mongodb", e)
