from typing import Dict, Type

from gometa.ingestion.api.source import Source
# from .mssql import SQLServerSource
# from .mysql import MySQLSource
from .kafka import KafkaSource
# from .ldap import LDAPSource
from .mce_file import MetadataFileSource

source_class_mapping: Dict[str, Type[Source]] = {
    # "mssql": SQLServerSource,
    # "mysql": MySQLSource,
    "kafka": KafkaSource,
    # "ldap": LDAPSource,
    "file": MetadataFileSource,
}
