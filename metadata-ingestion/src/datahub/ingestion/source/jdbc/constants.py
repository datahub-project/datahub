"""Constants and enums for JDBC source."""

from enum import Enum, auto
from typing import Dict, Type

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


class TableType(Enum):
    """Enumeration of supported table types."""

    TABLE = "TABLE"
    VIEW = "VIEW"

    @classmethod
    def from_string(cls, value: str) -> "TableType":
        """Convert string to TableType, default to TABLE if unknown."""
        try:
            return cls(value.upper())
        except ValueError:
            return cls.TABLE


class ContainerType(Enum):
    """Enumeration of container types."""

    DATABASE = "Database"
    SCHEMA = "Schema"
    FOLDER = "Folder"
    STORED_PROCEDURE = "StoredProcedure"


class SQLColumnType(Enum):
    """Enumeration of SQL column types for string-like columns."""

    VARCHAR = auto()
    CLOB = auto()
    TEXT = auto()
    LONGVARCHAR = auto()
    NVARCHAR = auto()
    CHARACTER_VARYING = auto()
    STRING = auto()

    @classmethod
    def is_string_type(cls, type_name: str) -> bool:
        """Check if the given type name represents a string type."""
        try:
            return cls[type_name.upper().replace(" ", "_")] is not None
        except KeyError:
            return False


class ProcedureType(Enum):
    """Enumeration of stored procedure types."""

    NO_RESULT = 0
    RETURNS_RESULT = 1
    RETURNS_OUTPUT = 2
    UNKNOWN = -1

    @classmethod
    def from_value(cls, value: int) -> Enum:
        """Convert integer value to ProcedureType, default to UNKNOWN if not found."""
        return cls._value2member_map_.get(value, cls.UNKNOWN)


# JDBC type mapping with type hints
JDBC_TYPE_MAP: Dict[str, Type] = {
    # Standard JDBC/SQL Types
    "CHAR": StringTypeClass,
    "VARCHAR": StringTypeClass,
    "VARCHAR2": StringTypeClass,
    "LONGVARCHAR": StringTypeClass,
    "NCHAR": StringTypeClass,
    "NVARCHAR": StringTypeClass,
    "NVARCHAR2": StringTypeClass,
    "LONGNVARCHAR": StringTypeClass,
    "CHARACTER": StringTypeClass,
    "CHARACTER VARYING": StringTypeClass,
    "NATIONAL CHAR": StringTypeClass,
    "NATIONAL CHARACTER": StringTypeClass,
    "NATIONAL CHARACTER VARYING": StringTypeClass,
    "NATIONAL CHAR VARYING": StringTypeClass,
    "STRING": StringTypeClass,
    "TEXT": StringTypeClass,
    "NTEXT": StringTypeClass,
    "LONGTEXT": StringTypeClass,
    "MEDIUMTEXT": StringTypeClass,
    "TINYTEXT": StringTypeClass,
    # Numeric Types
    "NUMERIC": NumberTypeClass,
    "DECIMAL": NumberTypeClass,
    "DEC": NumberTypeClass,
    "NUMBER": NumberTypeClass,
    "BIT": BooleanTypeClass,
    "BOOLEAN": BooleanTypeClass,
    "BOOL": BooleanTypeClass,
    "TINYINT": NumberTypeClass,
    "SMALLINT": NumberTypeClass,
    "MEDIUMINT": NumberTypeClass,
    "INTEGER": NumberTypeClass,
    "INT": NumberTypeClass,
    "INT2": NumberTypeClass,
    "INT4": NumberTypeClass,
    "INT8": NumberTypeClass,
    "BIGINT": NumberTypeClass,
    "REAL": NumberTypeClass,
    "FLOAT": NumberTypeClass,
    "FLOAT4": NumberTypeClass,
    "FLOAT8": NumberTypeClass,
    "DOUBLE": NumberTypeClass,
    "DOUBLE PRECISION": NumberTypeClass,
    # Auto-increment Types
    "SERIAL": NumberTypeClass,
    "SMALLSERIAL": NumberTypeClass,
    "BIGSERIAL": NumberTypeClass,
    "IDENTITY": NumberTypeClass,
    "AUTOINCREMENT": NumberTypeClass,
    # Binary Types
    "BINARY": BytesTypeClass,
    "VARBINARY": BytesTypeClass,
    "LONGVARBINARY": BytesTypeClass,
    "BYTEA": BytesTypeClass,
    "RAW": BytesTypeClass,
    "LONG RAW": BytesTypeClass,
    "BINARY VARYING": BytesTypeClass,
    "BIT VARYING": BytesTypeClass,
    "VARBIT": BytesTypeClass,
    "BLOB": BytesTypeClass,
    "TINYBLOB": BytesTypeClass,
    "MEDIUMBLOB": BytesTypeClass,
    "LONGBLOB": BytesTypeClass,
    "IMAGE": BytesTypeClass,
    # Date/Time Types
    "DATE": DateTypeClass,
    "TIME": TimeTypeClass,
    "TIMESTAMP": DateTypeClass,
    "DATETIME": DateTypeClass,
    "DATETIME2": DateTypeClass,
    "SMALLDATETIME": DateTypeClass,
    "TIMESTAMPTZ": DateTypeClass,
    "TIMESTAMP WITH TIME ZONE": DateTypeClass,
    "TIMESTAMP WITHOUT TIME ZONE": DateTypeClass,
    "TIMESTAMP WITH LOCAL TIME ZONE": DateTypeClass,
    "TIME WITH TIME ZONE": TimeTypeClass,
    "TIME WITHOUT TIME ZONE": TimeTypeClass,
    "TIMETZ": TimeTypeClass,
    "INTERVAL": StringTypeClass,
    "INTERVAL YEAR": StringTypeClass,
    "INTERVAL DAY": StringTypeClass,
    "INTERVAL YEAR TO MONTH": StringTypeClass,
    "INTERVAL DAY TO SECOND": StringTypeClass,
    "YEAR": NumberTypeClass,
    # Large Object Types
    "CLOB": StringTypeClass,
    "NCLOB": StringTypeClass,
    "DBCLOB": StringTypeClass,
    "XML": StringTypeClass,
    "XMLTYPE": StringTypeClass,
    # JSON Types
    "JSON": StringTypeClass,
    "JSONB": StringTypeClass,
    "BSON": StringTypeClass,
    # Array Types
    "ARRAY": StringTypeClass,
    "VARRAY": StringTypeClass,
    "SET": StringTypeClass,
    "MULTISET": StringTypeClass,
    # Object/Struct Types
    "STRUCT": StringTypeClass,
    "OBJECT": StringTypeClass,
    "REF": StringTypeClass,
    "ROWID": StringTypeClass,
    "UROWID": StringTypeClass,
    # Network Types
    "INET": StringTypeClass,
    "CIDR": StringTypeClass,
    "MACADDR": StringTypeClass,
    "MACADDR8": StringTypeClass,
    # Geometric Types
    "POINT": StringTypeClass,
    "LINE": StringTypeClass,
    "LSEG": StringTypeClass,
    "BOX": StringTypeClass,
    "PATH": StringTypeClass,
    "POLYGON": StringTypeClass,
    "CIRCLE": StringTypeClass,
    "GEOMETRY": StringTypeClass,
    # Other Types
    "UUID": StringTypeClass,
    "UNIQUEIDENTIFIER": StringTypeClass,
    "MONEY": NumberTypeClass,
    "SMALLMONEY": NumberTypeClass,
    "ENUM": StringTypeClass,
    "BIT_VARYING": StringTypeClass,
    "HIERARCHYID": StringTypeClass,
    "DECFLOAT": NumberTypeClass,
    "SDO_GEOMETRY": StringTypeClass,
    "GEOGRAPHY": StringTypeClass,
    "VARIANT": StringTypeClass,
    "CURSOR": StringTypeClass,
    "TABLE": StringTypeClass,
    "REFCURSOR": StringTypeClass,
    # Snowflake Specific
    "OBJECT": StringTypeClass,
    "ARRAY": StringTypeClass,
    "VARIANT": StringTypeClass,
    "GEOGRAPHY": StringTypeClass,
    "ANALYTICS": StringTypeClass,
    # DB2 Specific
    "GRAPHIC": StringTypeClass,
    "VARGRAPHIC": StringTypeClass,
    "LONG VARGRAPHIC": StringTypeClass,
    "DATALINK": StringTypeClass,
    "XML": StringTypeClass,
    # Oracle Specific
    "BFILE": BytesTypeClass,
    "LONG": StringTypeClass,
    "SDO_GEOMETRY": StringTypeClass,
    "ANYTYPE": StringTypeClass,
    "ANYDATA": StringTypeClass,
    "ANYDATASET": StringTypeClass,
    "URIType": StringTypeClass,
    # MySQL Specific
    "LINESTRING": StringTypeClass,
    "MULTIPOINT": StringTypeClass,
    "MULTILINESTRING": StringTypeClass,
    "MULTIPOLYGON": StringTypeClass,
    "GEOMETRYCOLLECTION": StringTypeClass,
    "GEOMCOLLECTION": StringTypeClass,
    # PostgreSQL Specific (additional to standard types)
    "BOX2D": StringTypeClass,
    "BOX3D": StringTypeClass,
    "GTSVECTOR": StringTypeClass,
    "TSQUERY": StringTypeClass,
    "TSVECTOR": StringTypeClass,
    "TXID_SNAPSHOT": StringTypeClass,
    "PG_LSN": StringTypeClass,
    "PG_SNAPSHOT": StringTypeClass,
    "INT4RANGE": StringTypeClass,
    "INT8RANGE": StringTypeClass,
    "NUMRANGE": StringTypeClass,
    "TSRANGE": StringTypeClass,
    "TSTZRANGE": StringTypeClass,
    "DATERANGE": StringTypeClass,
    # SQL Server Specific (additional to standard types)
    "DATETIMEOFFSET": DateTypeClass,
    "SQL_VARIANT": StringTypeClass,
    "SYSNAME": StringTypeClass,
}
