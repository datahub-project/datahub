from typing import Dict, Tuple

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

PLATFORM = "informix"

# Informix syscolumns.coltype: low byte (coltype & 0xFF) is the base type code;
# bit 0x100 (256) is the NOT NULL flag. See IBM Informix SQL Reference (SYSCOLUMNS).
_NOT_NULL_BIT = 0x100
_BASE_TYPE_MASK = 0xFF

# base type code -> (DataHub type class, canonical native name)
INFORMIX_TYPE_MAP: Dict[int, Tuple[type, str]] = {
    0: (StringTypeClass, "CHAR"),
    1: (NumberTypeClass, "SMALLINT"),
    2: (NumberTypeClass, "INTEGER"),
    3: (NumberTypeClass, "FLOAT"),
    4: (NumberTypeClass, "SMALLFLOAT"),
    5: (NumberTypeClass, "DECIMAL"),
    6: (NumberTypeClass, "SERIAL"),
    7: (DateTypeClass, "DATE"),
    8: (NumberTypeClass, "MONEY"),
    10: (TimeTypeClass, "DATETIME"),
    11: (BytesTypeClass, "BYTE"),
    12: (StringTypeClass, "TEXT"),
    13: (StringTypeClass, "VARCHAR"),
    14: (TimeTypeClass, "INTERVAL"),
    15: (StringTypeClass, "NCHAR"),
    16: (StringTypeClass, "NVARCHAR"),
    17: (NumberTypeClass, "INT8"),
    18: (NumberTypeClass, "SERIAL8"),
    19: (RecordTypeClass, "SET"),
    20: (RecordTypeClass, "MULTISET"),
    21: (RecordTypeClass, "LIST"),
    # 40 (variable-length opaque/UDT: JSON, BSON, spatial) is intentionally
    # excluded — it has no single canonical native type, so it falls back to
    # NullTypeClass + UNKNOWN(40) via map_coltype's .get() default.
    43: (StringTypeClass, "LVARCHAR"),
    45: (BooleanTypeClass, "BOOLEAN"),
    52: (NumberTypeClass, "BIGINT"),
    53: (NumberTypeClass, "BIGSERIAL"),
}


def map_coltype(coltype: int) -> Tuple[SchemaFieldDataTypeClass, bool, str]:
    base = coltype & _BASE_TYPE_MASK
    nullable = (coltype & _NOT_NULL_BIT) == 0
    type_cls, native = INFORMIX_TYPE_MAP.get(base, (NullTypeClass, f"UNKNOWN({base})"))
    return SchemaFieldDataTypeClass(type=type_cls()), nullable, native


# tabid < 100 are reserved system-catalog objects; tabtype 'T' table, 'V' view.
SQL_TABLES = (
    "SELECT TRIM(tabname) AS tabname, TRIM(owner) AS owner, tabtype "
    "FROM systables WHERE tabid >= 100 AND tabtype IN ('T', 'V')"
)
SQL_COLUMNS = (
    "SELECT TRIM(c.colname) AS colname, c.coltype, c.collength, c.colno "
    "FROM syscolumns c JOIN systables t ON c.tabid = t.tabid "
    "WHERE TRIM(t.tabname) = ? AND TRIM(t.owner) = ? ORDER BY c.colno"
)
SQL_PK = (
    "SELECT TRIM(c.colname) AS colname "
    "FROM sysconstraints cn "
    "JOIN systables t ON cn.tabid = t.tabid "
    "JOIN sysindexes ix ON cn.idxname = ix.idxname "
    "JOIN syscolumns c ON c.tabid = t.tabid AND c.colno IN "
    # Descending index columns store partN as a negative colno, so ABS() is
    # required to match ascending and descending PK columns alike.
    "(ABS(ix.part1), ABS(ix.part2), ABS(ix.part3), ABS(ix.part4), ABS(ix.part5), "
    "ABS(ix.part6), ABS(ix.part7), ABS(ix.part8), ABS(ix.part9), ABS(ix.part10), "
    "ABS(ix.part11), ABS(ix.part12), ABS(ix.part13), ABS(ix.part14), ABS(ix.part15), "
    "ABS(ix.part16)) "
    "WHERE cn.constrtype = 'P' AND TRIM(t.tabname) = ? AND TRIM(t.owner) = ?"
)
