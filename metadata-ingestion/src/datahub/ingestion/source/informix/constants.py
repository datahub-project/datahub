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
# nrows is an approximate, catalog-maintained row count (-1/0 means unknown).
SQL_TABLES = (
    "SELECT TRIM(tabname) AS tabname, TRIM(owner) AS owner, tabtype, nrows "
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
# constrtype = 'R' is a referential (foreign key) constraint. The child and
# parent index columns are each joined via the same ABS(partN) IN(...) pattern
# as SQL_PK; for composite keys this yields a cross product of child/parent
# columns rather than pairwise-ordered rows, so client.get_foreign_keys() can
# only pair them best-effort (see its docstring/comment).
SQL_FK = (
    "SELECT TRIM(cn.constrname) AS fkname, TRIM(cc.colname) AS child_col, "
    "TRIM(pt.tabname) AS parent_table, TRIM(pt.owner) AS parent_owner, "
    "TRIM(pc.colname) AS parent_col "
    "FROM sysconstraints cn "
    "JOIN systables ct ON cn.tabid = ct.tabid "
    "JOIN sysindexes cix ON cn.idxname = cix.idxname "
    "JOIN syscolumns cc ON cc.tabid = ct.tabid AND cc.colno IN "
    "(ABS(cix.part1),ABS(cix.part2),ABS(cix.part3),ABS(cix.part4),ABS(cix.part5),"
    "ABS(cix.part6),ABS(cix.part7),ABS(cix.part8),ABS(cix.part9),ABS(cix.part10),"
    "ABS(cix.part11),ABS(cix.part12),ABS(cix.part13),ABS(cix.part14),ABS(cix.part15),"
    "ABS(cix.part16)) "
    "JOIN sysreferences r ON cn.constrid = r.constrid "
    "JOIN sysconstraints pcn ON r.primary = pcn.constrid "
    "JOIN systables pt ON pcn.tabid = pt.tabid "
    "JOIN sysindexes pix ON pcn.idxname = pix.idxname "
    "JOIN syscolumns pc ON pc.tabid = pt.tabid AND pc.colno IN "
    "(ABS(pix.part1),ABS(pix.part2),ABS(pix.part3),ABS(pix.part4),ABS(pix.part5),"
    "ABS(pix.part6),ABS(pix.part7),ABS(pix.part8),ABS(pix.part9),ABS(pix.part10),"
    "ABS(pix.part11),ABS(pix.part12),ABS(pix.part13),ABS(pix.part14),ABS(pix.part15),"
    "ABS(pix.part16)) "
    "WHERE cn.constrtype = 'R' AND TRIM(ct.tabname) = ? AND TRIM(ct.owner) = ?"
)
