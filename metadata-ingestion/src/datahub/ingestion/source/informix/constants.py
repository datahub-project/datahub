from typing import Dict

from datahub.ingestion.source.informix.models import InformixType, MappedColumn
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

DRIVER_CLASS = "com.informix.jdbc.IfxDriver"

# systables.tabtype discriminates base tables from views.
TABTYPE_TABLE = "T"
TABTYPE_VIEW = "V"

# Informix syscolumns.coltype: low byte (coltype & 0xFF) is the base type code;
# bit 0x100 (256) is the NOT NULL flag. See IBM Informix SQL Reference (SYSCOLUMNS).
_NOT_NULL_BIT = 0x100
_BASE_TYPE_MASK = 0xFF

INFORMIX_TYPE_MAP: Dict[int, InformixType] = {
    0: InformixType(StringTypeClass, "CHAR"),
    1: InformixType(NumberTypeClass, "SMALLINT"),
    2: InformixType(NumberTypeClass, "INTEGER"),
    3: InformixType(NumberTypeClass, "FLOAT"),
    4: InformixType(NumberTypeClass, "SMALLFLOAT"),
    5: InformixType(NumberTypeClass, "DECIMAL"),
    6: InformixType(NumberTypeClass, "SERIAL"),
    7: InformixType(DateTypeClass, "DATE"),
    8: InformixType(NumberTypeClass, "MONEY"),
    10: InformixType(TimeTypeClass, "DATETIME"),
    11: InformixType(BytesTypeClass, "BYTE"),
    12: InformixType(StringTypeClass, "TEXT"),
    13: InformixType(StringTypeClass, "VARCHAR"),
    14: InformixType(TimeTypeClass, "INTERVAL"),
    15: InformixType(StringTypeClass, "NCHAR"),
    16: InformixType(StringTypeClass, "NVARCHAR"),
    17: InformixType(NumberTypeClass, "INT8"),
    18: InformixType(NumberTypeClass, "SERIAL8"),
    19: InformixType(RecordTypeClass, "SET"),
    20: InformixType(RecordTypeClass, "MULTISET"),
    21: InformixType(RecordTypeClass, "LIST"),
    # 40 (variable-length opaque/UDT: JSON, BSON, spatial) is intentionally
    # excluded — it has no single canonical native type, so it falls back to
    # NullTypeClass + UNKNOWN(40) via map_coltype's .get() default.
    43: InformixType(StringTypeClass, "LVARCHAR"),
    45: InformixType(BooleanTypeClass, "BOOLEAN"),
    52: InformixType(NumberTypeClass, "BIGINT"),
    53: InformixType(NumberTypeClass, "BIGSERIAL"),
}


def map_coltype(coltype: int) -> MappedColumn:
    base = coltype & _BASE_TYPE_MASK
    mapped = INFORMIX_TYPE_MAP.get(
        base, InformixType(NullTypeClass, f"UNKNOWN({base})")
    )
    return MappedColumn(
        data_type=SchemaFieldDataTypeClass(type=mapped.datahub_type()),
        nullable=(coltype & _NOT_NULL_BIT) == 0,
        native=mapped.native_name,
    )


# sysindexes stores an index's key columns as 16 fixed part1..part16 colno slots.
# Descending index columns store partN as a negative colno, so ABS() is required to
# match ascending and descending key columns alike.
def _index_part_cols(alias: str) -> str:
    return ", ".join(f"ABS({alias}.part{n})" for n in range(1, 17))


# tabid < 100 are reserved system-catalog objects; tabtype 'T' table, 'V' view.
# nrows is an approximate, catalog-maintained row count (-1/0 means unknown).
SQL_TABLES = (
    "SELECT TRIM(tabname) AS tabname, TRIM(owner) AS owner, tabtype, nrows "
    f"FROM systables WHERE tabid >= 100 AND tabtype IN ('{TABTYPE_TABLE}', '{TABTYPE_VIEW}')"
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
    f"({_index_part_cols('ix')}) "
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
    f"({_index_part_cols('cix')}) "
    "JOIN sysreferences r ON cn.constrid = r.constrid "
    "JOIN sysconstraints pcn ON r.primary = pcn.constrid "
    "JOIN systables pt ON pcn.tabid = pt.tabid "
    "JOIN sysindexes pix ON pcn.idxname = pix.idxname "
    "JOIN syscolumns pc ON pc.tabid = pt.tabid AND pc.colno IN "
    f"({_index_part_cols('pix')}) "
    "WHERE cn.constrtype = 'R' AND TRIM(ct.tabname) = ? AND TRIM(ct.owner) = ?"
)
# View text is stored across multiple rows (Informix caps each viewtext row at
# 64 chars); seqno gives the chunk order to reconstruct the full CREATE VIEW SQL.
SQL_VIEW_DEF = (
    "SELECT viewtext FROM sysviews v JOIN systables t ON v.tabid = t.tabid "
    "WHERE TRIM(t.tabname) = ? AND TRIM(t.owner) = ? ORDER BY v.seqno"
)
