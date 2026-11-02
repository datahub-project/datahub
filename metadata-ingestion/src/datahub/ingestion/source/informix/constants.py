from typing import Dict, Optional

from datahub.ingestion.source.informix.models import (
    ExtendedType,
    InformixType,
    MappedColumn,
)
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


# sysxtdtypes.mode says what kind of extended type an extended_id refers to.
_XTD_BUILTIN = "B"  # server built-in: lvarchar, boolean, blob, clob, ...
_XTD_DISTINCT = "D"  # CREATE DISTINCT TYPE
_XTD_ROW = "R"  # CREATE ROW TYPE
_XTD_COLLECTION = "C"  # SET/LIST/MULTISET; sysxtdtypes.name is empty for these

# Built-in extended types that can legitimately be a user column, keyed by
# sysxtdtypes.name. Their base coltype is 40 or 41, which says only "some opaque
# type" -- without the sysxtdtypes name, LVARCHAR, BOOLEAN, BLOB and CLOB are
# indistinguishable. The same map resolves a DISTINCT declared over one of them.
_XTD_BUILTIN_TYPE_MAP: Dict[str, type] = {
    "LVARCHAR": StringTypeClass,
    "BOOLEAN": BooleanTypeClass,
    "BLOB": BytesTypeClass,
    "CLOB": StringTypeClass,
}


def _resolve_extended_type(
    mapped: InformixType, extended: Optional[ExtendedType]
) -> InformixType:
    """Recover a real type name from sysxtdtypes, falling back to the base type.

    Verified against Informix 15.0.1: LVARCHAR is coltype 40 with extended_id 1,
    while BOOLEAN/BLOB/CLOB all share coltype 41, so the base code alone resolves
    every one of them to UNKNOWN. DISTINCT and ROW types carry a user-defined
    name that is more informative than their base code.
    """
    if extended is None or not extended.name or extended.mode == _XTD_COLLECTION:
        # Collections store an empty name, and their base coltype (SET, LIST,
        # MULTISET) is already correct.
        return mapped
    native = extended.name.upper()
    if extended.mode == _XTD_BUILTIN:
        # An unmapped built-in is an internal one (pointer, sendrecv, ...) that
        # should never surface as a user column. Keep the real name; leave the
        # DataHub type unresolved.
        return InformixType(_XTD_BUILTIN_TYPE_MAP.get(native, NullTypeClass), native)
    if extended.mode == _XTD_ROW:
        return InformixType(RecordTypeClass, native)
    if extended.mode == _XTD_DISTINCT:
        # A DISTINCT over an ordinary built-in keeps it in coltype's low byte
        # (2053 = 2048 | 5, DECIMAL), so `mapped` is already right. A DISTINCT
        # over an opaque built-in cannot be read that way: measured on 15.0.1,
        # DISTINCT-of-BLOB and DISTINCT-of-CLOB are *both* coltype 2089 (low
        # byte 41), so no coltype bit tells them apart. sysxtdtypes.source names
        # the type it was declared over, which resolves all four.
        #
        # A DISTINCT of a DISTINCT reports the intermediate type as its source,
        # so a chain over an opaque built-in still falls back to NullType. That
        # would need a recursive walk of sysxtdtypes.source for no known gain.
        source = (extended.source_name or "").upper()
        return InformixType(
            _XTD_BUILTIN_TYPE_MAP.get(source, mapped.datahub_type), native
        )
    # Opaque (mode 'O': JSON, BSON, spatial, user-defined UDTs) and anything
    # else. The real type name still beats UNKNOWN(40).
    return InformixType(NullTypeClass, native)


def map_coltype(coltype: int, extended: Optional[ExtendedType] = None) -> MappedColumn:
    base = coltype & _BASE_TYPE_MASK
    mapped = INFORMIX_TYPE_MAP.get(
        base, InformixType(NullTypeClass, f"UNKNOWN({base})")
    )
    resolved = _resolve_extended_type(mapped, extended)
    return MappedColumn(
        data_type=SchemaFieldDataTypeClass(type=resolved.datahub_type()),
        nullable=(coltype & _NOT_NULL_BIT) == 0,
        native=resolved.native_name,
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
# syscolumns.extended_id is 0 for ordinary types, so the sysxtdtypes lookup has
# to be an outer join. sysxtdtypes.name/mode recover LVARCHAR, BOOLEAN, BLOB,
# CLOB, DISTINCT, ROW and opaque types, all of which base coltype cannot express.
# The second, self-referential outer join resolves sysxtdtypes.source -- the type
# a DISTINCT was declared over -- back to its name; it is 0 (and so matches no
# row, sysxtdtypes has no extended_id 0) for every other mode.
SQL_COLUMNS = (
    "SELECT TRIM(c.colname) AS colname, c.coltype, c.collength, c.colno, "
    "TRIM(x.name) AS xtdname, x.mode AS xtdmode, TRIM(xs.name) AS xtdsource "
    "FROM syscolumns c JOIN systables t ON c.tabid = t.tabid "
    "LEFT JOIN sysxtdtypes x ON c.extended_id = x.extended_id "
    "LEFT JOIN sysxtdtypes xs ON x.source = xs.extended_id "
    "WHERE TRIM(t.tabname) = ? AND TRIM(t.owner) = ? ORDER BY c.colno"
)
# A constraint's backing index is looked up by (tabid, idxname), not idxname
# alone: that is the join IBM's catalog documentation specifies, and it keeps
# resolution correct even where an index name is not unique database-wide.
SQL_PK = (
    "SELECT TRIM(c.colname) AS colname "
    "FROM sysconstraints cn "
    "JOIN systables t ON cn.tabid = t.tabid "
    "JOIN sysindexes ix ON cn.tabid = ix.tabid AND cn.idxname = ix.idxname "
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
    "JOIN sysindexes cix ON cn.tabid = cix.tabid AND cn.idxname = cix.idxname "
    "JOIN syscolumns cc ON cc.tabid = ct.tabid AND cc.colno IN "
    f"({_index_part_cols('cix')}) "
    "JOIN sysreferences r ON cn.constrid = r.constrid "
    "JOIN sysconstraints pcn ON r.primary = pcn.constrid "
    "JOIN systables pt ON pcn.tabid = pt.tabid "
    "JOIN sysindexes pix ON pcn.tabid = pix.tabid AND pcn.idxname = pix.idxname "
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
