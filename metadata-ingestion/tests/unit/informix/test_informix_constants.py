import re

import pytest

from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import (
    INFORMIX_TYPE_MAP,
    SQL_COLUMNS,
    SQL_FK,
    SQL_PK,
    map_coltype,
)
from datahub.ingestion.source.informix.models import (
    ExtendedType,
    InformixForeignKey,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


def test_map_coltype_integer_notnull():
    # coltype 258 = INTEGER(2) + NOT NULL(256)
    mapped = map_coltype(258)
    assert isinstance(mapped.data_type.type, NumberTypeClass)
    assert mapped.nullable is False
    assert mapped.native == "INTEGER"


def test_map_coltype_varchar_nullable():
    # coltype 13 = VARCHAR, nullable
    mapped = map_coltype(13)
    assert isinstance(mapped.data_type.type, StringTypeClass)
    assert mapped.nullable is True
    assert mapped.native == "VARCHAR"


def test_map_coltype_unknown_falls_back_to_null():
    # 99 is not a known Informix base type -> NullType, still decodes NOT NULL bit
    mapped = map_coltype(99 + 256)
    assert isinstance(mapped.data_type.type, NullTypeClass)
    assert mapped.nullable is False
    assert mapped.native.startswith("UNKNOWN")


def test_map_coltype_extended_type_40_unknown():
    # base type 40 (variable-length opaque/UDT: JSON, BSON, spatial) is
    # deliberately excluded from the map; it must fall back to UNKNOWN(40).
    mapped = map_coltype(40)
    assert isinstance(mapped.data_type.type, NullTypeClass)
    assert mapped.native == "UNKNOWN(40)"


@pytest.mark.parametrize(
    "code,native,type_cls",
    [
        (0, "CHAR", StringTypeClass),
        (1, "SMALLINT", NumberTypeClass),
        (2, "INTEGER", NumberTypeClass),
        (3, "FLOAT", NumberTypeClass),
        (4, "SMALLFLOAT", NumberTypeClass),
        (5, "DECIMAL", NumberTypeClass),
        (6, "SERIAL", NumberTypeClass),
        (7, "DATE", DateTypeClass),
        (8, "MONEY", NumberTypeClass),
        (10, "DATETIME", TimeTypeClass),
        (11, "BYTE", BytesTypeClass),
        (12, "TEXT", StringTypeClass),
        (13, "VARCHAR", StringTypeClass),
        (14, "INTERVAL", TimeTypeClass),
        (15, "NCHAR", StringTypeClass),
        (16, "NVARCHAR", StringTypeClass),
        (17, "INT8", NumberTypeClass),
        (18, "SERIAL8", NumberTypeClass),
        (19, "SET", RecordTypeClass),
        (20, "MULTISET", RecordTypeClass),
        (21, "LIST", RecordTypeClass),
        (43, "LVARCHAR", StringTypeClass),
        (45, "BOOLEAN", BooleanTypeClass),
        (52, "BIGINT", NumberTypeClass),
        (53, "BIGSERIAL", NumberTypeClass),
    ],
)
def test_map_coltype_covers_every_mapped_code(
    code: int, native: str, type_cls: type
) -> None:
    mapped = map_coltype(code)
    assert mapped.native == native
    assert isinstance(mapped.data_type.type, type_cls)
    assert mapped.nullable is True
    # NOT NULL bit must flip nullability without changing the native name.
    not_null = map_coltype(code + 256)
    assert not_null.native == native
    assert not_null.nullable is False
    assert isinstance(not_null.data_type.type, type_cls)


def test_informix_type_map_keys_match_parametrized_coverage():
    # Guard against adding a map entry without extending the parametrized test.
    assert set(INFORMIX_TYPE_MAP) == {
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        43,
        45,
        52,
        53,
    }


@pytest.mark.parametrize(
    "coltype,xtdname,xtdmode,xtdsource,native,type_cls",
    [
        # Every row measured against Informix 15.0.1 (see the integration
        # fixture). Base coltype 40/41 carries no type information on its own.
        (40, "lvarchar", "B", None, "LVARCHAR", StringTypeClass),
        (41, "boolean", "B", None, "BOOLEAN", BooleanTypeClass),
        (41, "blob", "B", None, "BLOB", BytesTypeClass),
        (41, "clob", "B", None, "CLOB", StringTypeClass),
        # An internal built-in should never be a user column; keep its name but
        # do not invent a DataHub type for it.
        (41, "pointer", "B", None, "POINTER", NullTypeClass),
        # A DISTINCT over an ordinary built-in keeps it in the low byte of
        # coltype: 2053 = 2048 | 5 (DECIMAL), 2050 = 2048 | 2 (INTEGER). Its
        # sysxtdtypes.source is 0, so no source name comes back.
        (2053, "money_usd", "D", None, "MONEY_USD", NumberTypeClass),
        (2050, "cust_id", "D", None, "CUST_ID", NumberTypeClass),
        # A DISTINCT over an opaque built-in cannot use the low byte -- it is 40
        # or 41 there -- so it resolves through sysxtdtypes.source instead.
        # Measured: DISTINCT-of-BOOLEAN is 18473 and DISTINCT-of-LVARCHAR 10280
        # (both carry a dedicated coltype bit), but DISTINCT-of-BLOB and
        # DISTINCT-of-CLOB are *both* 2089, so only the source name tells them
        # apart.
        (18473, "flag_type", "D", "boolean", "FLAG_TYPE", BooleanTypeClass),
        (10280, "long_text", "D", "lvarchar", "LONG_TEXT", StringTypeClass),
        (2089, "doc_blob", "D", "blob", "DOC_BLOB", BytesTypeClass),
        (2089, "doc_text", "D", "clob", "DOC_TEXT", StringTypeClass),
        # A DISTINCT of a DISTINCT reports the intermediate type as its source,
        # which is not a built-in, so the chain is not walked further.
        (18473, "flag2_type", "D", "flag_type", "FLAG2_TYPE", NullTypeClass),
        # ROW types are structs; base code 22 has no mapping of its own.
        (20502, "addr_t", "R", None, "ADDR_T", RecordTypeClass),
        # Opaque types (mode 'O') have no DataHub equivalent, but the real name
        # still beats UNKNOWN(40).
        (40, "json", "O", None, "JSON", NullTypeClass),
        # Collections store an empty name; the base coltype already resolves.
        (19, "", "C", None, "SET", RecordTypeClass),
        (21, "", "C", None, "LIST", RecordTypeClass),
    ],
)
def test_map_coltype_resolves_extended_types(
    coltype: int,
    xtdname: str,
    xtdmode: str,
    xtdsource: str,
    native: str,
    type_cls: type,
) -> None:
    mapped = map_coltype(
        coltype=coltype,
        extended=ExtendedType(name=xtdname, mode=xtdmode, source_name=xtdsource),
    )
    assert mapped.native == native
    assert isinstance(mapped.data_type.type, type_cls)


def test_map_coltype_without_extended_type_uses_base_coltype():
    # extended_id 0 -> no sysxtdtypes row at all.
    mapped = map_coltype(coltype=2)
    assert mapped.native == "INTEGER"
    assert isinstance(mapped.data_type.type, NumberTypeClass)


def test_map_coltype_extended_resolution_preserves_not_null_bit():
    mapped = map_coltype(
        coltype=41 + 256, extended=ExtendedType(name="boolean", mode="B")
    )
    assert mapped.native == "BOOLEAN"
    assert mapped.nullable is False


def test_sql_columns_resolves_distinct_source_type():
    # sysxtdtypes.source is 0 for everything but a DISTINCT over another
    # extended type, so resolving its name needs a second outer self-join.
    assert SQL_COLUMNS.count("LEFT JOIN sysxtdtypes") == 2
    assert "LEFT JOIN sysxtdtypes xs ON x.source = xs.extended_id" in SQL_COLUMNS


def test_sql_columns_outer_joins_sysxtdtypes():
    # extended_id is 0 for ordinary types, so an inner join would silently drop
    # every non-extended column.
    assert "LEFT JOIN sysxtdtypes" in SQL_COLUMNS


def test_every_sysindexes_join_is_scoped_by_tabid():
    # A constraint's backing index must be looked up by (tabid, idxname). Joining
    # on idxname alone can attach another table's index parts, which would flag
    # the wrong columns as primary keys and mislink foreign key fields.
    for sql in (SQL_PK, SQL_FK):
        aliases = re.findall(r"JOIN sysindexes (\w+) ON", sql)
        assert aliases
        for alias in aliases:
            assert f".tabid = {alias}.tabid" in sql


def test_foreign_key_rejects_mismatched_column_counts():
    with pytest.raises(ValueError, match="mismatched column counts"):
        InformixForeignKey(
            name="fk_bad",
            child_columns=["a", "b"],
            parent_table="parent",
            parent_owner="informix",
            parent_columns=["a"],
        )


def test_view_pattern_inherits_table_pattern_unless_specified():
    cfg = InformixSourceConfig.model_validate(
        {
            "server": "informix",
            "database": "testdb",
            "table_pattern": {"deny": ["testdb.informix.tmp.*"]},
        }
    )
    assert cfg.view_pattern.deny == ["testdb.informix.tmp.*"]


def test_view_pattern_preserved_when_explicitly_set():
    # The validator must not clobber an explicitly-provided view_pattern.
    cfg = InformixSourceConfig.model_validate(
        {
            "server": "informix",
            "database": "testdb",
            "table_pattern": {"deny": ["testdb.informix.tmp.*"]},
            "view_pattern": {"deny": ["testdb.informix.v_.*"]},
        }
    )
    assert cfg.view_pattern.deny == ["testdb.informix.v_.*"]
    assert cfg.table_pattern.deny == ["testdb.informix.tmp.*"]
