import pytest

from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import INFORMIX_TYPE_MAP, map_coltype
from datahub.ingestion.source.informix.models import InformixForeignKey
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
