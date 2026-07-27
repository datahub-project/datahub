from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import map_coltype
from datahub.metadata.schema_classes import (
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
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
