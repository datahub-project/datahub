from datahub.ingestion.source.informix.constants import map_coltype
from datahub.metadata.schema_classes import (
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)


def test_map_coltype_integer_notnull():
    # coltype 258 = INTEGER(2) + NOT NULL(256)
    dh_type, nullable, native = map_coltype(258)
    assert isinstance(dh_type.type, NumberTypeClass)
    assert nullable is False
    assert native == "INTEGER"


def test_map_coltype_varchar_nullable():
    # coltype 13 = VARCHAR, nullable
    dh_type, nullable, native = map_coltype(13)
    assert isinstance(dh_type.type, StringTypeClass)
    assert nullable is True
    assert native == "VARCHAR"


def test_map_coltype_unknown_falls_back_to_null():
    # 99 is not a known Informix base type -> NullType, still decodes NOT NULL bit
    dh_type, nullable, native = map_coltype(99 + 256)
    assert isinstance(dh_type.type, NullTypeClass)
    assert nullable is False
    assert native.startswith("UNKNOWN")


def test_config_minimal_parses():
    from datahub.ingestion.source.informix.config import InformixSourceConfig

    cfg = InformixSourceConfig.model_validate(
        {"server": "informix", "database": "testdb"}
    )
    assert cfg.host_port == "localhost:9088"
    assert cfg.accept_ibm_jdbc_license is False
    assert cfg.database_pattern.allowed("anything") is True
