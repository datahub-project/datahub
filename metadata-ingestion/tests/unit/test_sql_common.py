from typing import Dict
from unittest.mock import Mock

from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.api.source import Source
from datahub.ingestion.source.sql.sql_common import (
    PipelineContext,
    SQLAlchemyConfig,
    SQLAlchemySource,
)


class TestSQLAlchemyConfig(SQLAlchemyConfig):
    def get_sql_alchemy_url(self):
        pass


class TestSQLAlchemySource(SQLAlchemySource):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        pass


def test_generate_foreign_key():
    config: SQLAlchemyConfig = TestSQLAlchemyConfig()
    ctx: PipelineContext = PipelineContext(run_id="test_ctx")
    platform: str = "TEST"
    inspector: Inspector = Mock()
    source = TestSQLAlchemySource(config=config, ctx=ctx, platform=platform)
    fk_dict: Dict[str, str] = {
        "name": "test_constraint",
        "referred_table": "test_table",
        "referred_schema": "test_referred_schema",
        "constrained_columns": ["test_column"],  # type: ignore
        "referred_columns": ["test_referred_column"],  # type: ignore
    }
    foreign_key = source.get_foreign_key_metadata(
        dataset_urn="test_urn",
        schema="test_schema",
        fk_dict=fk_dict,
        inspector=inspector,
    )

    assert fk_dict.get("name") == foreign_key.name
    assert [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:TEST,test_referred_schema.test_table,PROD),test_referred_column)"
    ] == foreign_key.foreignFields
    assert ["urn:li:schemaField:(test_urn,test_column)"] == foreign_key.sourceFields


def test_use_source_schema_for_foreign_key_if_not_specified():
    config: SQLAlchemyConfig = TestSQLAlchemyConfig()
    ctx: PipelineContext = PipelineContext(run_id="test_ctx")
    platform: str = "TEST"
    inspector: Inspector = Mock()
    source = TestSQLAlchemySource(config=config, ctx=ctx, platform=platform)
    fk_dict: Dict[str, str] = {
        "name": "test_constraint",
        "referred_table": "test_table",
        "constrained_columns": ["test_column"],  # type: ignore
        "referred_columns": ["test_referred_column"],  # type: ignore
    }
    foreign_key = source.get_foreign_key_metadata(
        dataset_urn="test_urn",
        schema="test_schema",
        fk_dict=fk_dict,
        inspector=inspector,
    )

    assert fk_dict.get("name") == foreign_key.name
    assert [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:TEST,test_schema.test_table,PROD),test_referred_column)"
    ] == foreign_key.foreignFields
    assert ["urn:li:schemaField:(test_urn,test_column)"] == foreign_key.sourceFields
