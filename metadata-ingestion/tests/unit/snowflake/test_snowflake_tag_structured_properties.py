from typing import List
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeTag
from datahub.ingestion.source.snowflake.snowflake_tag import SnowflakeTagExtractor
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)

_SEMANTIC_MODEL = "urn:li:entityType:datahub.semanticModel"
_METRIC = "urn:li:entityType:datahub.metric"


def _entity_types(capable: bool) -> List[str]:
    config = SnowflakeV2Config(  # type: ignore[call-arg]
        account_id="test_account", username="user", password="pass"
    )
    report = SnowflakeV2Report()
    report.semantic_model_entity_types_capable = capable
    extractor = SnowflakeTagExtractor(
        config=config,
        data_dictionary=MagicMock(),
        report=report,
        snowflake_identifiers=SnowflakeIdentifierBuilder(config, report),
    )
    tag = SnowflakeTag(database="db", schema="sch", name="my_tag", value="v")
    aspects = [
        wu.metadata.aspect
        for wu in extractor.gen_tag_as_structured_property_workunits(tag)
        if getattr(wu.metadata, "aspect", None) is not None
    ]
    assert len(aspects) == 1
    return list(aspects[0].entityTypes)


def test_sp_entity_types_include_semantic_entities_when_capable():
    # #2: a capable server declares all five entity types, independent of the emit
    # decision, so the shared definition does not flap. The golden runs without a
    # graph (capable=False) and cannot cover this path, so it is tested explicitly.
    types = _entity_types(capable=True)
    assert _SEMANTIC_MODEL in types
    assert _METRIC in types
    assert len(types) == 5


def test_sp_entity_types_base_three_when_not_capable():
    types = _entity_types(capable=False)
    assert _SEMANTIC_MODEL not in types
    assert _METRIC not in types
    assert len(types) == 3
