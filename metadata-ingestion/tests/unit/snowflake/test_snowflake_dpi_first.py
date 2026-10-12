from typing import List, Optional
from unittest.mock import MagicMock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)


def _dataset_aspect_names(platform_instance: Optional[str]) -> List[str]:
    config = SnowflakeV2Config(
        account_id="test_account",
        username="user",
        password="pass",  # type: ignore[arg-type]
        platform_instance=platform_instance,
    )
    report = SnowflakeV2Report()
    schema_gen = SnowflakeSchemaGenerator(
        config=config,
        report=report,
        connection=MagicMock(),
        filters=MagicMock(),
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        domain_registry=None,
        profiler=None,
        aggregator=MagicMock(),
        snowsight_url_builder=None,
    )
    table = SnowflakeTable(
        name="MY_TABLE",
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        columns=[
            SnowflakeColumn(
                name="COL_A",
                ordinal_position=1,
                is_nullable=True,
                data_type="TEXT",
                comment=None,
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
            )
        ],
    )
    return [
        wu.metadata.aspectName or ""
        for wu in schema_gen.gen_dataset_workunits(table, "MY_SCHEMA", "MY_DB")
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    ]


def test_table_emits_data_platform_instance_first() -> None:
    names = _dataset_aspect_names(platform_instance="my_instance")

    assert names[0] == "dataPlatformInstance"
    assert names.count("dataPlatformInstance") == 1
    assert {"status", "schemaMetadata", "container"} <= set(names[1:])


def test_table_without_instance_starts_with_status() -> None:
    names = _dataset_aspect_names(platform_instance=None)

    assert names[0] == "status"
