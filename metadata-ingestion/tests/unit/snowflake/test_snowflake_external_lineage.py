import json

from pydantic import SecretStr

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    SnowflakeLineageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator


def test_external_s3_location_with_reserved_chars_produces_lineage() -> None:
    config = SnowflakeV2Config(  # type: ignore[call-arg]
        account_id="test_account",
        username="test_user",
        password=SecretStr("test_password"),
    )
    identifiers = SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=SnowflakeV2Report()
    )
    mapping = SnowflakeLineageExtractor._process_external_lineage_result_row(
        {
            "DOWNSTREAM_TABLE_NAME": "MY_DB.MY_SCHEMA.EVENTS",
            "UPSTREAM_LOCATIONS": json.dumps(["s3://my-bucket/data/folder(1)/"]),
        },
        discovered_tables=None,
        identifiers=identifiers,
    )
    assert mapping is not None

    # A registered downstream schema makes the aggregator build column lineage,
    # which parses the upstream URN.
    aggregator = SqlParsingAggregator(platform="snowflake", generate_queries=False)
    aggregator.register_schema(
        mapping.downstream_urn,
        SchemaMetadataClass(
            schemaName="events",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                SchemaFieldClass(
                    fieldPath="col_a",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="VARCHAR",
                )
            ],
        ),
    )
    aggregator.add(mapping)

    lineage = [
        mcp.aspect
        for mcp in aggregator.gen_metadata()
        if isinstance(mcp.aspect, UpstreamLineageClass)
    ]
    assert len(lineage) == 1
    assert [u.dataset for u in lineage[0].upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data/folder%281%29,PROD)"
    ]
