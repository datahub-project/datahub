import boto3
from moto import mock_kinesis

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kinesis.kinesis import KinesisSource, KinesisSourceConfig


@mock_kinesis
def test_kinesis_source_extraction():
    region = "us-east-1"

    # Create mock Kinesis resources
    # Must use boto3 inside the test or fixture when mock_aws is active
    kinesis = boto3.client("kinesis", region_name=region)

    stream_name = "test-stream"
    kinesis.create_stream(StreamName=stream_name, ShardCount=2)

    # Add tags
    kinesis.add_tags_to_stream(
        StreamName=stream_name, Tags={"environment": "production", "owner": "data-team"}
    )

    # Configure Source
    params = {
        "connection": {
            "aws_region": region,
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
        },
        "region_name": region,
    }
    config = KinesisSourceConfig.parse_obj(params)
    ctx = PipelineContext(run_id="test-run")
    source = KinesisSource(config, ctx)

    # Execute
    workunits = list(source.get_workunits_internal())

    # Verify Workunits
    # We expect dataset urn: urn:li:dataset:(urn:li:dataPlatform:kinesis,test-stream,PROD)
    expected_urn = f"urn:li:dataset:(urn:li:dataPlatform:kinesis,{stream_name},PROD)"

    wus = [w for w in workunits if w.metadata.entityUrn == expected_urn]
    assert len(wus) > 0, f"No workunits found for {expected_urn}"

    # Check properties (shard count)
    props_wu = next(
        (w for w in wus if w.metadata.aspectName == "datasetProperties"), None
    )
    assert props_wu, "datasetProperties aspect mismatch"
    assert props_wu.metadata.aspect.customProperties["ShardCount"] == "2"

    # Check tags
    tags_wu = next((w for w in wus if w.metadata.aspectName == "globalTags"), None)
    assert tags_wu, "globalTags aspect mismatch"
    tags = [t.tag for t in tags_wu.metadata.aspect.tags]
    assert "urn:li:tag:environment:production" in tags
    assert "urn:li:tag:owner:data-team" in tags


@mock_kinesis
def test_kinesis_test_connection():
    region = "us-east-1"
    # Basic connectivity check
    # mock_aws ensures boto3 works
    # kinesis client creation ensures the mock is active
    boto3.client("kinesis", region_name=region)
    # No streams needed

    params = {
        "connection": {
            "aws_region": region,
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
        },
        "region_name": region,
    }

    report = KinesisSource.test_connection(params)
    assert report.basic_connectivity
    assert report.basic_connectivity.capable is True
