import json
from typing import Union
from unittest.mock import MagicMock, Mock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
)


def test_file_sink_local_write_backward_compatibility(tmp_path):
    """Test that FileSink writes to local files maintaining backward compatibility."""
    output_file = tmp_path / "test_output.json"
    config = FileSinkConfig(filename=str(output_file))
    sink = FileSink(config=config, ctx=MagicMock())

    # Create a MetadataChangeProposalWrapper
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        aspect=DatasetPropertiesClass(description="test dataset"),
    )

    # Create record envelope with proper type
    record_envelope: RecordEnvelope[
        Union[
            MetadataChangeEventClass,
            MetadataChangeProposalClass,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})

    sink.write_record_async(record_envelope, write_callback=MagicMock())
    sink.close()

    # Verify file was created and has correct content
    assert output_file.exists()
    with output_file.open() as f:
        content = json.load(f)

    assert len(content) == 1
    assert (
        content[0]["entityUrn"]
        == "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
    )
    assert content[0]["aspect"]["json"]["description"] == "test dataset"


def test_file_sink_empty_records(tmp_path):
    """Test that FileSink handles empty records correctly."""
    output_file = tmp_path / "empty_output.json"
    config = FileSinkConfig(filename=str(output_file))
    sink = FileSink(config=config, ctx=MagicMock())

    # Don't write any records, just close
    sink.close()

    # Verify file contains empty array
    assert output_file.exists()
    with output_file.open() as f:
        content = json.load(f)
    assert content == []


def test_file_sink_legacy_nested_json_string(tmp_path):
    """Test that FileSink supports legacy nested JSON string format."""
    output_file = tmp_path / "legacy_output.json"
    config = FileSinkConfig(filename=str(output_file), legacy_nested_json_string=True)
    sink = FileSink(config=config, ctx=MagicMock())

    # Create a MetadataChangeProposalWrapper
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        aspect=DatasetPropertiesClass(description="test dataset"),
    )

    # Create record envelope with proper type
    record_envelope: RecordEnvelope[
        Union[
            MetadataChangeEventClass,
            MetadataChangeProposalClass,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})

    sink.write_record_async(record_envelope, write_callback=MagicMock())
    sink.close()

    # Verify file was created and has correct content
    assert output_file.exists()
    with output_file.open() as f:
        content = json.load(f)

    # In legacy mode, the aspect should be a nested string
    assert len(content) == 1
    assert "aspect" in content[0]


@patch("datahub.ingestion.fs.s3_fs.boto3")
def test_file_sink_s3_write(mock_boto3, tmp_path):
    """Test that FileSink can write to S3."""
    # Mock the S3 client
    mock_s3_client = Mock()
    mock_boto3.client.return_value = mock_s3_client

    # Mock successful put_object response
    mock_s3_client.put_object.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }

    config = FileSinkConfig(filename="s3://test-bucket/metadata/output.json")
    sink = FileSink(config=config, ctx=MagicMock())

    # Create a MetadataChangeProposalWrapper
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        aspect=DatasetPropertiesClass(description="test dataset"),
    )

    # Create record envelope with proper type
    record_envelope: RecordEnvelope[
        Union[
            MetadataChangeEventClass,
            MetadataChangeProposalClass,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})

    sink.write_record_async(record_envelope, write_callback=MagicMock())
    sink.close()

    # Verify S3 client was called correctly
    mock_boto3.client.assert_called_once_with("s3")
    mock_s3_client.put_object.assert_called_once()

    # Check the call arguments
    call_args = mock_s3_client.put_object.call_args
    assert call_args.kwargs["Bucket"] == "test-bucket"
    assert call_args.kwargs["Key"] == "metadata/output.json"

    # Verify the content is valid JSON
    content = call_args.kwargs["Body"].decode("utf-8")
    parsed_content = json.loads(content)
    assert len(parsed_content) == 1


@patch("datahub.ingestion.fs.s3_fs.boto3")
def test_file_sink_s3_write_failure(mock_boto3, tmp_path):
    """Test that FileSink handles S3 write failures gracefully."""
    # Mock the S3 client to raise an exception
    mock_s3_client = Mock()
    mock_boto3.client.return_value = mock_s3_client
    mock_s3_client.put_object.side_effect = Exception("S3 write failed")

    config = FileSinkConfig(filename="s3://test-bucket/metadata/output.json")
    sink = FileSink(config=config, ctx=MagicMock())

    # Create a MetadataChangeProposalWrapper
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        aspect=DatasetPropertiesClass(description="test dataset"),
    )

    # Create record envelope with proper type
    record_envelope: RecordEnvelope[
        Union[
            MetadataChangeEventClass,
            MetadataChangeProposalClass,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})

    sink.write_record_async(record_envelope, write_callback=MagicMock())

    # close() should raise the exception
    with pytest.raises(Exception, match="S3 write failed"):
        sink.close()


def test_file_sink_creates_parent_directories(tmp_path):
    """Test that FileSink creates parent directories when writing to local files."""
    nested_output_file = tmp_path / "nested" / "dir" / "output.json"
    config = FileSinkConfig(filename=str(nested_output_file))
    sink = FileSink(config=config, ctx=MagicMock())

    # Create a MetadataChangeProposalWrapper
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        aspect=DatasetPropertiesClass(description="test dataset"),
    )

    # Create record envelope with proper type
    record_envelope: RecordEnvelope[
        Union[
            MetadataChangeEventClass,
            MetadataChangeProposalClass,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})

    sink.write_record_async(record_envelope, write_callback=MagicMock())
    sink.close()

    # Verify nested directories were created
    assert nested_output_file.exists()
    assert nested_output_file.parent.exists()

    # Verify content
    with nested_output_file.open() as f:
        content = json.load(f)
    assert len(content) == 1
