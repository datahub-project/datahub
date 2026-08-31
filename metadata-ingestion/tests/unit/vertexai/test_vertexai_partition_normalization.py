import pytest

from datahub.ingestion.source.vertexai.vertexai_builder import VertexAIURIParser


@pytest.mark.parametrize(
    "uri,expected",
    [
        # Hive-style partitions
        ("gs://bucket/data/year=2024/month=01/file.parquet", "gs://bucket/data/"),
        (
            "s3://bucket/data/year=2024/month=01/day=15/data.json",
            "s3://bucket/data/",
        ),
        # Date partitions (dt=YYYY-MM-DD)
        ("s3://bucket/logs/dt=2024-01-15/data.json", "s3://bucket/logs/"),
        ("gs://bucket/events/dt=2024-12-31/events.csv", "gs://bucket/events/"),
        # Date hierarchy (YYYY/MM/DD)
        ("gs://bucket/events/2024/01/15/events.csv", "gs://bucket/events/"),
        ("s3a://bucket/logs/2024/12/31/log.txt", "s3a://bucket/logs/"),
        # Non-partitioned paths
        (
            "gs://bucket/raw/no-partitions/file.parquet",
            "gs://bucket/raw/no-partitions/",
        ),
        ("s3://bucket/data/models/model.pkl", "s3://bucket/data/models/"),
        # Multiple partition levels
        (
            "gs://bucket/data/year=2024/month=01/day=15/hour=12/file.parquet",
            "gs://bucket/data/",
        ),
        # Directory without file
        ("gs://bucket/data/year=2024/month=01/", "gs://bucket/data/"),
    ],
)
def test_partition_normalization_enabled(uri: str, expected: str) -> None:
    parser = VertexAIURIParser(
        env="PROD",
        normalize_paths=True,
        partition_patterns=[
            r"/[^/]+=([^/]+)",
            r"/dt=\d{4}-\d{2}-\d{2}",
            r"/\d{4}/\d{2}/\d{2}",
        ],
    )

    result = parser._strip_partition_segments(uri)
    assert result == expected


@pytest.mark.parametrize(
    "uri",
    [
        "gs://bucket/data/year=2024/month=01/file.parquet",
        "s3://bucket/logs/dt=2024-01-15/data.json",
        "gs://bucket/events/2024/01/15/events.csv",
    ],
)
def test_partition_normalization_disabled_preserves_full_path(uri: str) -> None:
    """When normalization is disabled, paths should be preserved exactly."""
    parser = VertexAIURIParser(
        env="PROD",
        normalize_paths=False,
        partition_patterns=[],
    )

    result = parser._strip_partition_segments(uri)
    assert result == uri


def test_invalid_regex_pattern_skipped() -> None:
    """Invalid regex patterns should be logged and skipped during initialization."""
    parser = VertexAIURIParser(
        env="PROD",
        normalize_paths=True,
        partition_patterns=[
            r"/[^/]+=([^/]+)",  # Valid
            r"/[invalid(regex/",  # Invalid
            r"/\d{4}/\d{2}/\d{2}",  # Valid
        ],
    )

    # Should compile 2 valid patterns, skip 1 invalid
    assert len(parser.compiled_partition_patterns) == 2


def test_dataset_urn_generation_with_normalization() -> None:
    """Test full dataset URN generation with partition normalization enabled."""
    parser = VertexAIURIParser(
        env="PROD",
        normalize_paths=True,
        partition_patterns=[r"/[^/]+=([^/]+)"],
    )

    # GCS partitioned path
    urns = parser.dataset_urns_from_artifact_uri(
        "gs://bucket/data/year=2024/month=01/file.parquet"
    )
    assert len(urns) == 1
    assert urns[0] == "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/data/,PROD)"

    # S3 partitioned path
    urns = parser.dataset_urns_from_artifact_uri(
        "s3://bucket/data/year=2024/month=01/file.parquet"
    )
    assert len(urns) == 1
    assert urns[0] == "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/data/,PROD)"


def test_dataset_urn_generation_without_normalization() -> None:
    """Test dataset URN generation preserves full paths when normalization is disabled."""
    parser = VertexAIURIParser(
        env="PROD",
        normalize_paths=False,
        partition_patterns=[],
    )

    urns = parser.dataset_urns_from_artifact_uri(
        "gs://bucket/data/year=2024/month=01/file.parquet"
    )
    assert len(urns) == 1
    assert (
        urns[0]
        == "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/data/year=2024/month=01/file.parquet,PROD)"
    )
