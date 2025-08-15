import pathlib
import re
from unittest import mock

import pytest
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.data_lake_common.data_lake_utils import PLATFORM_GCS
from datahub.ingestion.source.gcs.gcs_source import GCSSource


def test_gcs_source_setup():
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    # Baseline: valid config
    source: dict = {
        "path_specs": [
            {
                "include": "gs://bucket_name/{table}/year={partition[0]}/month={partition[1]}/day={partition[1]}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
        "stateful_ingestion": {"enabled": "true"},
    }
    gcs = GCSSource.create(source, ctx)
    assert gcs.s3_source.source_config.platform == PLATFORM_GCS
    assert (
        gcs.s3_source.create_s3_path(
            "bucket-name", "food_parquet/year%3D2023/month%3D4/day%3D24/part1.parquet"
        )
        == "gs://bucket-name/food_parquet/year=2023/month=4/day=24/part1.parquet"
    )


def test_data_lake_incorrect_config_raises_error():
    ctx = PipelineContext(run_id="test-gcs")

    # Case 1 : named variable in table name is not present in include
    source = {
        "path_specs": [{"include": "gs://a/b/c/d/{table}.*", "table_name": "{table1}"}],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }
    with pytest.raises(ValidationError, match="table_name"):
        GCSSource.create(source, ctx)

    # Case 2 : named variable in exclude is not allowed
    source = {
        "path_specs": [
            {
                "include": "gs://a/b/c/d/{table}/*.*",
                "exclude": ["gs://a/b/c/d/a-{exclude}/**"],
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }
    with pytest.raises(ValidationError, match=r"exclude.*named variable"):
        GCSSource.create(source, ctx)

    # Case 3 : unsupported file type not allowed
    source = {
        "path_specs": [
            {
                "include": "gs://a/b/c/d/{table}/*.hd5",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }
    with pytest.raises(ValidationError, match="file type"):
        GCSSource.create(source, ctx)

    # Case 4 : ** in include not allowed
    source = {
        "path_specs": [
            {
                "include": "gs://a/b/c/d/**/*.*",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }
    with pytest.raises(ValidationError, match=r"\*\*"):
        GCSSource.create(source, ctx)


def test_gcs_uri_normalization_fix():
    """Test that GCS URIs are normalized correctly for pattern matching."""
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    # Create a GCS source with a path spec that includes table templating
    source = {
        "path_specs": [
            {
                "include": "gs://test-bucket/data/{table}/year={partition[0]}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }

    gcs_source = GCSSource.create(source, ctx)

    # Check that the S3 source has the URI normalization method
    assert hasattr(gcs_source.s3_source, "_normalize_uri_for_pattern_matching")
    # Check that strip_s3_prefix is overridden for GCS
    assert hasattr(gcs_source.s3_source, "strip_s3_prefix")

    # Test URI normalization
    gs_uri = "gs://test-bucket/data/food_parquet/year=2023/file.parquet"
    normalized_uri = gcs_source.s3_source._normalize_uri_for_pattern_matching(gs_uri)
    assert normalized_uri == "s3://test-bucket/data/food_parquet/year=2023/file.parquet"

    # Test prefix stripping
    stripped_uri = gcs_source.s3_source.strip_s3_prefix(gs_uri)
    assert stripped_uri == "test-bucket/data/food_parquet/year=2023/file.parquet"


@pytest.mark.parametrize(
    "gs_uri,expected_normalized,expected_stripped",
    [
        (
            "gs://test-bucket/data/food_parquet/year=2023/file.parquet",
            "s3://test-bucket/data/food_parquet/year=2023/file.parquet",
            "test-bucket/data/food_parquet/year=2023/file.parquet",
        ),
        (
            "gs://my-bucket/simple/file.json",
            "s3://my-bucket/simple/file.json",
            "my-bucket/simple/file.json",
        ),
        (
            "gs://bucket/nested/deep/path/data.csv",
            "s3://bucket/nested/deep/path/data.csv",
            "bucket/nested/deep/path/data.csv",
        ),
    ],
)
def test_gcs_uri_transformations(gs_uri, expected_normalized, expected_stripped):
    """Test GCS URI normalization and prefix stripping with various inputs."""
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    source = {
        "path_specs": [
            {
                "include": "gs://test-bucket/data/{table}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }

    gcs_source = GCSSource.create(source, ctx)

    # Test URI normalization
    normalized_uri = gcs_source.s3_source._normalize_uri_for_pattern_matching(gs_uri)
    assert normalized_uri == expected_normalized

    # Test prefix stripping
    stripped_uri = gcs_source.s3_source.strip_s3_prefix(gs_uri)
    assert stripped_uri == expected_stripped


def test_gcs_path_spec_pattern_matching():
    """Test that GCS path specs correctly match files after URI normalization."""
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    # Create a GCS source
    source = {
        "path_specs": [
            {
                "include": "gs://test-bucket/data/{table}/year={partition[0]}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }

    gcs_source = GCSSource.create(source, ctx)

    # Get the path spec that was converted to S3 format
    s3_path_spec = gcs_source.s3_source.source_config.path_specs[0]

    # The path spec should have been converted to S3 format
    assert (
        s3_path_spec.include
        == "s3://test-bucket/data/{table}/year={partition[0]}/*.parquet"
    )

    # Test that a GCS file URI would be normalized for pattern matching
    gs_file_uri = "gs://test-bucket/data/food_parquet/year=2023/file.parquet"
    normalized_uri = gcs_source.s3_source._normalize_uri_for_pattern_matching(
        gs_file_uri
    )

    # Convert the path spec pattern to glob format (similar to what PathSpec.glob_include does)
    glob_pattern = re.sub(r"\{[^}]+\}", "*", s3_path_spec.include)
    assert pathlib.PurePath(normalized_uri).match(glob_pattern)


def test_gcs_source_preserves_gs_uris():
    """Test that GCS source preserves gs:// URIs in the final output."""
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    # Create a GCS source
    source = {
        "path_specs": [
            {
                "include": "gs://test-bucket/data/{table}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }

    gcs_source = GCSSource.create(source, ctx)

    # Test that create_s3_path creates GCS URIs
    gcs_path = gcs_source.s3_source.create_s3_path(
        "test-bucket", "data/food_parquet/file.parquet"
    )
    assert gcs_path == "gs://test-bucket/data/food_parquet/file.parquet"

    # Test that the platform is correctly set
    assert gcs_source.s3_source.source_config.platform == PLATFORM_GCS

    # Test that container subtypes are correctly set for GCS
    from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes

    container_creator = gcs_source.s3_source.container_WU_creator
    assert container_creator.get_sub_types() == DatasetContainerSubTypes.GCS_BUCKET


def test_gcs_container_subtypes():
    """Test that GCS containers use 'GCS bucket' subtype instead of 'S3 bucket'."""
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    source = {
        "path_specs": [
            {
                "include": "gs://test-bucket/data/{table}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }

    gcs_source = GCSSource.create(source, ctx)

    # Verify the platform is set correctly
    assert gcs_source.s3_source.source_config.platform == PLATFORM_GCS

    # Verify container subtypes use GCS bucket, not S3 bucket
    from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes

    container_creator = gcs_source.s3_source.container_WU_creator

    # Should return "GCS bucket" for GCS platform
    assert container_creator.get_sub_types() == DatasetContainerSubTypes.GCS_BUCKET
    assert container_creator.get_sub_types() == "GCS bucket"

    # Should NOT return "S3 bucket"
    assert container_creator.get_sub_types() != DatasetContainerSubTypes.S3_BUCKET
    assert container_creator.get_sub_types() != "S3 bucket"
