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
        == "s3://bucket-name/food_parquet/year=2023/month=4/day=24/part1.parquet"
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
