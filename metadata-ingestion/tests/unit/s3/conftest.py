"""Shared fixtures and builders for the S3 source unit tests.

Buckets are real moto buckets: request the ``s3`` fixture (or mark a module with
``pytest.mark.usefixtures("s3")``) to run inside ``mock_aws``, then seed objects
through ``s3_client`` / ``s3_resource``. The fixtures are not autouse so modules
that stub boto differently (the probe and zip tests) are unaffected.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List

import pytest
from boto3.session import Session
from moto import mock_aws
from mypy_boto3_s3 import S3Client, S3ServiceResource

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.s3.source import S3Source, TableData

AWS_CONFIG: Dict[str, str] = {
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
    "aws_region": "us-east-1",
}
CSV_BODY = "a,b\n1,2\n3,4\n"


@pytest.fixture
def s3() -> Iterator[Session]:
    """A boto3 session whose every call is served by moto for this test."""
    with mock_aws():
        yield Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )


@pytest.fixture
def s3_client(s3: Session) -> S3Client:
    return s3.client("s3")


@pytest.fixture
def s3_resource(s3: Session) -> S3ServiceResource:
    return s3.resource("s3")


def make_s3_source(path_spec: Dict[str, Any], **config: Any) -> S3Source:
    """An S3Source for one raw ``path_spec`` plus any other top-level config."""
    return S3Source.create(
        config_dict={"path_spec": path_spec, **config},
        ctx=PipelineContext(run_id="test-s3"),
    )


def make_s3_source_for_spec(path_spec: PathSpec) -> S3Source:
    """An S3Source on moto credentials for an already-built PathSpec."""
    return make_s3_source(
        {
            "include": path_spec.include,
            "table_name": path_spec.table_name,
            "emit_folders_only": path_spec.emit_folders_only,
            "exclude": path_spec.exclude,
            "include_hidden_folders": path_spec.include_hidden_folders,
        },
        aws_config=AWS_CONFIG,
    )


def make_table_data(
    full_path: str, size: int = len(CSV_BODY), display_name: str = "t"
) -> TableData:
    """One single-file table at ``full_path``, as ``ingest_table`` receives it."""
    return TableData(
        display_name=display_name,
        is_s3=True,
        full_path=full_path,
        timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
        table_path=full_path,
        size_in_bytes=size,
        number_of_files=1,
    )


def dataset_urns(aspects: Dict[str, List[Any]]) -> List[str]:
    """The dataset urns among ``aspects_by_urn`` keys, sorted."""
    return sorted(u for u in aspects if u.startswith("urn:li:dataset:"))
