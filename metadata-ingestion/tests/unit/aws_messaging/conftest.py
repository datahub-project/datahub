from typing import Any, Dict, Iterator

import boto3
import pytest
from moto import mock_aws

REGION = "us-east-1"
ACCOUNT = "123456789012"


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    with mock_aws():
        yield


def base_config(**overrides: Any) -> Dict[str, Any]:
    return {"aws_config": {"aws_region": REGION}, **overrides}


def sqs_client() -> Any:
    return boto3.client("sqs", region_name=REGION)
