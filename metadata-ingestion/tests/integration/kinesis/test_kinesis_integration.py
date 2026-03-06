import os

import pytest

from datahub.ingestion.run.pipeline import Pipeline


@pytest.mark.integration
def test_kinesis_ingest(pytestconfig):
    # This test requires AWS credentials to be configured in the environment
    if not (
        os.environ.get("AWS_ACCESS_KEY_ID")
        and os.environ.get("AWS_SECRET_ACCESS_KEY")
        and os.environ.get("AWS_REGION")
    ):
        pytest.skip("AWS credentials not configured")

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "kinesis",
                "config": {
                    # Uses default Boto3 chain for credentials
                    # Or explicitly pass from env vars which we checked above
                    "aws_region": os.environ.get("AWS_REGION", "us-east-1"),
                    "pydantic_config": {"extra": "ignore"},
                },
            },
            "sink": {"type": "console"},
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
