import pytest
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.tidb import TiDBConfig, TiDBSource


def test_platform_correctly_set_tidb():
    source = TiDBSource(
        ctx=PipelineContext(run_id="tidb-source-test"),
        config=TiDBConfig(),
    )
    assert source.platform == "tidb"


def test_tidb_config_defaults():
    config = TiDBConfig()
    assert config.host_port == "localhost:4000"
    assert config.include_stored_procedures is False


def test_tidb_config_rejects_aws_iam_auth_mode():
    with pytest.raises(
        ValidationError, match="TiDB only supports password authentication"
    ):
        TiDBConfig.model_validate({"auth_mode": "AWS_IAM"})
