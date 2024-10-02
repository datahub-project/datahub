from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.risingwave import (
    RisingWaveConfig,
    RisingWaveSource,
)

_BASE_CONFIG = {
    "username": "user",
    "password": "password",
    "host_port": "host:1521",
}


def test_platform_correctly_set_risingwave():
    source = RisingWaveSource(
        ctx=PipelineContext(run_id="risingwave-source-test"),
        config=RisingWaveConfig.model_validate(_BASE_CONFIG),
    )
    assert source.platform == "risingwave"
