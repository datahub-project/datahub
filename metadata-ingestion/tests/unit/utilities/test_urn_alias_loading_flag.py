import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline_config import PipelineConfig
from datahub.utilities.urn_alias_resolver import (
    set_urn_alias_loading,
    urn_alias_loading_enabled,
)


@pytest.fixture(autouse=True)
def reset_flag():
    # Process-wide state: without a reset it leaks into every later test.
    set_urn_alias_loading(False)
    yield
    set_urn_alias_loading(False)


def _context(auto_resolve: dict) -> PipelineContext:
    config = PipelineConfig.parse_obj(
        {
            "source": {"type": "file", "config": {"filename": "unused.json"}},
            "flags": {"auto_resolve_lineage_urns": auto_resolve},
        }
    )
    return PipelineContext(run_id="test", pipeline_config=config)


def test_loading_is_off_by_default() -> None:
    assert not urn_alias_loading_enabled()


def test_pipeline_enables_loading_when_a_consumer_needs_it() -> None:
    _context(
        {
            "enabled": True,
            "upstream_platforms": [{"platform": "snowflake", "env": "PROD"}],
        }
    )

    assert urn_alias_loading_enabled()


def test_pipeline_leaves_loading_off_when_no_consumer_needs_it() -> None:
    _context({"enabled": False})

    assert not urn_alias_loading_enabled()
