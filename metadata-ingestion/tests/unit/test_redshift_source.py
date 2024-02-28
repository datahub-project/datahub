from typing import Iterable

import pytest

from datahub.emitter.mcp import (
    MetadataChangeProposalClass,
    MetadataChangeProposalWrapper,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift import RedshiftSource
from datahub.ingestion.source.redshift.redshift_schema import RedshiftTable


@pytest.fixture
def redshift_source_setup(request: pytest.FixtureRequest) -> Iterable[MetadataWorkUnit]:
    custom_props_flag = request.param
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="test",
        patch_custom_properties=custom_props_flag,
    )
    source: RedshiftSource = RedshiftSource(config, ctx=PipelineContext(run_id="test"))
    gen = source.gen_dataset_workunits(
        table=RedshiftTable(
            name="category",
            columns=[],
            created=None,
            comment="",
        ),
        database="dev",
        schema="public",
        sub_type="test_sub_type",
        custom_properties={"my_key": "my_value"},
    )
    return gen


@pytest.mark.parametrize("redshift_source_setup", [True], indirect=True)
def test_gen_dataset_workunits_patch_custom_properties_patch(redshift_source_setup):
    gen = redshift_source_setup
    custom_props_exist = False
    for item in gen:
        mcp = item.metadata
        if mcp.aspectName == "datasetProperties":
            assert isinstance(item.metadata, MetadataChangeProposalClass)
            assert mcp.changeType == "PATCH"
            custom_props_exist = True
        else:
            assert isinstance(item.metadata, MetadataChangeProposalWrapper)

    assert custom_props_exist


@pytest.mark.parametrize("redshift_source_setup", [False], indirect=True)
def test_gen_dataset_workunits_patch_custom_properties_upsert(redshift_source_setup):
    gen = redshift_source_setup
    custom_props_exist = False
    for item in gen:
        assert isinstance(item.metadata, MetadataChangeProposalWrapper)
        mcp = item.metadata
        if mcp.aspectName == "datasetProperties":
            assert mcp.changeType == "UPSERT"
            custom_props_exist = True

    assert custom_props_exist
