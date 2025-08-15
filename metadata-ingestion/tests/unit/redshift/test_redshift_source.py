from typing import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift import RedshiftSource
from datahub.ingestion.source.redshift.redshift_schema import RedshiftTable
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
)


def redshift_source_setup(custom_props_flag: bool) -> Iterable[MetadataWorkUnit]:
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


def test_gen_dataset_workunits_patch_custom_properties_patch():
    gen = redshift_source_setup(True)
    custom_props_exist = False
    for item in gen:
        mcp = item.metadata
        assert not isinstance(mcp, MetadataChangeEventClass)
        if mcp.aspectName == "datasetProperties":
            assert isinstance(mcp, MetadataChangeProposalClass)
            assert mcp.changeType == "PATCH"
            custom_props_exist = True
        else:
            assert isinstance(mcp, MetadataChangeProposalWrapper)

    assert custom_props_exist


def test_gen_dataset_workunits_patch_custom_properties_upsert():
    gen = redshift_source_setup(False)
    custom_props_exist = False
    for item in gen:
        assert isinstance(item.metadata, MetadataChangeProposalWrapper)
        mcp = item.metadata
        if mcp.aspectName == "datasetProperties":
            assert mcp.changeType == "UPSERT"
            custom_props_exist = True

    assert custom_props_exist
