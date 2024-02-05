from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp import MetadataChangeProposalClass
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift import RedshiftSource
from datahub.ingestion.source.redshift.redshift_schema import RedshiftTable


def test_gen_dataset_workunits_patch_custom_properties_patch():
    config = RedshiftConfig(
        host_port="localhost:5439", database="test", patch_custom_properties=True
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


def test_gen_dataset_workunits_patch_custom_properties_upsert():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
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

    custom_props_exist = False
    for item in gen:
        assert isinstance(item.metadata, MetadataChangeProposalWrapper)
        mcp = item.metadata
        if mcp.aspectName == "datasetProperties":
            assert mcp.changeType == "UPSERT"
            custom_props_exist = True

    assert custom_props_exist
