from typing import cast

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift import RedshiftSource
from datahub.ingestion.source.redshift.redshift_schema import RedshiftTable
from datahub.metadata.schema_classes import MetadataChangeProposalClass


def test_gen_dataset_workunits_patch_custom_properties():
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
        mcp = cast(MetadataChangeProposalClass, item.metadata)
        if mcp.aspectName == "datasetProperties":
            assert mcp.changeType == "PATCH"
            custom_props_exist = True

    assert custom_props_exist
