import pathlib

import pytest

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_dataset_urn,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.ingestion.sink.file import write_metadata_file
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
    TagAssociationClass,
    UpstreamClass,
)
from datahub.specific.chart import ChartPatchBuilder
from datahub.specific.dashboard import DashboardPatchBuilder
from datahub.specific.dataset import DatasetPatchBuilder
from tests.test_helpers import mce_helpers


def test_basic_dataset_patch_builder():
    patcher = DatasetPatchBuilder(
        make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")
    ).add_tag(TagAssociationClass(tag=make_tag_urn("test_tag")))

    assert patcher.build() == [
        MetadataChangeProposalClass(
            entityType="dataset",
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
            changeType="PATCH",
            aspectName="globalTags",
            aspect=GenericAspectClass(
                value=b'[{"op": "add", "path": "/tags/urn:li:tag:test_tag", "value": {"tag": "urn:li:tag:test_tag"}}]',
                contentType="application/json-patch+json",
            ),
        ),
    ]


def test_complex_dataset_patch(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    patcher = (
        DatasetPatchBuilder(
            make_dataset_urn(platform="hive", name="fct_users_created", env="PROD")
        )
        .set_description("test description")
        .add_custom_property("test_key_1", "test_value_1")
        .add_custom_property("test_key_2", "test_value_2")
        .add_tag(TagAssociationClass(tag=make_tag_urn("test_tag")))
        .add_upstream_lineage(
            upstream=UpstreamClass(
                dataset=make_dataset_urn(
                    platform="hive", name="fct_users_created_upstream", env="PROD"
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
        )
        .add_upstream_lineage(
            upstream=UpstreamClass(
                dataset=make_dataset_urn(
                    platform="s3", name="my-bucket/my-folder/my-file.txt", env="PROD"
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
        )
        .add_fine_grained_upstream_lineage(
            fine_grained_lineage=FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreams=[
                    make_schema_field_urn(
                        make_dataset_urn(
                            platform="hive",
                            name="fct_users_created",
                            env="PROD",
                        ),
                        field_path="foo",
                    )
                ],
                upstreams=[
                    make_schema_field_urn(
                        make_dataset_urn(
                            platform="hive",
                            name="fct_users_created_upstream",
                            env="PROD",
                        ),
                        field_path="bar",
                    )
                ],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                transformOperation="TRANSFORM",
                confidenceScore=1.0,
            )
        )
        .add_fine_grained_upstream_lineage(
            fine_grained_lineage=FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.DATASET,
                upstreams=[
                    make_schema_field_urn(
                        make_dataset_urn(
                            platform="s3",
                            name="my-bucket/my-folder/my-file.txt",
                            env="PROD",
                        ),
                        field_path="foo",
                    )
                ],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                downstreams=[
                    make_schema_field_urn(
                        make_dataset_urn(
                            platform="hive",
                            name="fct_users_created",
                            env="PROD",
                        ),
                        field_path="foo",
                    )
                ],
            )
        )
    )
    patcher.for_field("field1").add_tag(TagAssociationClass(tag=make_tag_urn("tag1")))

    out_path = tmp_path / "patch.json"
    write_metadata_file(out_path, patcher.build())

    mce_helpers.check_golden_file(
        pytestconfig,
        out_path,
        pytestconfig.rootpath / "tests/unit/patch/complex_dataset_patch.json",
    )


def test_basic_chart_patch_builder():
    patcher = ChartPatchBuilder(
        make_chart_urn(platform="hive", name="fct_users_created")
    ).add_tag(TagAssociationClass(tag=make_tag_urn("test_tag")))

    assert patcher.build() == [
        MetadataChangeProposalClass(
            entityType="chart",
            entityUrn="urn:li:chart:(hive,fct_users_created)",
            changeType="PATCH",
            aspectName="globalTags",
            aspect=GenericAspectClass(
                value=b'[{"op": "add", "path": "/tags/urn:li:tag:test_tag", "value": {"tag": "urn:li:tag:test_tag"}}]',
                contentType="application/json-patch+json",
            ),
        ),
    ]


def test_basic_dashboard_patch_builder():
    patcher = DashboardPatchBuilder(
        make_dashboard_urn(platform="hive", name="fct_users_created")
    ).add_tag(TagAssociationClass(tag=make_tag_urn("test_tag")))

    assert patcher.build() == [
        MetadataChangeProposalClass(
            entityType="dashboard",
            entityUrn="urn:li:dashboard:(hive,fct_users_created)",
            changeType="PATCH",
            aspectName="globalTags",
            aspect=GenericAspectClass(
                value=b'[{"op": "add", "path": "/tags/urn:li:tag:test_tag", "value": {"tag": "urn:li:tag:test_tag"}}]',
                contentType="application/json-patch+json",
            ),
        ),
    ]
