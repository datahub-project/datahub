import json
import pathlib
from typing import Any, Dict, Union

import pytest
from freezegun.api import freeze_time

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.ingestion.sink.file import write_metadata_file
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetLineageTypeClass,
    EdgeClass,
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
from datahub.specific.datajob import DataJobPatchBuilder
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


@pytest.mark.parametrize(
    "created_on,last_modified,expected_actor",
    [
        (1586847600000, 1586847600000, "urn:li:corpuser:datahub"),
        (None, None, "urn:li:corpuser:datahub"),
        (1586847600000, None, "urn:li:corpuser:datahub"),
        (None, 1586847600000, "urn:li:corpuser:datahub"),
    ],
    ids=["both_timestamps", "no_timestamps", "only_created", "only_modified"],
)
@freeze_time("2020-04-14 07:00:00")
def test_datajob_patch_builder(created_on, last_modified, expected_actor):
    def make_edge_or_urn(urn: str) -> Union[EdgeClass, str]:
        if created_on or last_modified:
            return EdgeClass(
                destinationUrn=str(urn),
                created=(
                    AuditStampClass(
                        time=created_on,
                        actor=expected_actor,
                    )
                    if created_on
                    else None
                ),
                lastModified=(
                    AuditStampClass(
                        time=last_modified,
                        actor=expected_actor,
                    )
                    if last_modified
                    else None
                ),
            )
        return urn

    def get_edge_expectation(urn: str) -> Dict[str, Any]:
        if created_on or last_modified:
            expected = {
                "destinationUrn": str(urn),
                "created": (
                    AuditStampClass(
                        time=created_on,
                        actor=expected_actor,
                    ).to_obj()
                    if created_on
                    else None
                ),
                "lastModified": (
                    AuditStampClass(
                        time=last_modified,
                        actor=expected_actor,
                    ).to_obj()
                    if last_modified
                    else None
                ),
            }
            # filter out None values
            return {k: v for k, v in expected.items() if v is not None}
        return {"destinationUrn": str(urn)}

    flow_urn = make_data_flow_urn(
        orchestrator="nifi", flow_id="252C34e5af19-0192-1000-b248-b1abee565b5d"
    )
    job_urn = make_data_job_urn_with_flow(
        flow_urn, "5ca6fee7-0192-1000-f206-dfbc2b0d8bfb"
    )
    patcher = DataJobPatchBuilder(job_urn)

    patcher.add_output_dataset(
        make_edge_or_urn(
            "urn:li:dataset:(urn:li:dataPlatform:s3,output-bucket/folder1,DEV)"
        )
    )
    patcher.add_output_dataset(
        make_edge_or_urn(
            "urn:li:dataset:(urn:li:dataPlatform:s3,output-bucket/folder3,DEV)"
        )
    )
    patcher.add_output_dataset(
        make_edge_or_urn(
            "urn:li:dataset:(urn:li:dataPlatform:s3,output-bucket/folder2,DEV)"
        )
    )

    assert patcher.build() == [
        MetadataChangeProposalClass(
            entityType="dataJob",
            entityUrn="urn:li:dataJob:(urn:li:dataFlow:(nifi,252C34e5af19-0192-1000-b248-b1abee565b5d,prod),5ca6fee7-0192-1000-f206-dfbc2b0d8bfb)",
            changeType="PATCH",
            aspectName="dataJobInputOutput",
            aspect=GenericAspectClass(
                value=json.dumps(
                    [
                        {
                            "op": "add",
                            "path": "/outputDatasetEdges/urn:li:dataset:(urn:li:dataPlatform:s3,output-bucket~1folder1,DEV)",
                            "value": get_edge_expectation(
                                "urn:li:dataset:(urn:li:dataPlatform:s3,output-bucket/folder1,DEV)"
                            ),
                        },
                        {
                            "op": "add",
                            "path": "/outputDatasetEdges/urn:li:dataset:(urn:li:dataPlatform:s3,output-bucket~1folder3,DEV)",
                            "value": get_edge_expectation(
                                "urn:li:dataset:(urn:li:dataPlatform:s3,output-bucket/folder3,DEV)"
                            ),
                        },
                        {
                            "op": "add",
                            "path": "/outputDatasetEdges/urn:li:dataset:(urn:li:dataPlatform:s3,output-bucket~1folder2,DEV)",
                            "value": get_edge_expectation(
                                "urn:li:dataset:(urn:li:dataPlatform:s3,output-bucket/folder2,DEV)"
                            ),
                        },
                    ]
                ).encode("utf-8"),
                contentType="application/json-patch+json",
            ),
        )
    ]
