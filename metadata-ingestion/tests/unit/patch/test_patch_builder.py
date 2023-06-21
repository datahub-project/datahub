import json
import pathlib

import pytest

from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
from datahub.ingestion.sink.file import write_metadata_file
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
    TagAssociationClass,
    UpstreamClass,
)
from datahub.specific.dataset import DatasetPatchBuilder


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
    )
    patcher.for_field("field1").add_tag(TagAssociationClass(tag=make_tag_urn("tag1")))

    out_path = tmp_path / "patch.json"
    write_metadata_file(out_path, patcher.build())

    assert json.loads(out_path.read_text()) == json.loads(
        (
            pytestconfig.rootpath / "tests/unit/patch/complex_dataset_patch.json"
        ).read_text()
    )
