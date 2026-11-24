import pathlib
from typing import Iterable, List, Optional, Tuple
from unittest.mock import _Call, patch

import pytest
from deltalake import DeltaTable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.delta_lake.config import DeltaLakeSourceConfig
from datahub.ingestion.source.delta_lake.source import DeltaLakeSource
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
    _Aspect,
)

TABLE_DIR = (
    pathlib.Path(__file__).parents[2]
    / "integration/delta_lake/test_data/delta_tables/my_table_basic"
)
S3_PATH = "s3://my-bucket/my-prefix/my_table_basic"
DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:delta-lake,"
    "my-bucket/my-prefix/my_table_basic,PROD)"
)
TAGS = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:team:data")])


def _flatten(wus: Iterable[MetadataWorkUnit]) -> List[Tuple[str, _Aspect]]:
    out: List[Tuple[str, _Aspect]] = []
    for wu in wus:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper), wu.id
        assert wu.metadata.aspect is not None
        out.append((wu.get_urn(), wu.metadata.aspect))
    return out


def _ingest(
    s3_tags: Optional[GlobalTagsClass],
) -> Tuple[DeltaLakeSource, List[Tuple[str, _Aspect]], List[_Call]]:
    config = DeltaLakeSourceConfig.model_validate(
        {
            "base_path": S3_PATH,
            "s3": {
                "aws_config": {"aws_region": "us-east-1"},
                "use_s3_bucket_tags": True,
            },
        }
    )
    source = DeltaLakeSource(config, PipelineContext(run_id="delta-lake-s3-tags"))
    source.container_WU_creator = ContainerWUCreator(
        config.platform, config.platform_instance, config.env
    )
    with patch(
        "datahub.ingestion.source.delta_lake.source.get_s3_tags",
        return_value=s3_tags,
    ) as get_s3_tags:
        records = _flatten(source.ingest_table(DeltaTable(str(TABLE_DIR)), S3_PATH))
    return source, records, get_s3_tags.call_args_list


@pytest.mark.parametrize("s3_tags", [TAGS, None])
def test_s3_tags_on_dataset(s3_tags: Optional[GlobalTagsClass]) -> None:
    source, records, calls = _ingest(s3_tags)

    assert not source.report.failures
    assert not source.report.warnings

    assert len(calls) == 1
    args = calls[0].args
    assert args[:3] == ("my-bucket", "my-prefix/my_table_basic", DATASET_URN)
    assert args[3] is source.source_config.s3.aws_config  # type: ignore[union-attr]
    assert args[5:] == (True, False)  # use_s3_bucket_tags, use_s3_object_tags

    dataset_aspects = [aspect for urn, aspect in records if urn == DATASET_URN]
    names = [aspect.get_aspect_name() for aspect in dataset_aspects]
    expected = ["status", "datasetProperties", "schemaMetadata"]
    if s3_tags is not None:
        expected.append("globalTags")
    # The dataset's own aspects lead the stream, back to back, before the
    # container hierarchy and the dataset's container aspect.
    assert [(urn, a.get_aspect_name()) for urn, a in records[: len(expected)]] == [
        (DATASET_URN, name) for name in expected
    ]
    assert names.index("container") == len(expected)

    tags = [a for a in dataset_aspects if isinstance(a, GlobalTagsClass)]
    assert tags == ([s3_tags] if s3_tags is not None else [])
