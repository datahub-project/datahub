from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.set_browse_path import SetBrowsePathTransformer
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    StatusClass,
)

SAMPLE_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)"


def make_generic_dataset(
    entity_urn: str = SAMPLE_URN,
    aspects: Optional[List[Any]] = None,
) -> Iterable[RecordEnvelope]:
    if aspects is None:
        aspects = [StatusClass(removed=False)]
    for aspect in aspects:
        yield RecordEnvelope(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=aspect,
            ),
            metadata={},
        )


def make_browse_paths_aspect(*nodes: str) -> BrowsePathsV2Class:
    path = []
    for node in nodes:
        path.append(
            BrowsePathEntryClass(id=node, urn=node if node.startswith("urn:") else None)
        )
    return BrowsePathsV2Class(path)


def records_to_aspects_map(
    records: Iterable[RecordEnvelope],
) -> Dict[str, Dict[str, List]]:
    aspects_map = defaultdict(lambda: defaultdict(list))
    for record in records:
        if isinstance(record.record, MetadataChangeProposalWrapper):
            aspects_map[record.record.entityUrn][record.record.aspectName].append(
                record.record.aspect
            )
    return aspects_map


@pytest.mark.parametrize(
    "config,input,output",
    [
        pytest.param(
            {"path": ["bcdef", "$container[0]"]},
            ["abc", "urn:li:container:a"],
            ["bcdef", "urn:li:container:a", "abc", "urn:li:container:a"],
            id="simple",
        ),
        pytest.param(
            {"path": ["bcdef", "$containera[0]"]},
            ["abc", "urn:li:container:a"],
            ["bcdef", "abc", "urn:li:container:a"],
            id="non_existent_variable",
        ),
        pytest.param(
            {"path": ["$container[*]", "def"]},
            ["urn:li:container:a", "urn:li:container:b"],
            [
                "urn:li:container:a",
                "urn:li:container:b",
                "def",
                "urn:li:container:a",
                "urn:li:container:b",
            ],
            id="simple_expansion",
        ),
        pytest.param(
            {"path": ["abc"], "replace_existing": True},
            ["abc", "urn:li:container:a", "def", "urn:li:container:b"],
            ["abc"],
            id="simple_overwrite",
        ),
    ],
)
def test_set_browse_paths_against_existing(
    config: Dict, input: List[str], output: List[str]
):
    dataset = make_generic_dataset(
        aspects=[StatusClass(removed=False), make_browse_paths_aspect(*input)]
    )

    transformer = SetBrowsePathTransformer.create(
        config,
        PipelineContext(run_id="test"),
    )
    transformed = list(
        transformer.transform(
            [
                *dataset,
                RecordEnvelope(EndOfStream(), metadata={}),
            ]
        )
    )
    aspects_map = records_to_aspects_map(transformed)
    assert len(aspects_map[SAMPLE_URN]["browsePathsV2"]) == 1
    assert (
        aspects_map[SAMPLE_URN]["browsePathsV2"][0].path
        == make_browse_paths_aspect(*output).path
    )
