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
    aspects_map: Dict[str, Dict[str, List]] = defaultdict(lambda: defaultdict(list))
    for record in records:
        if (
            isinstance(record.record, MetadataChangeProposalWrapper)
            and record.record.entityUrn
            and record.record.aspectName
        ):
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
        pytest.param(
            {"path": ["$container[*]"], "replace_existing": True},
            ["abc", "urn:li:container:a", "def", "urn:li:container:b"],
            ["urn:li:container:a", "urn:li:container:b"],
            id="expansion_overwrite",
        ),
        pytest.param(
            {"path": ["$dataPlatformInstance[*]"], "replace_existing": True},
            [
                "urn:li:dataPlatformInstance:(urn:li:dataPlatform:glue,my_instance)",
                "urn:li:container:a",
                "urn:li:container:b",
            ],
            ["urn:li:dataPlatformInstance:(urn:li:dataPlatform:glue,my_instance)"],
            id="expansion_instance_overwrite",
        ),
        pytest.param(
            {"path": ["$container[*]"], "replace_existing": True},
            [
                "urn:li:dataPlatformInstance:(urn:li:dataPlatform:glue,my_instance)",
                "urn:li:container:a",
                "urn:li:container:b",
            ],
            ["urn:li:container:a", "urn:li:container:b"],
            id="expansion_containers_overwrite",
        ),
        pytest.param(
            {
                "path": ["static1", "static2", "static3", "$container[*]"],
                "replace_existing": True,
            },
            [
                "urn:li:dataPlatformInstance:(urn:li:dataPlatform:glue,my_instance)",
                "urn:li:container:a",
                "urn:li:container:b",
            ],
            [
                "static1",
                "static2",
                "static3",
                "urn:li:container:a",
                "urn:li:container:b",
            ],
            id="overwrite_platform_instance",
        ),
        pytest.param(
            {
                "path": [
                    "static1",
                    "static2",
                    "$dataPlatformInstance[1]",
                    "static3",
                    "$container[*]",
                ],
                "replace_existing": True,
            },
            ["urn:li:dataPlatformInstance:(urn:li:dataPlatform:glue,my_instance)"],
            ["static1", "static2", "static3"],
            id="expand_non_existent_variables",
        ),
        pytest.param(
            {
                "path": [
                    "static1",
                    "static2",
                    "$dataPlatformInstance[1]",
                    "static3",
                    "$container[*]",
                ],
                "replace_existing": True,
            },
            [],
            ["static1", "static2", "static3"],
            id="empty_initial_path",
        ),
        pytest.param(
            {"path": [], "replace_existing": True},
            [
                "static",
                "urn:li:dataPlatformInstance:(urn:li:dataPlatform:glue,my_instance)",
                "urn:li:container:a",
                "urn:li:container:b",
            ],
            [],
            id="set_empty_path",
        ),
        pytest.param(
            {"path": []},
            [
                "static",
                "urn:li:dataPlatformInstance:(urn:li:dataPlatform:glue,my_instance)",
                "urn:li:container:a",
                "urn:li:container:b",
            ],
            [
                "static",
                "urn:li:dataPlatformInstance:(urn:li:dataPlatform:glue,my_instance)",
                "urn:li:container:a",
                "urn:li:container:b",
            ],
            id="set_empty_path_noop",
        ),
        pytest.param(
            {
                "path": ["$container[*]", "$dataFlow[*]", "my_suffix"],
                "replace_existing": True,
            },
            [
                "urn:li:dataFlow:(airflow,MyFlow,prod)",
            ],
            [
                "urn:li:dataFlow:(airflow,MyFlow,prod)",
                "my_suffix",
            ],
            id="airflow_case",
        ),
    ],
)
def test_set_browse_paths_against_existing(
    config: Dict, input: List[str], output: List[str]
) -> None:
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


@pytest.mark.parametrize(
    "config,output",
    [
        pytest.param(
            {"path": ["bcdef", "$container[0]"]},
            ["bcdef"],
            id="simple",
        ),
        pytest.param(
            {"path": ["$container[1]", "def", "$container[1]"]},
            [
                "def",
            ],
            id="simple2",
        ),
        pytest.param(
            {"path": ["abc"], "replace_existing": True},
            ["abc"],
            id="simple_overwrite",
        ),
        pytest.param(
            {"path": ["$container[*]"], "replace_existing": True},
            [],
            id="expansion_overwrite",
        ),
        pytest.param(
            {"path": ["  \t", "abcd"], "replace_existing": True},
            ["abcd"],
            id="white_spaces_reduction",
        ),
        pytest.param(
            {"path": ["", "abcd"], "replace_existing": True},
            ["abcd"],
            id="empty_node_reduction",
        ),
    ],
)
def test_set_browse_paths_against_non_existing(config: Dict, output: List[str]) -> None:
    dataset = make_generic_dataset(aspects=[StatusClass(removed=False)])

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
