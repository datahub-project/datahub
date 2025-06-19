from typing import List

import pytest

from datahub_integrations.propagation.propagation_utils import (
    filter_downstreams_by_entity_type,
)


@pytest.mark.parametrize(
    "entity_urn, downstreams, expected",
    [
        # Case 1: entity_urn is a schemaField; keep only schemaField downstreams
        (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD),field1)",
            [
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD),field2)",
                "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)",
            ],
            [
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD),field2)"
            ],
        ),
        # Case 2: entity_urn is not a schemaField; keep only non-schemaField downstreams
        (
            "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)",
            [
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD),field2)",
                "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)",
            ],
            ["urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)"],
        ),
        # Case 3: entity_urn is an unknown type; should still apply same logic
        (
            "urn:li:chart:(looker,chart1)",
            [
                "urn:li:chart:(looker,chart2)",
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD),field3)",
            ],
            [
                "urn:li:chart:(looker,chart2)",
            ],
        ),
        # Case 4: No downstreams
        (
            "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)",
            [],
            [],
        ),
        # Case 5: All downstreams are schema fields, but entity_urn is not
        (
            "urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD)",
            [
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD),field1)",
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,myDataset,PROD),field2)",
            ],
            [],
        ),
    ],
)
def test_filter_downstreams_by_entity_type(
    entity_urn: str, downstreams: List[str], expected: List[str]
) -> None:
    result = filter_downstreams_by_entity_type(entity_urn, downstreams)
    assert result == expected
