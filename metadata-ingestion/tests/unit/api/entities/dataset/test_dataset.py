import pathlib
from pathlib import Path
from typing import Iterable

from freezegun import freeze_time

from datahub.api.entities.dataset.dataset import Dataset
from datahub.testing.mce_helpers import check_goldens_stream
from tests.test_helpers.graph_helpers import MockDataHubGraph

FROZEN_TIME = "2023-04-14 07:00:00"
RESOURCE_DIR = pathlib.Path(__file__).parent


@freeze_time(FROZEN_TIME)
def test_dataset_from_yaml() -> None:
    example_dataset_file = RESOURCE_DIR / "dataset.yml"

    datasets: Iterable[Dataset] = Dataset.from_yaml(str(example_dataset_file))

    mcps = []

    for dataset in datasets:
        mcps.extend(dataset.generate_mcp())

    check_goldens_stream(
        mcps,
        golden_path=RESOURCE_DIR / "golden_dataset_out_upsert.json",
    )


@freeze_time(FROZEN_TIME)
def test_dataset_from_datahub() -> None:
    mock_graph = MockDataHubGraph()
    golden_file = Path(RESOURCE_DIR / "golden_dataset_out.json")
    mock_graph.import_file(golden_file)

    dataset: Dataset = Dataset.from_datahub(
        mock_graph,
        urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,CustomerAnalytics,PROD)",
    )

    assert (
        dataset.urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,CustomerAnalytics,PROD)"
    )
    assert dataset.domains == ["urn:li:domain:retail"]
    assert (
        dataset.description
        == "Analytics data concerning customer interactions and behaviors."
    )
    assert dataset.name == "CustomerAnalytics"
    assert dataset.properties is not None
    assert dataset.properties == {}
    assert dataset.schema_metadata is not None
    assert dataset.tags == ["sales"]
    assert dataset.glossary_terms == [
        "data-quality.high",
        "data-privacy.sensitive",
        "data-security.confidential",
    ]

    assert dataset.owners is not None
    assert len(dataset.owners) == 1
    assert dataset.owners[0].id == "urn:li:corpuser:jsmith"
    assert dataset.owners[0].type == "urn:li:ownershipType:dataSteward"
