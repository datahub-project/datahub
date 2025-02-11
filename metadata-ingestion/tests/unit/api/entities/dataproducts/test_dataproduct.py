import difflib
from pathlib import Path
from typing import Any, Dict

import pytest
from freezegun import freeze_time

from datahub.api.entities.dataproduct.dataproduct import DataProduct
from datahub.metadata.schema_classes import DomainPropertiesClass
from tests.test_helpers.graph_helpers import MockDataHubGraph
from tests.test_helpers.mce_helpers import check_golden_file

FROZEN_TIME = "2023-04-14 07:00:00"


@pytest.fixture
def base_entity_metadata():
    return {
        "urn:li:domain:12345": {
            "domainProperties": DomainPropertiesClass(
                name="Marketing", description="Marketing Domain"
            )
        }
    }


@pytest.fixture
def base_mock_graph(
    base_entity_metadata: Dict[str, Dict[str, Any]],
) -> MockDataHubGraph:
    return MockDataHubGraph(entity_graph=base_entity_metadata)


@pytest.fixture
def test_resources_dir(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath / "tests/unit/api/entities/dataproducts"


def check_yaml_golden_file(input_file: str, golden_file: str) -> bool:
    with open(input_file) as input:
        input_lines = input.readlines()

    with open(golden_file) as golden:
        golden_lines = golden.readlines()

    diff_exists = False
    for line in difflib.unified_diff(
        input_lines, golden_lines, fromfile=input_file, tofile=golden_file, lineterm=""
    ):
        print(line)
        diff_exists = True

    return diff_exists


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "data_product_filename, upsert,golden_filename",
    [
        ("dataproduct.yaml", False, "golden_dataproduct_out.json"),
        ("dataproduct_upsert.yaml", True, "golden_dataproduct_out_upsert.json"),
    ],
    ids=["update", "upsert"],
)
def test_dataproduct_from_yaml(
    pytestconfig: pytest.Config,
    test_resources_dir: Path,
    tmp_path: Path,
    base_mock_graph: MockDataHubGraph,
    data_product_filename: str,
    upsert: bool,
    golden_filename: str,
) -> None:
    data_product_file = test_resources_dir / data_product_filename
    mock_graph = base_mock_graph
    data_product = DataProduct.from_yaml(data_product_file, mock_graph)
    assert data_product._resolved_domain_urn == "urn:li:domain:12345"
    assert data_product.assets is not None
    assert len(data_product.assets) == 3

    for mcp in data_product.generate_mcp(upsert=upsert):
        mock_graph.emit(mcp)

    output_file = Path(tmp_path / "test_dataproduct_out.json")
    mock_graph.sink_to_file(output_file)
    golden_file = Path(test_resources_dir / golden_filename)
    check_golden_file(pytestconfig, output_file, golden_file)


@freeze_time(FROZEN_TIME)
def test_dataproduct_from_datahub(
    pytestconfig: pytest.Config,
    test_resources_dir: Path,
    tmp_path: Path,
    base_mock_graph: MockDataHubGraph,
) -> None:
    mock_graph = base_mock_graph
    golden_file = Path(test_resources_dir / "golden_dataproduct_out.json")
    mock_graph.import_file(golden_file)

    data_product: DataProduct = DataProduct.from_datahub(
        mock_graph, id="urn:li:dataProduct:pet_of_the_week"
    )
    assert data_product.domain == "urn:li:domain:12345"
    assert data_product.assets is not None
    assert len(data_product.assets) == 3

    # validate that output looks exactly the same

    for mcp in data_product.generate_mcp(upsert=False):
        mock_graph.emit(mcp)

    output_file = Path(tmp_path / "test_dataproduct_to_datahub_out.json")
    mock_graph.sink_to_file(output_file)
    golden_file = Path(test_resources_dir / "golden_dataproduct_out.json")
    check_golden_file(pytestconfig, output_file, golden_file)


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "original_file",
    [
        ("dataproduct_v2_asset_add.yaml"),
        ("dataproduct_v2_asset_remove.yaml"),
    ],
    ids=["asset_add", "asset_remove"],
)
def test_dataproduct_patch_yaml(
    pytestconfig: pytest.Config,
    tmp_path: Path,
    test_resources_dir: Path,
    base_mock_graph: MockDataHubGraph,
    original_file: str,
) -> None:
    """Validates that patching data product files by reading from DataHub works as expected"""
    mock_graph = base_mock_graph
    golden_file = Path(test_resources_dir / "golden_dataproduct_out.json")
    mock_graph.import_file(golden_file)

    data_product_file = test_resources_dir / original_file
    original_data_product: DataProduct = DataProduct.from_yaml(
        data_product_file, mock_graph
    )
    data_product: DataProduct = DataProduct.from_datahub(
        mock_graph, id="urn:li:dataProduct:pet_of_the_week"
    )
    dataproduct_output_file = Path(tmp_path / f"patch_{original_file}")
    data_product.patch_yaml(original_data_product, dataproduct_output_file)
    dataproduct_golden_file = Path(test_resources_dir / "golden_dataproduct_v2.yaml")
    assert (
        check_yaml_golden_file(
            str(dataproduct_output_file), str(dataproduct_golden_file)
        )
        is False
    )


@freeze_time(FROZEN_TIME)
def test_dataproduct_ownership_type_urn_from_yaml(
    pytestconfig: pytest.Config,
    tmp_path: Path,
    test_resources_dir: Path,
    base_mock_graph: MockDataHubGraph,
) -> None:
    data_product_file = test_resources_dir / "dataproduct_ownership_type_urn.yaml"
    mock_graph = base_mock_graph
    data_product = DataProduct.from_yaml(data_product_file, mock_graph)

    for mcp in data_product.generate_mcp(upsert=False):
        mock_graph.emit(mcp)

    output_file = tmp_path / "test_dataproduct_out.json"
    mock_graph.sink_to_file(output_file)
    golden_file = test_resources_dir / "golden_dataproduct_out_ownership_type_urn.json"
    check_golden_file(pytestconfig, output_file, golden_file)


@freeze_time(FROZEN_TIME)
def test_dataproduct_ownership_type_urn_patch_yaml(
    tmp_path: Path, test_resources_dir: Path, base_mock_graph: MockDataHubGraph
) -> None:
    mock_graph = base_mock_graph
    source_file = test_resources_dir / "golden_dataproduct_out_ownership_type_urn.json"
    mock_graph.import_file(source_file)

    data_product_file = (
        test_resources_dir / "dataproduct_ownership_type_urn_different_owner.yaml"
    )
    original_data_product: DataProduct = DataProduct.from_yaml(
        data_product_file, mock_graph
    )

    data_product: DataProduct = DataProduct.from_datahub(
        mock_graph, id="urn:li:dataProduct:pet_of_the_week"
    )

    dataproduct_output_file = (
        tmp_path / "patch_dataproduct_ownership_type_urn_different_owner.yaml"
    )
    data_product.patch_yaml(original_data_product, dataproduct_output_file)

    assert not check_yaml_golden_file(
        str(dataproduct_output_file),
        str(test_resources_dir / "dataproduct_ownership_type_urn.yaml"),
    )
