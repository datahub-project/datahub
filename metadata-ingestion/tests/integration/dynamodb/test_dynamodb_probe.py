"""`datahub recipe probe` against the DynamoDB emulator the ingestion suite
uses: the listing, and the probe's verdicts against what ingestion emits."""

import pathlib
from typing import Callable, Dict, Iterator, Set

import pytest

from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.metadata.urns import DatasetUrn

# The suite's directory is not a package, so mypy cannot follow the import.
from tests.integration.dynamodb.test_dynamodb import (  # type: ignore[import-untyped]
    EAST,
    WEST,
    _base_config,
    _seed_test_data,
    test_resources_dir,
)
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

# The parity harness masks its reports against the process-global registry,
# as the CLI does; an earlier test's registered secret would otherwise redact
# this fixture's identifiers.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.usefixtures("_isolate_secret_registry"),
]

_INSTANCE = "my_instance"


@pytest.fixture(scope="module")
def dynamodb_emulator(docker_compose_runner: Callable) -> Iterator[None]:
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml",
        "dynamodb",
        setup_command=["up -d --wait"],
    ):
        _seed_test_data()
        yield


def _recipe(region: str, **overrides: object) -> Dict[str, object]:
    return {**_base_config(region), "platform_instance": _INSTANCE, **overrides}


def _emitted_tables(index: EmittedIndex) -> Set[str]:
    # The URN name carries the platform instance as a prefix; table_pattern
    # sees region.table without it.
    prefix = f"{_INSTANCE}."
    return {
        DatasetUrn.from_string(urn).name[len(prefix) :] for urn in index.urns("dataset")
    }


def test_tables_lists_the_recipe_region_only(dynamodb_emulator: None) -> None:
    result = run_probe_method("dynamodb", _recipe(EAST), "tables", {})
    assert isinstance(result.result, list)
    names = sorted(row["name"] for row in result.result)
    assert names == ["us-east-1.Orders", "us-east-1.Products"]


@pytest.mark.parametrize(
    "region, overrides",
    [
        pytest.param(WEST, {}, id="west-allow-all"),
        pytest.param(EAST, {}, id="east-allow-all"),
        pytest.param(
            EAST, {"table_pattern": {"allow": ["us-east-1.Products"]}}, id="east-one"
        ),
    ],
)
def test_verdicts_match_ingestion(
    dynamodb_emulator: None,
    tmp_path: pathlib.Path,
    region: str,
    overrides: Dict[str, object],
) -> None:
    assert_probe_parity(
        "dynamodb",
        _recipe(region, **overrides),
        pipeline_ingestion("dynamodb", tmp_path),
        [ParityListing(label="tables", command="tables", emitted=_emitted_tables)],
    )
