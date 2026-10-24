"""`datahub recipe probe` against the real Dremio test_dremio.py stands up.

The unit tests drive the probe over a fake REST API. This covers what only a
real server shows: the catalog walk ingestion makes, INFORMATION_SCHEMA as
Community edition fills it, and REGEXP_LIKE's matching in ingestion's dataset
query, against what Dremio ingestion actually emits.
"""

import json
import re
from pathlib import Path
from typing import Callable, Dict, FrozenSet, List, Set

import pytest
import yaml
from click.testing import CliRunner, Result

from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.metadata.schema_classes import ContainerPropertiesClass, SubTypesClass
from datahub.metadata.urns import DatasetUrn

# The Dremio stack and its seed are test_dremio.py's module fixtures, imported
# so this module brings the same stack up. The two autouse ones (s3_bkt,
# populate_minio) run because they are in this module's namespace.
from tests.integration.dremio.test_dremio import (  # noqa: F401
    dremio_setup,
    mock_dremio_service,
    populate_minio,
    s3_bkt,
    test_resources_dir,
)
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

# The parity harness masks its reports against the process-global registry, as
# the CLI does; an earlier test's registered secret would otherwise redact this
# fixture's identifiers.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_batch_1,
    pytest.mark.usefixtures("_isolate_secret_registry", "dremio_setup"),
]

_SOURCE_TYPE = "dremio"
_EXIT_USER = 2
_EXIT_SOURCE = 3
_BAD_LOGIN = "probe-bad-login"
# Above the fixture's object counts, so no listing is cut.
_LIMIT = 1000
_HOME_WITHHELD = (
    "1 home space(s) seen and not listed: a home space is named after its "
    "user, and ingestion does not emit these under this recipe"
)


# dremio_probe.py, `folders`: the Samples source lists its unpromoted
# directories, which Dremio will not look up. On every run, and not a gap:
# ingestion's walk skips them alike.
_UNPROMOTED_DIRECTORIES = re.compile(
    r"\d+ catalog container\(s\) answered 404 when looked up and are not "
    r"listed; a file source lists its unpromoted directories this way, and "
    r"ingestion skips them too"
)


def _recipe(**overrides: object) -> Dict[str, object]:
    """dremio_to_file.yml's source config, without profiling: the probe
    judges which objects are emitted, which profiling does not change."""
    with open(Path(__file__).parent / "dremio_to_file.yml") as f:
        config = yaml.safe_load(f)["source"]["config"]
    for profiling_key in ("profiling", "profile_pattern"):
        config.pop(profiling_key, None)
    return {**config, **overrides}


def _containers(sub_type: str) -> Callable[[EmittedIndex], Set[str]]:
    """Emitted containers of one subtype by qualifiedName, the dotted path
    the probe names them by."""

    def emitted(index: EmittedIndex) -> Set[str]:
        names: Set[str] = set()
        for aspects in index.aspects.values():
            if not any(
                isinstance(a, SubTypesClass) and sub_type in a.typeNames
                for a in aspects
            ):
                continue
            for aspect in aspects:
                if isinstance(aspect, ContainerPropertiesClass):
                    names.add(aspect.qualifiedName or aspect.name)
        return names

    return emitted


def _datasets(sub_type: str) -> Callable[[EmittedIndex], Set[str]]:
    def emitted(index: EmittedIndex) -> Set[str]:
        return {
            DatasetUrn.from_string(urn).name
            for urn in index.urns("dataset", with_aspect=SubTypesClass)
            if DatasetUrn.from_string(urn).platform == "urn:li:dataPlatform:dremio"
            and any(
                isinstance(a, SubTypesClass) and sub_type in a.typeNames
                for a in index.aspects[urn]
            )
        }

    return emitted


def _dataset_urn_name(record: JudgedRecord) -> str:
    # Ingestion names a dataset "dremio.<path>", lower-cased.
    return f"dremio.{record.name.lower()}"


def _listings(
    *, empty: FrozenSet[str] = frozenset(), **accept: tuple
) -> List[ParityListing]:
    """One listing per kind; `empty` names the kinds the recipe filters out
    entirely, so ingestion emitting none of them is the answer expected."""
    limit = {"limit": _LIMIT}
    return [
        ParityListing(
            "sources",
            "sources",
            _containers(DatasetContainerSubTypes.DREMIO_SOURCE),
            kwargs=limit,
            expect_empty="sources" in empty,
        ),
        ParityListing(
            "spaces",
            "spaces",
            _containers(DatasetContainerSubTypes.DREMIO_SPACE),
            kwargs=limit,
            expect_empty="spaces" in empty,
            accept_warnings=accept.get("spaces", ()),
        ),
        ParityListing(
            "folders",
            "folders",
            _containers(DatasetContainerSubTypes.DREMIO_FOLDER),
            kwargs=limit,
            expect_empty="folders" in empty,
            accept_warnings=(_UNPROMOTED_DIRECTORIES, *accept.get("folders", ())),
        ),
        ParityListing(
            "tables",
            "tables",
            _datasets(DatasetSubTypes.TABLE),
            kwargs=limit,
            expect_empty="tables" in empty,
            identity=_dataset_urn_name,
        ),
        ParityListing(
            "views",
            "views",
            _datasets(DatasetSubTypes.VIEW),
            kwargs=limit,
            expect_empty="views" in empty,
            identity=_dataset_urn_name,
        ),
    ]


def _probe_cli(tmp_path: Path, recipe: Dict[str, object], *args: str) -> Result:
    recipe_file = tmp_path / "recipe.yml"
    recipe_file.write_text(
        yaml.safe_dump({"source": {"type": _SOURCE_TYPE, "config": recipe}})
    )
    return CliRunner().invoke(
        recipe_cli,
        ["probe", "run", args[0], "--recipe", str(recipe_file), *args[1:]],
    )


def _names(command: str, **kwargs: object) -> List[str]:
    result = run_probe_method(_SOURCE_TYPE, _recipe(), command, dict(kwargs))
    assert isinstance(result.result, list)
    return [record["name"] for record in result.result]


def test_listings_return_the_seeded_objects() -> None:
    assert {"Samples", "s3", "mysql"} <= set(_names("sources"))
    assert {"space", "@admin"} <= set(_names("spaces"))
    assert _names("folders", container="space") == ["space.test_folder"]
    assert "space.test_folder.raw" in _names("views", limit=_LIMIT)
    assert "s3.warehouse" in _names("tables", limit=_LIMIT)


def test_default_recipe_matches_ingestion(tmp_path: Path) -> None:
    report = assert_probe_parity(
        _SOURCE_TYPE,
        _recipe(),
        pipeline_ingestion(_SOURCE_TYPE, tmp_path),
        _listings(),
    )
    assert report.kinds["views"].included >= {"dremio.space.test_folder.raw"}
    # The MySQL source's tables no view reads have no column metadata in
    # Dremio, so ingestion's dataset query never sees them.
    assert (
        report.excluded_by("tables")["dremio.mysql.datacharmer.employees"]
        == "no_column_metadata"
    )


def test_a_root_allow_list_matches_ingestion(tmp_path: Path) -> None:
    """dremio_schema_filter_to_file.yml's pattern: containers match it from
    the start of the path, datasets anywhere in it."""
    report = assert_probe_parity(
        _SOURCE_TYPE,
        _recipe(schema_pattern={"allow": ["Samples"]}),
        pipeline_ingestion(_SOURCE_TYPE, tmp_path),
        _listings(
            empty=frozenset({"spaces", "views"}),
            spaces=(_HOME_WITHHELD,),
            folders=(_HOME_WITHHELD,),
        ),
    )
    assert report.excluded_by("sources") == {
        "s3": "schema_pattern",
        "mysql": "schema_pattern",
    }
    assert set(report.excluded_by("views").values()) == {"schema_pattern"}


def test_a_schema_deny_reaches_datasets_by_search_and_folders_by_prefix(
    tmp_path: Path,
) -> None:
    report = assert_probe_parity(
        _SOURCE_TYPE,
        _recipe(
            schema_pattern={"deny": ["test_folder"]},
            dataset_pattern={"deny": [".*\\.csv$"]},
        ),
        pipeline_ingestion(_SOURCE_TYPE, tmp_path),
        _listings(),
    )
    # "space.test_folder" does not start with the deny entry, so ingestion
    # keeps the folder; its dataset query finds the entry inside the path and
    # drops the views in it.
    assert "space.test_folder" in report.kinds["folders"].included
    assert report.excluded_by("views")["dremio.space.test_folder.raw"] == (
        "schema_pattern"
    )
    assert (
        report.excluded_by("tables")[
            "dremio.samples.samples.dremio.com.nyc-weather.csv"
        ]
        == "dataset_pattern"
    )


def test_a_wrong_password_is_the_sources(tmp_path: Path) -> None:
    result = _probe_cli(tmp_path, _recipe(password=_BAD_LOGIN), "sources")
    assert result.exit_code == _EXIT_SOURCE, result.output
    assert _BAD_LOGIN not in result.output


def test_an_unknown_container_is_the_callers(tmp_path: Path) -> None:
    result = _probe_cli(tmp_path, _recipe(), "folders", "--container", "SPACE")
    assert result.exit_code == _EXIT_USER, result.output
    assert "'space'" in json.loads(result.stderr)["error"]
