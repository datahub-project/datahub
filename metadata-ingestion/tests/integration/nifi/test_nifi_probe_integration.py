"""`datahub recipe probe` against a real NiFi.

The unit tests drive the probe over a mocked API. This covers what only a
real server shows: the process group endpoints' actual shapes, a nested group
created over the API, a refused sign-in, and the probe's verdicts against what
NiFi ingestion emits.

Runs the standalone flow from setup/conf (groups `Single_Site_S3_to_S3` and
`WIP` under `Standalone Flow`), plus one group the test nests under each.
"""

import json
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Tuple

import pytest
import requests
import yaml
from click.testing import CliRunner

from datahub.cli.recipe_cli import EXIT_CONNECTION, recipe as recipe_cli
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.source.nifi import NifiProcessorType
from tests.test_helpers.docker_helpers import wait_for_port
from tests.test_helpers.probe_parity import (
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

pytestmark = [
    pytest.mark.integration_batch_3,
    pytest.mark.usefixtures("_isolate_secret_registry"),
]

_CONTAINER = "nifi_probe"
_PORT = 9443
_REQUEST_TIMEOUT_SECONDS = 30
_KIND = "Process Group"
_ROOT = "Standalone Flow"
_KEPT = "Single_Site_S3_to_S3"
_WIP = "WIP"
# Created by the fixture: one under a kept group, one under WIP, whose own
# name no recipe below refuses.
_NESTED_KEPT = "Nested_Kept"
_NESTED_IN_WIP = "Nested_In_Wip"


def _is_ready(api: str) -> Callable[[], bool]:
    def check() -> bool:
        try:
            response = requests.get(
                f"{api}flow/process-groups/root", timeout=_REQUEST_TIMEOUT_SECONDS
            )
        except requests.exceptions.RequestException:
            return False
        return response.status_code == 200

    return check


def _post(url: str, body: Dict[str, object]) -> Dict[str, object]:
    response = requests.post(url, json=body, timeout=_REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def _nest(api: str, parent_id: str, name: str) -> None:
    """A group under `parent_id` holding a ListS3, so ingestion emits it."""
    position = {"x": 0.0, "y": 0.0}
    group = _post(
        f"{api}process-groups/{parent_id}/process-groups",
        {"revision": {"version": 0}, "component": {"name": name, "position": position}},
    )
    _post(
        f"{api}process-groups/{group['id']}/processors",
        {
            "revision": {"version": 0},
            "component": {
                "type": NifiProcessorType.ListS3,
                "name": "ListS3",
                "position": position,
            },
        },
    )


def _top_level_ids(api: str) -> Dict[str, str]:
    response = requests.get(
        f"{api}flow/process-groups/root", timeout=_REQUEST_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    groups = response.json()["processGroupFlow"]["flow"]["processGroups"]
    return {g["component"]["name"]: g["id"] for g in groups}


@pytest.fixture(scope="module")
def site_url(docker_compose_runner: Callable) -> Iterator[str]:
    compose = Path(__file__).parent / "docker-compose.probe.yml"
    with docker_compose_runner(compose, "nifi-probe") as docker_services:
        host = f"http://localhost:{docker_services.port_for(_CONTAINER, _PORT)}"
        api = f"{host}/nifi-api/"
        wait_for_port(
            docker_services, _CONTAINER, _PORT, timeout=300, checker=_is_ready(api)
        )
        ids = _top_level_ids(api)
        _nest(api, ids[_KEPT], _NESTED_KEPT)
        _nest(api, ids[_WIP], _NESTED_IN_WIP)
        yield f"{host}/nifi/"


def _listing(site_url: str) -> List[Tuple[str, List[str]]]:
    result = run_probe_method("nifi", {"site_url": site_url}, "process_groups", {})
    assert isinstance(result.result, list)
    return sorted(
        (str(r["name"]), json.loads(str(r["ancestors"]))) for r in result.result
    )


def test_process_groups_list_the_nested_tree(site_url: str) -> None:
    assert _listing(site_url) == [
        (_NESTED_IN_WIP, [_ROOT, _WIP]),
        (_NESTED_KEPT, [_ROOT, _KEPT]),
        (_KEPT, [_ROOT]),
        (_WIP, [_ROOT]),
    ]


def test_flow_reports_the_root_group(site_url: str) -> None:
    result = run_probe_method("nifi", {"site_url": site_url}, "flow", {})
    assert isinstance(result.result, dict)
    assert result.result["name"] == _ROOT


def _parity(
    site_url: str, tmp_path: Path, pattern: Dict[str, object], expect_empty: bool
) -> Dict[str, object]:
    recipe = {
        "site_url": site_url,
        "emit_process_group_as_container": True,
        "provenance_days": 1,
        "process_group_pattern": pattern,
    }
    report = assert_probe_parity(
        "nifi",
        recipe,
        pipeline_ingestion("nifi", tmp_path),
        [
            ParityListing(
                label="process_groups",
                command="process_groups",
                emitted=lambda index: index.container_names(_KIND),
                expect_empty=expect_empty,
            )
        ],
    )
    return dict(report.excluded_by("process_groups"))


def test_probe_verdicts_match_ingestion(site_url: str, tmp_path: Path) -> None:
    excluded = _parity(site_url, tmp_path, {"deny": ["^WIP$"]}, expect_empty=False)
    # The nested group's own name is allowed; ingestion never walks into WIP.
    assert excluded == {
        _WIP: "process_group_pattern",
        _NESTED_IN_WIP: "process_group_pattern",
    }


def test_a_denied_root_leaves_nothing_to_ingest(site_url: str, tmp_path: Path) -> None:
    excluded = _parity(site_url, tmp_path, {"deny": [f"^{_ROOT}$"]}, expect_empty=True)
    assert len(excluded) == 4


def test_a_refused_sign_in_exits_on_the_source_code(
    site_url: str, tmp_path: Path
) -> None:
    # This NiFi is unsecured, so it issues no access tokens.
    recipe_file = tmp_path / "recipe.yml"
    recipe_file.write_text(
        yaml.safe_dump(
            {
                "source": {
                    "type": "nifi",
                    "config": {
                        "site_url": site_url,
                        "auth": "SINGLE_USER",
                        "username": "probe",
                        "password": "test_password",
                    },
                }
            }
        )
    )
    result = CliRunner().invoke(
        recipe_cli, ["probe", "run", "flow", "--recipe", str(recipe_file)]
    )
    assert result.exit_code == EXIT_CONNECTION, result.output
    assert "test_password" not in result.output
