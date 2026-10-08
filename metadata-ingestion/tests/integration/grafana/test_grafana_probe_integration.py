"""`datahub recipe probe` against a real Grafana.

The unit tests drive the probe over a mocked API. This covers what only a
real server shows: the search and folder endpoints' actual shapes and paging,
basic mode's untyped search returning folders, a least-privilege (Viewer)
service account token, and the probe's verdicts against what Grafana
ingestion emits.
"""

import re
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Set

import pytest
import requests
import yaml
from click.testing import CliRunner

from datahub.cli.recipe_cli import EXIT_CONNECTION, recipe as recipe_cli
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.metadata.urns import DashboardUrn
from tests.test_helpers.docker_helpers import wait_for_port
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

pytestmark = [
    pytest.mark.integration_batch_5,
    pytest.mark.usefixtures("_isolate_secret_registry"),
]

_CONTAINER = "grafana_probe"
_PORT = 3000
# Grafana's default first-run admin; docker-compose.probe.yml leaves it as is.
_ADMIN = ("admin", "admin")
_REQUEST_TIMEOUT_SECONDS = 10
_FOLDER = "Folder"

# Folder title -> dashboard titles in it; None is the General (root) level.
_CONTENT: Dict[object, List[str]] = {
    "Operations": ["Ops Overview", "Ops Latency"],
    "Sandbox": ["Sandbox Metrics"],
    None: ["Home Board"],
}


def _admin_session() -> requests.Session:
    session = requests.Session()
    session.auth = _ADMIN
    return session


def _is_ready(base_url: str) -> Callable[[], bool]:
    def check() -> bool:
        try:
            response = _admin_session().get(
                f"{base_url}/api/folders", timeout=_REQUEST_TIMEOUT_SECONDS
            )
        except requests.exceptions.RequestException:
            return False
        return response.status_code == 200

    return check


def _seed(base_url: str) -> str:
    """Create the folders and dashboards, and return a Viewer service
    account's token: the least privilege that reads both."""
    session = _admin_session()

    def post(path: str, body: Dict[str, object]) -> Dict[str, object]:
        response = session.post(
            f"{base_url}{path}", json=body, timeout=_REQUEST_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        return response.json()

    for folder, titles in _CONTENT.items():
        folder_uid = None
        if folder is not None:
            folder_uid = post("/api/folders", {"title": folder})["uid"]
        for title in titles:
            post(
                "/api/dashboards/db",
                {
                    "dashboard": {"title": title, "panels": []},
                    **({"folderUid": folder_uid} if folder_uid else {}),
                },
            )
    account = post("/api/serviceaccounts", {"name": "probe", "role": "Viewer"})
    token = post(f"/api/serviceaccounts/{account['id']}/tokens", {"name": "probe"})
    return str(token["key"])


@pytest.fixture(scope="module")
def grafana(docker_compose_runner: Callable) -> Iterator[Dict[str, object]]:
    compose = Path(__file__).parent / "docker-compose.probe.yml"
    with docker_compose_runner(compose, "grafana-probe") as docker_services:
        base_url = f"http://localhost:{docker_services.port_for(_CONTAINER, _PORT)}"
        wait_for_port(
            docker_services,
            _CONTAINER,
            _PORT,
            timeout=180,
            checker=_is_ready(base_url),
        )
        yield {"url": base_url, "service_account_token": _seed(base_url)}


def _emitted_dashboards(index: EmittedIndex) -> Set[str]:
    return {DashboardUrn.from_string(u).dashboard_id for u in index.urns("dashboard")}


def _by_uid(record: JudgedRecord) -> str:
    return record.attributes["uid"]


def _titles(recipe: Dict[str, object], command: str) -> List[str]:
    result = run_probe_method("grafana", recipe, command, {})
    assert isinstance(result.result, list)
    return sorted(str(r["name"]) for r in result.result)


def test_listings_return_the_seeded_content(grafana: Dict[str, object]) -> None:
    # Grafana 11+ answers /api/folders with a virtual "Shared with me" folder
    # as well; ingestion reads the same listing and emits it too.
    assert {"Operations", "Sandbox"} <= set(_titles(grafana, "folders"))
    assert _titles(grafana, "dashboards") == [
        "Home Board",
        "Ops Latency",
        "Ops Overview",
        "Sandbox Metrics",
    ]


def test_dashboards_page_through_a_small_page_size(
    grafana: Dict[str, object],
) -> None:
    assert len(_titles({**grafana, "page_size": 1}, "dashboards")) == 4


def test_probe_verdicts_match_enhanced_ingestion(
    grafana: Dict[str, object], tmp_path: Path
) -> None:
    recipe = {
        **grafana,
        "include_lineage": False,
        "folder_pattern": {"deny": ["^Sandbox$"]},
        "dashboard_pattern": {"deny": [".*Latency$"]},
    }
    report = assert_probe_parity(
        "grafana",
        recipe,
        pipeline_ingestion("grafana", tmp_path),
        [
            ParityListing(
                label="folders",
                command="folders",
                emitted=lambda index: index.container_names(_FOLDER),
            ),
            ParityListing(
                label="dashboards",
                command="dashboards",
                emitted=_emitted_dashboards,
                identity=_by_uid,
            ),
        ],
    )
    assert report.excluded_by("folders") == {"Sandbox": "folder_pattern"}
    # Only the denied title; the dashboard in the denied folder is kept.
    assert list(report.excluded_by("dashboards").values()) == ["dashboard_pattern"]
    assert len(report.kinds["dashboards"].included) == 3


def test_probe_verdicts_match_basic_mode_ingestion(
    grafana: Dict[str, object], tmp_path: Path
) -> None:
    recipe = {**grafana, "basic_mode": True}
    report = assert_probe_parity(
        "grafana",
        recipe,
        pipeline_ingestion("grafana", tmp_path),
        [
            ParityListing(
                label="folders",
                command="folders",
                emitted=lambda index: index.container_names(_FOLDER),
                expect_empty=True,
                accept_warnings=(re.compile("basic_mode is set.*"),),
            ),
            ParityListing(
                label="dashboards",
                command="dashboards",
                emitted=_emitted_dashboards,
                identity=_by_uid,
            ),
        ],
    )
    assert set(report.excluded_by("folders").values()) == {"basic_mode"}
    # Grafana's untyped search returns the two folders too, which basic mode
    # ingestion emits as dashboards; the probe lists what it emits.
    assert len(report.kinds["dashboards"].included) == 6


def test_a_bad_token_exits_on_the_source_code(
    grafana: Dict[str, object], tmp_path: Path
) -> None:
    recipe_file = tmp_path / "recipe.yml"
    recipe_file.write_text(
        yaml.safe_dump(
            {
                "source": {
                    "type": "grafana",
                    "config": {**grafana, "service_account_token": "glsa_wrong"},
                }
            }
        )
    )
    result = CliRunner().invoke(
        recipe_cli, ["probe", "run", "folders", "--recipe", str(recipe_file)]
    )
    assert result.exit_code == EXIT_CONNECTION, result.output
    assert "HTTP 401" in result.output
