"""Golden-file test: the full MCP stream from a representative deployment.

The other tests assert specific properties. This one pins the *whole* output, so any
unintended change to any aspect shows up as a diff rather than slipping through
because no test happened to look at that field.

Qualytics has no public sandbox, so the deployment is a synthetic fixture replayed
through requests_mock rather than a container, the same shape as upstream's Monte
Carlo test. The payloads are invented, not captured. The fixture is deliberately
broad -- four datastores covering all three store types (jdbc, dfs, native), tables and
views and files, mapped and inferred platforms, passing and failing checks, a mapped
and an unmapped rule type -- because a golden file is only worth as much as the input
behind it.

The anomaly window is pinned in the fixture's recipe rather than left to default: the
30-day default is relative to now, so the golden would otherwise start failing on a
calendar boundary the moment the anomalies mock began honouring date params.

Regenerate with:

    uv run pytest tests/unit/test_golden.py --update-golden-files
"""

import json
from pathlib import Path
from typing import Any

import pytest
from requests_mock import Mocker

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.qualytics.source import QualyticsSource
from datahub.testing import mce_helpers

BASE = "https://acme.qualytics.io/api"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
GOLDEN = Path(__file__).resolve().parent / "golden" / "qualytics_mces_golden.json"


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def _page(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"items": items, "page": 1, "pages": 1, "size": 100, "total": len(items)}


def _register(m: Mocker, deployment: dict[str, Any]) -> None:
    """Serve the recorded deployment over the endpoints the connector calls.

    One dispatching callback per endpoint rather than several matchers on the same
    URL: requests_mock resolves overlapping registrations by reverse registration
    order, which silently shadowed the per-datastore responses on the first attempt
    and produced a three-event golden file.
    """
    m.get(
        f"{BASE}/openapi.json",
        json={"info": {"version": deployment["version"]}, "paths": {}},
    )
    m.get(f"{BASE}/datastores", json=_page(deployment["datastores"]))

    def by_query(table: dict[str, Any], param: str) -> Any:
        def callback(request: Any, context: Any) -> dict[str, Any]:
            values = request.qs.get(param)
            return _page(table.get(values[0], []) if values else [])

        return callback

    m.get(f"{BASE}/containers", json=by_query(deployment["containers"], "datastore"))
    m.get(
        f"{BASE}/quality-checks",
        json=by_query(deployment["quality_checks"], "container"),
    )
    m.get(f"{BASE}/anomalies", json=by_query(deployment["anomalies"], "container"))

    for container_id, payload in deployment["profiles"].items():
        m.get(f"{BASE}/containers/{container_id}/profile", json=payload)
    for container_id, payload in deployment["field_profiles"].items():
        m.get(f"{BASE}/containers/{container_id}/field-profiles", json=_page(payload))


def test_full_ingestion_matches_the_golden_file(
    pytestconfig: pytest.Config, tmp_path: Path
) -> None:
    deployment = _load("golden_deployment.json")

    with Mocker() as m:
        _register(m, deployment)
        source = QualyticsSource.create(
            deployment["recipe"], PipelineContext(run_id="golden")
        )
        # get_workunits(), not get_workunits_internal(): the golden file is what a
        # pipeline would write, framework processors included (status aspects, and
        # the casing processor this source excludes).
        workunits = list(source.get_workunits())

    # write_metadata_file, not a hand-rolled json.dumps: check_golden_file compares
    # against DataHub's normalized on-disk shape, and `to_obj()` produces the raw
    # serialized-aspect form instead. Using their writer keeps both sides in the same
    # representation, and matches how upstream integration tests produce output.
    output = tmp_path / "qualytics_mces.json"
    write_metadata_file(output, [wu.metadata for wu in workunits])

    mce_helpers.check_golden_file(
        pytestconfig, output_path=output, golden_path=GOLDEN, ignore_order=False
    )


def test_the_golden_file_is_substantial_enough_to_be_worth_having() -> None:
    # DataHub's testing standard treats a thin golden file as an incomplete test: a
    # handful of container aspects passes while verifying nothing. Guard the shape of
    # the fixture, not just its bytes.
    golden = json.loads(GOLDEN.read_text())
    aspects = {entry["aspectName"] for entry in golden if "aspectName" in entry}

    assert len(golden) >= 20, f"golden file has only {len(golden)} events"
    assert {"assertionInfo", "assertionRunEvent", "datasetProfile"} <= aspects, (
        f"golden file is missing core aspects; has {sorted(aspects)}"
    )
    # More than one platform proves the resolver is exercised across store types
    # rather than one happy path repeated.
    platforms = {
        entry["entityUrn"].split("dataPlatform:")[1].split(",")[0]
        for entry in golden
        if "dataPlatform:" in entry.get("entityUrn", "")
    }
    assert len(platforms) >= 2, f"golden file only covers {platforms}"

    # All three Qualytics store types must be exercised: each has its own dataset
    # naming rule, and `native` (catalog.schema.table) was uncovered while this
    # module's docstring claimed otherwise.
    deployment = _load("golden_deployment.json")
    store_types = {d["store_type"] for d in deployment["datastores"]}
    assert store_types == {"jdbc", "dfs", "native"}, (
        f"store types covered: {store_types}"
    )
