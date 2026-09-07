import json
import pathlib
from typing import Any, Dict, Optional

import jsonschema
import pytest

_FIXTURE_DIRS = [
    pathlib.Path("tests/integration/dbt"),
    pathlib.Path("tests/unit/dbt/artifacts"),
]
_SCHEMA_DIR = pathlib.Path("tests/integration/dbt/schemas")


def _manifest_paths() -> list[pathlib.Path]:
    paths = []
    for directory in _FIXTURE_DIRS:
        paths.extend(sorted(directory.glob("*manifest*.json")))
    assert paths, "no dbt manifest fixtures found"
    return paths


def _schema_version(manifest: Dict[str, Any]) -> Optional[str]:
    """Extract the vNN from e.g. https://schemas.getdbt.com/dbt/manifest/v11.json."""
    url = manifest.get("metadata", {}).get("dbt_schema_version")
    if not isinstance(url, str):
        return None
    return url.rstrip(".json").rsplit("/", 1)[-1]


@pytest.mark.parametrize("path", _manifest_paths(), ids=lambda p: p.name)
def test_manifest_fixture_matches_the_dbt_schema_it_declares(
    path: pathlib.Path,
) -> None:
    """Guards against a fixture that is valid JSON but not a real dbt manifest.

    Only versions we have vendored a schema for are checked; the others are
    skipped rather than silently passing. Schemas are vendored (not fetched)
    so the test never depends on network access, and stored minified to keep
    the repo weight down -- refresh one with:

        curl -s https://schemas.getdbt.com/dbt/manifest/vNN.json \
          | python3 -c 'import json,sys; json.dump(json.load(sys.stdin), sys.stdout, separators=(",",":"))'
    """
    manifest = json.loads(path.read_text())
    version = _schema_version(manifest)
    if version is None:
        pytest.skip(f"{path.name} declares no dbt_schema_version")

    schema_path = _SCHEMA_DIR / f"dbt_manifest_{version}.json"
    if not schema_path.exists():
        pytest.skip(f"no vendored schema for dbt manifest {version}")

    validator = jsonschema.Draft7Validator(json.loads(schema_path.read_text()))
    errors = [
        f"{'/'.join(str(p) for p in e.absolute_path)}: {e.message}"
        for e in validator.iter_errors(manifest)
    ]
    assert not errors, (
        f"{path.name} is not a valid dbt {version} manifest:\n" + "\n".join(errors[:10])
    )


def test_semantic_model_fixture_exercises_the_emission_paths() -> None:
    """The golden fixture must keep covering the cases the mapper branches on."""
    manifest = json.loads(
        (
            pathlib.Path("tests/integration/dbt") / "dbt_manifest_semantic_models.json"
        ).read_text()
    )

    semantic_models = manifest["semantic_models"]
    assert len(semantic_models) == 3
    entity_types = {
        entity["type"]
        for model in semantic_models.values()
        for entity in model["entities"]
    }
    assert {"primary", "foreign", "unique"} <= entity_types

    payments = semantic_models["semantic_model.sample_dbt.payments"]
    assert any(measure["create_metric"] for measure in payments["measures"])
    assert any(not measure["create_metric"] for measure in payments["measures"])
    assert any(
        dimension["type"] == "time"
        and dimension["type_params"]["time_granularity"] == "month"
        for dimension in payments["dimensions"]
    )
    assert semantic_models["semantic_model.sample_dbt.regions"]["primary_entity"]
    assert any(
        measure["expr"]
        for model in semantic_models.values()
        for measure in model["measures"]
    )

    metric_types = {m["type"] for m in manifest["metrics"].values()}
    assert {"simple", "ratio", "derived"} <= metric_types
    # A top-level metric that collides with a create_metric measure.
    assert "metric.sample_dbt.payment_amount" in manifest["metrics"]
