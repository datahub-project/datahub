import json
import pathlib
from typing import Any, Dict, Optional

import jsonschema
import pytest

# Resolved from this file, not the working directory: the parametrize below
# runs at collection time, so a relative path breaks when pytest is invoked
# from the repository root.
_TESTS_ROOT = pathlib.Path(__file__).resolve().parents[2]
_FIXTURE_DIRS = [
    _TESTS_ROOT / "integration" / "dbt",
    _TESTS_ROOT / "unit" / "dbt" / "artifacts",
]
_SCHEMA_DIR = _TESTS_ROOT / "integration" / "dbt" / "schemas"


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

    schema = json.loads(schema_path.read_text())
    # Chosen from the schema's own $schema (dbt's manifests declare 2020-12),
    # so a newly vendored version is validated under the right draft.
    validator = jsonschema.validators.validator_for(schema)(schema)
    errors = [
        f"{'/'.join(str(p) for p in e.absolute_path)}: {e.message}"
        for e in validator.iter_errors(manifest)
    ]
    assert not errors, (
        f"{path.name} is not a valid dbt {version} manifest:\n" + "\n".join(errors[:10])
    )


def test_semantic_model_fixture_exercises_the_emission_paths() -> None:
    """The golden fixture must keep covering the cases the mapper branches on.

    Membership only: asserting exact counts or ids would penalize extending
    the fixture, which is the opposite of what this guards.
    """
    manifest = json.loads(
        (
            _TESTS_ROOT / "integration" / "dbt" / "dbt_manifest_semantic_models.json"
        ).read_text()
    )

    semantic_models = manifest["semantic_models"]
    entity_types = {
        entity["type"]
        for model in semantic_models.values()
        for entity in model["entities"]
    }
    assert {"primary", "foreign", "unique"} <= entity_types
    assert any(model.get("primary_entity") for model in semantic_models.values())

    measures = [m for model in semantic_models.values() for m in model["measures"]]
    assert any(m["create_metric"] for m in measures)
    assert any(not m["create_metric"] for m in measures)
    assert any(m["expr"] for m in measures)

    dimensions = [d for model in semantic_models.values() for d in model["dimensions"]]
    assert any(
        d["type"] == "time" and (d.get("type_params") or {}).get("time_granularity")
        for d in dimensions
    )

    metric_types = {m["type"] for m in manifest["metrics"].values()}
    assert {"simple", "ratio", "derived"} <= metric_types
    # A top-level metric that collides with a create_metric measure.
    measure_names = {m["name"] for m in measures if m["create_metric"]}
    metric_names = {m["name"] for m in manifest["metrics"].values()}
    assert measure_names & metric_names
