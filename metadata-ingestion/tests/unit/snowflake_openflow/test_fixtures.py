import pathlib
from typing import Any

import pytest
import yaml

from datahub.ingestion.run.pipeline_config import PipelineConfig

# Anchored to this file, not to the working directory. A relative path works only
# when pytest is invoked from `metadata-ingestion/`, so it breaks from the repo
# root and in IDE runners. `Path(__file__).parent` is the repo's convention --
# see tests/unit/glue/test_glue_source.py.
FIXTURE_DIR = (
    pathlib.Path(__file__).parent.parent.parent
    / "integration"
    / "snowflake_openflow"
    / "fixtures"
)
FIXTURES = ["inventory.yml", "lineage.yml", "capabilities.yml"]


@pytest.mark.parametrize("filename", FIXTURES)
def test_fixture_recipe_parses(filename):
    recipe = yaml.safe_load((FIXTURE_DIR / filename).read_text())
    assert recipe["source"]["type"] == "snowflake-openflow"


@pytest.mark.parametrize("filename", FIXTURES)
def test_fixture_recipe_writes_to_datahub(filename):
    # Recipes must emit to datahub-rest (GMS) so verification assertions
    # can check entities actually land in DataHub. File sinks produce
    # JSON that the verifier cannot assert against.
    recipe = yaml.safe_load((FIXTURE_DIR / filename).read_text())
    assert recipe["sink"]["type"] == "datahub-rest"


@pytest.mark.parametrize("filename", FIXTURES)
def test_fixture_recipe_validates_as_pipeline(filename):
    # Validate the full recipe by loading through PipelineConfig.model_validate.
    # This is the most comprehensive validation: it exercises source config,
    # sink config, and pipeline-level interactions (e.g., pipeline_name required
    # when stateful_ingestion is enabled). Catches all errors that would appear
    # at runtime: nested fields, types, business logic, and inter-field dependencies.
    recipe = yaml.safe_load((FIXTURE_DIR / filename).read_text())
    recipe_dict = _resolve_template_vars(recipe)

    # This will raise ValidationError if the recipe is invalid in any way.
    PipelineConfig.model_validate(recipe_dict)


@pytest.mark.parametrize("filename", FIXTURES)
def test_fixture_sink_does_not_use_sync_mode(filename):
    # SYNC is banned here, and this assertion is the guard against it coming back.
    #
    # On GMS v1.5.0.6 the datahub-rest sink in SYNC mode writes records but does
    # not count them: total_records_written stays 0 and the pipeline reports
    # "produced 0 events". Measured with a plain file source yielding one aspect --
    # SYNC reported 0 where ASYNC and ASYNC_BATCH both reported 3 -- and the write
    # itself was confirmed by reading systemMetadata.lastObserved back from GMS,
    # which updated 1.8s after a run the sink had reported as empty.
    #
    # That matters because the milestone verifier and the capability check both read
    # that count, so SYNC makes a healthy run look like it emitted nothing. SYNC was
    # originally set here on the theory that it would remove an async-batch race in
    # the M3 assertion; it did not fix M3 (the real defect was the assertion's
    # direction) and it broke the accounting instead.
    recipe = yaml.safe_load((FIXTURE_DIR / filename).read_text())
    assert recipe["sink"]["config"].get("mode") != "SYNC"


def _resolve_template_vars(recipe: dict[str, Any]) -> dict[str, Any]:
    """Recursively resolve ${VAR} placeholders with dummy values for validation."""
    dummy_vars = {
        "SNOWFLAKE_ACCOUNT": "dummy_account",
        "SNOWFLAKE_USER": "dummy_user",
        "SNOWFLAKE_PRIVATE_KEY": "dummy_key",
        "SNOWFLAKE_ROLE": "dummy_role",
        "SNOWFLAKE_WAREHOUSE": "dummy_warehouse",
        "DATAHUB_GMS_URL": "http://localhost:8080",
        "DATAHUB_GMS_TOKEN": "dummy_token",
    }

    def resolve_value(val: Any) -> Any:
        if isinstance(val, str):
            # Replace ${VAR} or ${VAR:-default} placeholders
            if val.startswith("${") and val.endswith("}"):
                var_expr = val[2:-1]
                if ":-" in var_expr:
                    var_name, default_val = var_expr.split(":-", 1)
                    return dummy_vars.get(var_name, default_val)
                else:
                    return dummy_vars.get(var_expr, val)
            return val
        elif isinstance(val, dict):
            return {k: resolve_value(v) for k, v in val.items()}
        elif isinstance(val, list):
            return [resolve_value(item) for item in val]
        else:
            return val

    return resolve_value(recipe)
