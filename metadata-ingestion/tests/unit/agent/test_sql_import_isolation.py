"""Ingestion of a SQL source must not import the probe framework.

Every SQL source imports sql_config, whose SQLCommonConfig declares the probe
hooks; the framework behind them is imported only when the probe calls one.
Each import runs in a fresh interpreter, since this process's sys.modules
already holds whatever the other tests imported.
"""

import json
import subprocess
import sys
from typing import List, Sequence

import pytest

# The framework modules behind the probe's commands, none of which ingestion
# needs. `requests` is not among them: sql_config reaches it through the graph
# client whatever the probe does.
_PROBE_FRAMEWORK_MODULES = (
    "datahub.ingestion.agent.probe_methods",
    "datahub.ingestion.agent.declarations",
    "datahub.ingestion.agent.error_policy",
    "datahub.ingestion.agent.log_guard",
    "datahub.ingestion.agent.redact",
    "datahub.ingestion.agent.api_gate",
    "datahub.ingestion.agent.sql_passthrough",
)


# The object-store and Glue probe modules, which only `probe` needs.
_PROBE_ONLY_SOURCE_MODULES = (
    "datahub.ingestion.source.aws.glue_probe",
    "datahub.ingestion.source.data_lake_common.object_store_probe",
    "datahub.ingestion.source.data_lake_common.path_spec_verdict",
    "datahub.ingestion.source.s3.s3_probe",
    "datahub.ingestion.source.gcs.gcs_probe",
)


def _framework_modules_loaded_by(
    module: str, candidates: Sequence[str] = _PROBE_FRAMEWORK_MODULES
) -> List[str]:
    script = (
        "import importlib, json, sys\n"
        f"importlib.import_module({module!r})\n"
        f"print(json.dumps([m for m in {list(candidates)!r} "
        "if m in sys.modules]))\n"
    )
    result = subprocess.run(
        [sys.executable, "-W", "ignore", "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize(
    "module",
    [
        "datahub.ingestion.source.sql.sql_config",
        "datahub.ingestion.source.sql.postgres.source",
        "datahub.ingestion.source.sql.mysql",
        "datahub.ingestion.source.snowflake.snowflake_config",
        "datahub.ingestion.source.unity.config",
        "datahub.ingestion.source.unity.source",
        # BI sources: their configs hold probe hooks, and their selection
        # modules are shared with the probe, but import only agent.verdicts.
        "datahub.ingestion.source.tableau.tableau",
        "datahub.ingestion.source.powerbi.config",
        "datahub.ingestion.source.powerbi.powerbi",
    ],
)
def test_sql_source_import_leaves_out_the_probe_framework(module: str) -> None:
    assert _framework_modules_loaded_by(module) == []


def test_sql_probe_verdicts_import_loads_the_framework() -> None:
    # The control: the check above sees the framework when it is imported.
    loaded = _framework_modules_loaded_by(
        "datahub.ingestion.source.sql.sql_probe_verdicts"
    )
    assert "datahub.ingestion.agent.probe_methods" in loaded


@pytest.mark.parametrize(
    "module",
    [
        "datahub.ingestion.source.aws.glue",
        "datahub.ingestion.source.s3.source",
        "datahub.ingestion.source.gcs.gcs_source",
    ],
)
def test_object_store_and_glue_import_leaves_out_the_probe(module: str) -> None:
    assert (
        _framework_modules_loaded_by(
            module, _PROBE_FRAMEWORK_MODULES + _PROBE_ONLY_SOURCE_MODULES
        )
        == []
    )
