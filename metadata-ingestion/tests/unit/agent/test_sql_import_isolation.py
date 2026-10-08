"""Ingestion of a SQL source must not import the probe framework.

Every SQL source imports sql_config, whose SQLCommonConfig declares the probe
hooks; the framework behind them is imported only when the probe calls one.
Each import runs in a fresh interpreter, since this process's sys.modules
already holds whatever the other tests imported.
"""

import json
import subprocess
import sys
from typing import List

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


def _framework_modules_loaded_by(module: str) -> List[str]:
    script = (
        "import importlib, json, sys\n"
        f"importlib.import_module({module!r})\n"
        f"print(json.dumps([m for m in {list(_PROBE_FRAMEWORK_MODULES)!r} "
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
