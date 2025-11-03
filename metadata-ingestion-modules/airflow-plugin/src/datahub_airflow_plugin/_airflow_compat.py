# This module must be imported before any Airflow imports in any of our files.
# It dispatches to version-specific compatibility modules.

from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

assert MARKUPSAFE_PATCHED

# Detect Airflow version and dispatch to version-specific compat module
# These imports must be after MARKUPSAFE_PATCHED assertion.
import airflow  # noqa: E402
import packaging.version  # noqa: E402

AIRFLOW_VERSION = packaging.version.parse(airflow.__version__)
IS_AIRFLOW_3_OR_HIGHER = AIRFLOW_VERSION >= packaging.version.parse("3.0.0")

if IS_AIRFLOW_3_OR_HIGHER:
    from datahub_airflow_plugin.plugin_v2_airflow3._airflow_compat import (
        AIRFLOW_PATCHED,
    )
else:
    from datahub_airflow_plugin.plugin_v2._airflow_compat import AIRFLOW_PATCHED

__all__ = [
    "AIRFLOW_PATCHED",
]
