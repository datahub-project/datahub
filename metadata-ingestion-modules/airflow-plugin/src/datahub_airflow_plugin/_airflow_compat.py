# This module must be imported before any Airflow imports in any of our files.
# It dispatches to version-specific compatibility modules.

from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

# Critical safety check: Ensure MarkupSafe compatibility patch is applied
# This must happen before importing Airflow to prevent MarkupSafe version conflicts
# Using explicit exception instead of assert to ensure it runs even with python -O
if not MARKUPSAFE_PATCHED:
    raise RuntimeError(
        "MarkupSafe compatibility patch must be applied before importing Airflow modules. "
        "This is a critical safety check that cannot be disabled. "
        "The patch ensures compatibility between different MarkupSafe versions used by "
        "Airflow and DataHub dependencies."
    )

# Detect Airflow version and dispatch to version-specific compat module
# These imports must be after MARKUPSAFE_PATCHED assertion.
import airflow
import packaging.version

AIRFLOW_VERSION = packaging.version.parse(airflow.__version__)
IS_AIRFLOW_3_OR_HIGHER = AIRFLOW_VERSION >= packaging.version.parse("3.0.0")

if IS_AIRFLOW_3_OR_HIGHER:
    from datahub_airflow_plugin.airflow3._airflow_compat import AIRFLOW_PATCHED
else:
    from datahub_airflow_plugin.airflow2._airflow_compat import AIRFLOW_PATCHED

__all__ = [
    "AIRFLOW_PATCHED",
]
