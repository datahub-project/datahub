# Airflow 3.x compatibility module
# This module must be imported before any Airflow imports in any of our files.

from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

assert MARKUPSAFE_PATCHED

# Apply Airflow 3.x patches
# These imports must be after MARKUPSAFE_PATCHED assertion because they import Airflow modules.
# We need to ensure markupsafe is patched first to maintain compatibility.

# Airflow 3.0+ SQLParser patch
try:
    from datahub_airflow_plugin.plugin_v2_airflow3._airflow3_sql_parser_patch import (
        patch_sqlparser,
    )

    patch_sqlparser()
except ImportError:
    # Not available when openlineage packages aren't installed
    pass

# Operator-specific patches (conditional based on operator availability)
try:
    from datahub_airflow_plugin.plugin_v2_airflow3._sqlite_openlineage_patch import (
        patch_sqlite_hook,
    )

    patch_sqlite_hook()
except ImportError:
    pass

try:
    from datahub_airflow_plugin.plugin_v2_airflow3._athena_openlineage_patch import (
        patch_athena_operator,
    )

    patch_athena_operator()
except ImportError:
    pass

try:
    from datahub_airflow_plugin.plugin_v2_airflow3._bigquery_openlineage_patch import (
        patch_bigquery_insert_job_operator,
    )

    patch_bigquery_insert_job_operator()
except ImportError:
    pass

AIRFLOW_PATCHED = True

__all__ = [
    "AIRFLOW_PATCHED",
]
