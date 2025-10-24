# This module must be imported before any Airflow imports in any of our files.
# The AIRFLOW_PATCHED just helps avoid flake8 errors.

from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

assert MARKUPSAFE_PATCHED

# Patch Airflow 3.0's SQLParser to use DataHub's SQL parser
# These imports must be after MARKUPSAFE_PATCHED assertion because they import Airflow modules.
# We need to ensure markupsafe is patched first to maintain compatibility.
# The noqa: E402 suppresses "module level import not at top of file" linting errors.
from datahub_airflow_plugin._airflow3_sql_parser_patch import patch_sqlparser  # noqa: E402, I001
from datahub_airflow_plugin._athena_openlineage_patch import patch_athena_operator  # noqa: E402
from datahub_airflow_plugin._bigquery_openlineage_patch import (  # noqa: E402
    patch_bigquery_insert_job_operator,
)
from datahub_airflow_plugin._sqlite_openlineage_patch import patch_sqlite_hook  # noqa: E402

patch_sqlparser()
patch_sqlite_hook()
patch_athena_operator()
patch_bigquery_insert_job_operator()

AIRFLOW_PATCHED = True

__all__ = [
    "AIRFLOW_PATCHED",
]
