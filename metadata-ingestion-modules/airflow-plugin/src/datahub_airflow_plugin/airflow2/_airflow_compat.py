# Airflow 2.x compatibility module
# This module must be imported before any Airflow imports in any of our files.

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

# Airflow 2.x doesn't need the OpenLineage patches that are specific to Airflow 3.x
# Those patches target Airflow 3.0+ features like:
# - SQLParser.generate_openlineage_metadata_from_sql()
# - get_openlineage_facets_on_complete() methods
# - get_openlineage_database_info() for SqliteHook
#
# These don't exist in Airflow 2.x, so we don't apply any patches here.

AIRFLOW_PATCHED = True

__all__ = [
    "AIRFLOW_PATCHED",
]
