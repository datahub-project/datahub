# This module must be imported before any Airflow imports in any of our files.
# The AIRFLOW_PATCHED just helps avoid flake8 errors.

from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

assert MARKUPSAFE_PATCHED

AIRFLOW_PATCHED = True

__all__ = [
    "AIRFLOW_PATCHED",
]
