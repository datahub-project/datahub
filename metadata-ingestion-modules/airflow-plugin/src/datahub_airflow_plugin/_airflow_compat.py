# Must be imported before any Airflow imports — applies the MarkupSafe patch
# and the Airflow 3 OpenLineage patches.
from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

# Use an explicit RuntimeError instead of `assert` so the check still runs
# under `python -O` (which strips assertions).
if not MARKUPSAFE_PATCHED:
    raise RuntimeError(
        "MarkupSafe compatibility patch must be applied before importing Airflow modules. "
        "This is a critical safety check that cannot be disabled. "
        "The patch ensures compatibility between different MarkupSafe versions used by "
        "Airflow and DataHub dependencies."
    )

from datahub_airflow_plugin.airflow3._airflow_compat import AIRFLOW_PATCHED

__all__ = [
    "AIRFLOW_PATCHED",
]
