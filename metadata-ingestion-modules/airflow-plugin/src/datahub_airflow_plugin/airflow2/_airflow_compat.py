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

# Apply SQLParser patch for Airflow 2.10+ with apache-airflow-providers-openlineage
# When using the provider package, SQL operators call SQLParser.generate_openlineage_metadata_from_sql()
# directly (similar to Airflow 3.x), so we need to patch that method to use DataHub's SQL parser.
#
# For legacy openlineage-airflow package (Airflow 2.5-2.9), we use the extractor-based approach
# in _extractors.py instead.
import importlib.util

if importlib.util.find_spec("airflow.providers.openlineage.sqlparser") is not None:
    # Provider package detected - apply SQL parser patch
    from datahub_airflow_plugin.airflow2._airflow2_sql_parser_patch import (
        patch_sqlparser,
    )

    patch_sqlparser()
else:
    # Provider package not available - using legacy openlineage-airflow package
    # No patching needed, extractors will handle SQL parsing
    pass

AIRFLOW_PATCHED = True

__all__ = [
    "AIRFLOW_PATCHED",
]
