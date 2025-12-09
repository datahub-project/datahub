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
import logging

logger = logging.getLogger(__name__)

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

# Apply operator-specific patches for provider mode
# These patches work for both Airflow 2.x and 3.x when using OpenLineage provider
try:
    from datahub_airflow_plugin._config import get_lineage_config

    config = get_lineage_config()
    enable_extractors = config.enable_extractors
    extract_teradata_operator = config.extract_teradata_operator
except Exception:
    # If config loading fails, apply patches by default (backward compatibility)
    enable_extractors = True
    extract_teradata_operator = True

if enable_extractors and extract_teradata_operator:
    # TeradataOperator patch - works for both Airflow 2.x provider mode and Airflow 3.x
    # The patch checks for method existence, so it's safe to import from airflow3 module
    try:
        from datahub_airflow_plugin.airflow3._teradata_openlineage_patch import (
            patch_teradata_operator,
        )

        patch_teradata_operator()
    except ImportError:
        # Teradata provider not installed or patch not available
        pass

AIRFLOW_PATCHED = True

__all__ = [
    "AIRFLOW_PATCHED",
]
