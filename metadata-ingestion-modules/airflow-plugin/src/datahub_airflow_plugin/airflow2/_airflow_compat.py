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

# Check if OpenLineage provider package is available
# Use try-except because find_spec can raise ModuleNotFoundError if parent module doesn't exist
try:
    has_openlineage_provider = (
        importlib.util.find_spec("airflow.providers.openlineage.sqlparser") is not None
    )
except (ModuleNotFoundError, ImportError, ValueError):
    # Parent module doesn't exist or other import error
    has_openlineage_provider = False

if has_openlineage_provider:
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
    # Note: We defer the import to avoid potential issues with Airflow 3.x specific imports
    # in Airflow 2.x environments. The patch function itself handles version compatibility.
    import logging

    logger = logging.getLogger(__name__)
    try:
        logger.debug("Attempting to import and apply TeradataOperator patch")
        # Use importlib to safely import the patch module
        import importlib.util

        patch_module_path = (
            "datahub_airflow_plugin.airflow3._teradata_openlineage_patch"
        )
        patch_module = importlib.import_module(patch_module_path)
        patch_teradata_operator = patch_module.patch_teradata_operator

        patch_teradata_operator()
        logger.debug("TeradataOperator patch import and call completed")
    except ImportError as e:
        # Teradata provider not installed or patch not available
        logger.debug(f"Could not import TeradataOperator patch: {e}")
    except Exception as e:
        # Log error but don't fail - this is optional functionality
        logger.warning(f"Error applying TeradataOperator patch: {e}", exc_info=True)

AIRFLOW_PATCHED = True

__all__ = [
    "AIRFLOW_PATCHED",
]
