# Airflow 3.x compatibility module
# This module must be imported before any Airflow imports in any of our files.

import logging
from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

logger = logging.getLogger(__name__)

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

# Apply Airflow 3.x patches
# These imports must be after MARKUPSAFE_PATCHED assertion because they import Airflow modules.
# We need to ensure markupsafe is patched first to maintain compatibility.

# Load configuration to determine which patches to apply
try:
    from datahub_airflow_plugin._config import get_lineage_config

    config = get_lineage_config()
    enable_extractors = config.enable_extractors
    patch_sql_parser = config.patch_sql_parser
    extract_athena_operator = config.extract_athena_operator
    extract_bigquery_insert_job_operator = config.extract_bigquery_insert_job_operator
    extract_teradata_operator = config.extract_teradata_operator
except Exception:
    # If config loading fails, apply all patches by default (backward compatibility)
    enable_extractors = True
    patch_sql_parser = True
    extract_athena_operator = True
    extract_bigquery_insert_job_operator = True
    extract_teradata_operator = True

# Only apply patches if extractors are enabled
if enable_extractors:
    # Airflow 3.0+ SQLParser patch
    if patch_sql_parser:
        try:
            from datahub_airflow_plugin.airflow3._airflow3_sql_parser_patch import (
                patch_sqlparser,
            )

            patch_sqlparser()
            # Log success for debugging
            logger.debug("✓ Successfully applied Airflow 3 SQL parser patch")
        except ImportError as e:
            # Not available when openlineage packages aren't installed
            logger.warning(
                f"SQL parser patch not applied - OpenLineage packages not available: {e}"
            )
        except Exception as e:
            # Log any other errors
            logger.warning(f"Failed to apply SQL parser patch: {e}", exc_info=True)

    # Operator-specific patches (conditional based on config and operator availability)
    # SQLite patch is always applied when available (no config flag yet)
    try:
        from datahub_airflow_plugin.airflow3._sqlite_openlineage_patch import (
            patch_sqlite_hook,
        )

        patch_sqlite_hook()
    except ImportError:
        pass

    if extract_athena_operator:
        try:
            from datahub_airflow_plugin.airflow3._athena_openlineage_patch import (
                patch_athena_operator,
            )

            patch_athena_operator()
        except ImportError:
            pass

    if extract_bigquery_insert_job_operator:
        try:
            from datahub_airflow_plugin.airflow3._bigquery_openlineage_patch import (
                patch_bigquery_insert_job_operator,
            )

            patch_bigquery_insert_job_operator()
        except ImportError:
            pass

    if extract_teradata_operator:
        try:
            from datahub_airflow_plugin.airflow3._teradata_openlineage_patch import (
                patch_teradata_operator,
            )

            patch_teradata_operator()
        except ImportError:
            pass

AIRFLOW_PATCHED = True

__all__ = [
    "AIRFLOW_PATCHED",
]
