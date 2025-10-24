"""
Compatibility layer for OpenLineage imports across Airflow 2 and Airflow 3.

This module centralizes all OpenLineage-related imports to avoid mypy redefinition
errors when the same classes are imported from different packages depending on the
Airflow version.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # For type checking, use proper types based on what's available
    # Try Airflow 3 first, fall back to Airflow 2
    try:
        from airflow.providers.openlineage.extractors.base import (
            BaseExtractor,
            OperatorLineage,
        )
        from airflow.providers.openlineage.extractors.manager import (
            ExtractorManager as OLExtractorManager,
        )
        from airflow.providers.openlineage.extractors.snowflake import (
            SnowflakeExtractor,
        )
        from airflow.providers.openlineage.extractors.sql import SqlExtractor
        from airflow.providers.openlineage.utils.utils import (
            get_operator_class,
            try_import_from_string,
        )
        from openlineage.airflow.extractors import TaskMetadata

        IS_AIRFLOW_3: bool = True
    except ImportError:
        # Airflow 2 types
        from openlineage.airflow.extractors import (  # type: ignore[no-redef]
            BaseExtractor,
            ExtractorManager as OLExtractorManager,
            TaskMetadata,
        )
        from openlineage.airflow.extractors.snowflake_extractor import (  # type: ignore[no-redef]
            SnowflakeExtractor,
        )
        from openlineage.airflow.extractors.sql_extractor import (
            SqlExtractor,  # type: ignore[no-redef]
        )
        from openlineage.airflow.utils import (  # type: ignore[no-redef]
            get_operator_class,
            try_import_from_string,
        )

        OperatorLineage: Any  # type: ignore[no-redef]  # Doesn't exist in Airflow 2
        IS_AIRFLOW_3: bool = False  # type: ignore[no-redef]

else:
    # Runtime imports - conditionally import based on Airflow version
    IS_AIRFLOW_3 = False

    try:
        # Airflow 3.x: Use native OpenLineage provider
        from airflow.providers.openlineage.extractors.base import (
            BaseExtractor,
            OperatorLineage,
        )
        from airflow.providers.openlineage.extractors.manager import (
            ExtractorManager as OLExtractorManager,
        )
        from airflow.providers.openlineage.utils.utils import (
            get_operator_class,
            try_import_from_string,
        )

        IS_AIRFLOW_3 = True

        try:
            from airflow.providers.openlineage.extractors.snowflake import (
                SnowflakeExtractor,
            )
        except ImportError:
            SnowflakeExtractor = None  # type: ignore

        try:
            from airflow.providers.openlineage.extractors.sql import SqlExtractor
        except ImportError:
            SqlExtractor = None  # type: ignore

        # For Airflow 3, TaskMetadata doesn't exist
        # Define it as a runtime type for compatibility
        TaskMetadata: Any = type("TaskMetadata", (), {})

    except (ImportError, ModuleNotFoundError):
        # Airflow 2.x: Use standalone openlineage-airflow package
        from openlineage.airflow.extractors import (
            BaseExtractor,
            ExtractorManager as OLExtractorManager,
            TaskMetadata,
        )
        from openlineage.airflow.extractors.snowflake_extractor import (
            SnowflakeExtractor,
        )
        from openlineage.airflow.extractors.sql_extractor import SqlExtractor
        from openlineage.airflow.utils import get_operator_class, try_import_from_string

        # For Airflow 2, OperatorLineage doesn't exist
        OperatorLineage: Any = type("OperatorLineage", (), {})


# Export all symbols
__all__ = [
    "IS_AIRFLOW_3",
    "BaseExtractor",
    "OperatorLineage",
    "TaskMetadata",
    "OLExtractorManager",
    "get_operator_class",
    "try_import_from_string",
    "SnowflakeExtractor",
    "SqlExtractor",
]
