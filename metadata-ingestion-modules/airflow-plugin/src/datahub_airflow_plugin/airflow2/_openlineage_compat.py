"""
Compatibility layer for OpenLineage imports in Airflow 2.x.

This module handles two different OpenLineage variants that can be used with Airflow 2.x:
1. Legacy OpenLineage (openlineage-airflow package) - used in Airflow 2.5-2.6
2. OpenLineage Provider (apache-airflow-providers-openlineage) - used in Airflow 2.7+

The module detects which variant is installed and imports the appropriate classes.

Note: This file is only used for Airflow 2.x. Airflow 3.x has its own separate module.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # For type checking, use proper types based on what's available
    # Try OpenLineage Provider first, fall back to Legacy OpenLineage
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

        USE_OPENLINEAGE_PROVIDER: bool = True
    except ImportError:
        # Legacy OpenLineage types
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

        OperatorLineage: Any  # type: ignore[no-redef]  # Doesn't exist in Legacy OpenLineage
        USE_OPENLINEAGE_PROVIDER: bool = False  # type: ignore[no-redef]

else:
    # Runtime imports - detect which OpenLineage variant is installed
    USE_OPENLINEAGE_PROVIDER = False

    try:
        # Try OpenLineage Provider (apache-airflow-providers-openlineage)
        # Available in Airflow 2.7+ when installed with [airflow2-provider] extra
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

        USE_OPENLINEAGE_PROVIDER = True

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

        # OpenLineage Provider uses OperatorLineage, not TaskMetadata
        TaskMetadata = None  # type: ignore

    except (ImportError, ModuleNotFoundError):
        # Fall back to Legacy OpenLineage (openlineage-airflow package)
        # Used in Airflow 2.5-2.6 or when installed with [airflow2] extra
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

        # Legacy OpenLineage uses TaskMetadata, not OperatorLineage
        OperatorLineage = None  # type: ignore


# Export all symbols
__all__ = [
    "USE_OPENLINEAGE_PROVIDER",
    "BaseExtractor",
    "OperatorLineage",
    "TaskMetadata",
    "OLExtractorManager",
    "get_operator_class",
    "try_import_from_string",
    "SnowflakeExtractor",
    "SqlExtractor",
]
