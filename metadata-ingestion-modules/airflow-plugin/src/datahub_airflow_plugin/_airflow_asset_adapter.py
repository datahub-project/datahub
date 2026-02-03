"""Adapter for converting Airflow Assets/Datasets to DataHub URNs.

Airflow 2.4+ introduced airflow.datasets.Dataset for data-aware scheduling.
Airflow 3.0+ renamed this to airflow.sdk.Asset (Dataset still works as alias).

This module provides utilities to detect and convert these native Airflow
Asset/Dataset objects to DataHub dataset URNs based on their URI.
"""

import logging
from typing import Any, Iterable, List, Optional
from urllib.parse import urlparse

import datahub.emitter.mce_builder as builder
from datahub_airflow_plugin.entities import _Entity

logger = logging.getLogger(__name__)

# URI scheme to DataHub platform mapping
URI_SCHEME_TO_PLATFORM = {
    "s3": "s3",
    "s3a": "s3",
    "gs": "gcs",
    "gcs": "gcs",
    "file": "file",
    "hdfs": "hdfs",
    "abfs": "adls",
    "abfss": "adls",
    "postgresql": "postgres",
    "mysql": "mysql",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
}


def is_airflow_asset(obj: Any) -> bool:
    """Check if object is an Airflow Asset or Dataset.

    Works with both Airflow 2.x Dataset and Airflow 3.x Asset classes,
    including subclasses, by checking the class hierarchy (MRO) and
    required 'uri' attribute.
    """
    if not hasattr(obj, "uri"):
        return False

    # Check class name and all base classes in the MRO
    for cls in type(obj).__mro__:
        if cls.__name__ in ("Asset", "Dataset"):
            return True

    return False


def translate_airflow_asset_to_urn(
    asset: Any, env: str = "PROD", platform_fallback: str = "airflow"
) -> Optional[str]:
    """Convert Airflow Asset URI to DataHub dataset URN.

    Args:
        asset: An Airflow Asset or Dataset object with a 'uri' attribute.
        env: The DataHub environment (default: "PROD").
        platform_fallback: Platform to use when URI has no scheme (default: "airflow").
            This is used for @asset decorated functions that don't specify a URI.

    Returns:
        A DataHub dataset URN string, or None if the asset URI is invalid.

    Examples:
        >>> translate_airflow_asset_to_urn(mock_asset("s3://bucket/path"))
        "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)"

        >>> translate_airflow_asset_to_urn(mock_asset("postgresql://host/db/table"))
        "urn:li:dataset:(urn:li:dataPlatform:postgres,host/db/table,PROD)"

        >>> translate_airflow_asset_to_urn(mock_asset("my_asset"))  # @asset decorator
        "urn:li:dataset:(urn:li:dataPlatform:airflow,my_asset,PROD)"
    """
    uri = getattr(asset, "uri", None)
    if not uri:
        return None

    try:
        parsed = urlparse(uri)
    except Exception as e:
        logger.warning(
            f"Failed to parse Airflow asset URI '{uri}': {e}. "
            f"This asset will be excluded from lineage."
        )
        return None

    scheme = parsed.scheme.lower() if parsed.scheme else ""

    if scheme:
        # URI has a scheme - map to DataHub platform
        platform = URI_SCHEME_TO_PLATFORM.get(scheme, scheme)
        # Build dataset name from URI components
        if parsed.netloc:
            name = f"{parsed.netloc}{parsed.path}".lstrip("/")
        else:
            name = parsed.path.lstrip("/")
    else:
        # No scheme - this is likely an @asset decorated function with just a name
        # Use the fallback platform (default: "airflow") and the URI as the name
        platform = platform_fallback
        name = uri

    # Ensure we have a valid name
    if not name:
        logger.debug(f"Airflow asset URI has empty name: '{uri}'. Skipping.")
        return None

    try:
        return builder.make_dataset_urn(platform=platform, name=name, env=env)
    except Exception as e:
        logger.warning(
            f"Failed to create DataHub URN for Airflow asset: {e}. "
            f"URI: '{uri}', platform: '{platform}', name: '{name}'. "
            f"This asset will be excluded from lineage."
        )
        return None


def extract_urns_from_iolets(
    iolets: Iterable[Any],
    capture_airflow_assets: bool,
    env: str = "PROD",
) -> List[str]:
    """Extract URNs from a list of inlets or outlets.

    Processes both DataHub entity objects and native Airflow Assets/Datasets.

    Args:
        iolets: Iterable of inlet/outlet objects (from task.inlets or task.outlets).
        capture_airflow_assets: Whether to capture native Airflow Assets.
        env: The DataHub environment (default: "PROD").

    Returns:
        List of URN strings extracted from the iolets.
    """
    urns: List[str] = []
    for iolet in iolets:
        try:
            if isinstance(iolet, _Entity):
                urns.append(iolet.urn)
            elif capture_airflow_assets and is_airflow_asset(iolet):
                urn = translate_airflow_asset_to_urn(iolet, env=env)
                if urn:
                    urns.append(urn)
                else:
                    # translate_airflow_asset_to_urn already logs details
                    uri = getattr(iolet, "uri", None)
                    logger.warning(
                        f"Skipping Airflow asset with URI '{uri}' - "
                        f"could not convert to DataHub URN. "
                        f"Check that the URI is valid and has a proper scheme."
                    )
        except Exception as e:
            # Catch any unexpected errors to avoid breaking lineage extraction
            logger.warning(
                f"Failed to extract URN from iolet {type(iolet).__name__}: {e}. "
                f"Continuing with remaining iolets."
            )
            continue
    return urns
