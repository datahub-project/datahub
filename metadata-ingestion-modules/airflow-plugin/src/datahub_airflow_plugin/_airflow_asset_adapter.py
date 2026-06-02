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


def is_airflow_asset_alias(obj: Any) -> bool:
    """Check if object is an Airflow AssetAlias (Airflow 3.x only).

    AssetAlias has a 'name' attribute but no 'uri'. It represents a named
    alias that resolves to concrete Assets at task runtime, enabling
    asset-triggered DAGs when the actual asset URI is only known at execution
    time (e.g. contains Jinja templates).
    """
    if not hasattr(obj, "name"):
        return False
    # Exclude Asset/Dataset objects — some versions also carry a 'name' attr
    if hasattr(obj, "uri"):
        return False
    for cls in type(obj).__mro__:
        if cls.__name__ == "AssetAlias":
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
            # strip() removes both the leading slash that urlsplit puts between
            # netloc and path, and any trailing slash that Airflow's _sanitize_uri
            # injects for netloc-only URIs (e.g. iceberg://catalog → stored as
            # iceberg://catalog/ → name should be "catalog", not "catalog/").
            name = f"{parsed.netloc}{parsed.path}".strip("/")
        else:
            name = parsed.path.strip("/")
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


def translate_airflow_asset_alias_to_urn(
    alias: Any, env: str = "PROD"
) -> Optional[str]:
    """Convert an Airflow AssetAlias to a DataHub dataset URN.

    Uses the alias name as the dataset name on the 'airflow' platform,
    consistent with how plain-name Assets (no URI scheme) are handled in
    translate_airflow_asset_to_urn().

    Args:
        alias: An Airflow AssetAlias object with a 'name' attribute.
        env: The DataHub environment (default: "PROD").

    Returns:
        A DataHub dataset URN string, or None if the alias name is empty.

    Example:
        >>> translate_airflow_asset_alias_to_urn(mock_alias("parsed_data"))
        "urn:li:dataset:(urn:li:dataPlatform:airflow,parsed_data,PROD)"
    """
    name = getattr(alias, "name", None)
    if not name:
        return None
    try:
        return builder.make_dataset_urn(platform="airflow", name=name, env=env)
    except Exception as e:
        logger.warning(
            f"Failed to create DataHub URN for Airflow AssetAlias '{name}': {e}. "
            f"This alias will be excluded from lineage."
        )
        return None


def extract_urns_from_task_instance_outlet_events(
    task_instance: Any,
    env: str = "PROD",
) -> List[str]:
    """Extract runtime-resolved AssetAlias URNs from the task instance's execution context.

    In Airflow 3.x, the listener fires inside the task worker process where
    direct DB access is intentionally blocked (``create_session()`` raises).
    Outlet events accumulated during task execution are accessible via an
    ``OutletEventAccessors`` object that was mutated in-place as the task ran.

    This function supports two sources for the ``OutletEventAccessors``:

    1. ``task_instance._datahub_outlet_events`` — set by our own
       ``on_starting`` patch (see ``_patch_runtime_ti_for_outlet_events`` in
       ``datahub_listener.py``).  Works on **all** Airflow 3.x versions.
    2. ``task_instance._cached_template_context["outlet_events"]`` — set by
       Airflow 3.2.x natively on ``RuntimeTaskInstance``.

    Only returns URNs for assets added via ``outlet_events[AssetAlias(...)].add(Asset(...))``
    during task execution. Direct ``Asset`` outlets are already captured statically.
    """
    try:
        from airflow.sdk.definitions.asset import AssetAlias
        from airflow.sdk.execution_time.context import OutletEventAccessors
    except ImportError:
        return []

    try:
        # Primary path: our own attribute set by _patch_runtime_ti_for_outlet_events
        outlet_events = getattr(task_instance, "_datahub_outlet_events", None)

        # Fallback: Airflow 3.2.x stores _cached_template_context as a PrivateAttr
        if outlet_events is None:
            context = getattr(task_instance, "_cached_template_context", None)
            if context is None:
                pydantic_private = getattr(task_instance, "__pydantic_private__", None)
                if pydantic_private:
                    context = pydantic_private.get("_cached_template_context")
            if context:
                outlet_events = context.get("outlet_events")

        if outlet_events is None:
            logger.debug(
                "extract_urns_from_task_instance_outlet_events: "
                "outlet_events not available on task_instance — "
                "on_starting patch may not have fired (capture_airflow_assets=False?)"
            )
            return []

        if not isinstance(outlet_events, OutletEventAccessors):
            logger.debug(
                f"extract_urns_from_task_instance_outlet_events: "
                f"outlet_events is {type(outlet_events).__name__}, not OutletEventAccessors"
            )
            return []

        urns: List[str] = []
        for key in outlet_events:
            if not isinstance(key, AssetAlias):
                continue
            accessor = outlet_events[key]
            for event in accessor.asset_alias_events:
                asset = event.dest_asset_key.to_asset()
                urn = translate_airflow_asset_to_urn(asset, env=env)
                if urn:
                    urns.append(urn)
                    logger.info(
                        f"Resolved AssetAlias '{key.name}' via in-process context: {urn}"
                    )
        return urns
    except Exception as e:
        logger.warning(
            f"Could not read outlet events from task instance context: {e}",
            exc_info=True,
        )
        return []


def extract_urns_from_resolved_alias_events(
    dag_id: str,
    task_id: str,
    run_id: str,
    map_index: int = -1,
    env: str = "PROD",
    session: Optional[Any] = None,
) -> List[str]:
    """Query runtime-resolved AssetAlias events from the Airflow DB.

    Called at task completion to find actual Asset URIs that were emitted via
    AssetAlias at runtime (e.g. Jinja-rendered S3 paths). Only includes events
    that originated from an AssetAlias; direct Asset outlet events are already
    captured statically and excluded here to avoid duplicates.

    Args:
        session: An active SQLAlchemy session provided by the Airflow listener
            hook (preferred). When provided, ``create_session()`` is NOT called,
            which is required for Airflow 3.0 where direct session creation is
            blocked inside listener threads. Falls back to ``create_session()``
            when ``session`` is None (e.g. in tests or non-Airflow contexts).

    Requires Airflow 3.x ORM (airflow.models.asset.AssetEvent). Returns an
    empty list if the models are unavailable or any query fails.
    """
    try:
        from airflow.models.asset import AssetEvent
        from sqlalchemy import and_, select
    except ImportError:
        return []

    def _query(s: Any) -> List[str]:
        urns: List[str] = []
        events = s.scalars(
            select(AssetEvent).where(
                and_(
                    AssetEvent.source_dag_id == dag_id,
                    AssetEvent.source_run_id == run_id,
                    AssetEvent.source_task_id == task_id,
                    AssetEvent.source_map_index == map_index,
                )
            )
        ).all()
        for event in events:
            # Skip direct Asset outlets — only process alias-resolved events
            if not event.source_aliases:
                continue
            if not (event.asset and event.asset.uri):
                continue
            urn = translate_airflow_asset_to_urn(event.asset, env=env)
            if urn:
                urns.append(urn)
                logger.debug(
                    f"Resolved AssetAlias event: "
                    f"aliases={[a.name for a in event.source_aliases]} "
                    f"-> {urn}"
                )
        return urns

    try:
        if session is not None:
            return _query(session)

        # Fallback: create a new session (blocked in Airflow 3.0 listener threads;
        # available in tests and Airflow 2.x).
        from airflow.utils.session import create_session

        with create_session() as new_session:
            return _query(new_session)
    except Exception as e:
        logger.warning(
            f"Failed to query resolved AssetAlias events for "
            f"{dag_id}.{task_id} run={run_id}: {e}. "
            f"Runtime-resolved alias lineage will be missing."
        )
        return []


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
            elif capture_airflow_assets and is_airflow_asset_alias(iolet):
                urn = translate_airflow_asset_alias_to_urn(iolet, env=env)
                if urn:
                    urns.append(urn)
                else:
                    name = getattr(iolet, "name", None)
                    logger.warning(
                        f"Skipping Airflow AssetAlias with name '{name}' - "
                        f"could not convert to DataHub URN."
                    )
        except Exception as e:
            # Catch any unexpected errors to avoid breaking lineage extraction
            logger.warning(
                f"Failed to extract URN from iolet {type(iolet).__name__}: {e}. "
                f"Continuing with remaining iolets."
            )
            continue
    return urns
