# ruff: noqa: INP001
"""
Shared utilities and constants for Time Series Explorer pages.

This module contains:
- Session state keys
- Shared helper functions
- Configuration dataclasses
- Logging setup
"""

import logging

# Import shared config for connection status
# shared_config is in scripts/ directory (added to sys.path by assertions_ui.py)
# For tests, we add scripts to path via conftest or the test imports
import sys as _sys
import threading
from dataclasses import dataclass, field
from pathlib import Path as _Path

import pandas as pd  # noqa: F401
import plotly.graph_objects as go  # type: ignore[import-untyped]  # noqa: F401
import streamlit as st

# Local imports - using relative imports within common package
# These are re-exported for use by other modules
from .cache_manager import (  # noqa: F401
    ALL_ASPECTS,
    METRIC_CUBE_ASPECTS,
    MONITOR_ASPECTS,
    AnomalyEdit,
    AnomalyEditTracker,
    get_cache_dir,
    hostname_to_dir,
    url_to_hostname,
)
from .data_loaders import DataLoader
from .env_config import DataHubEnvConfig  # noqa: F401
from .preprocessing_ui import (  # noqa: F401
    apply_preprocessing,
    render_before_after_chart,
    render_preprocessing_config_panel,
    render_preprocessing_stats,
)

# Ensure scripts directory is in path for imports
_scripts_dir = _Path(__file__).parent.parent.parent
if str(_scripts_dir) not in _sys.path:
    _sys.path.insert(0, str(_scripts_dir))

from shared_config import (  # type: ignore[import-not-found]  # noqa: E402
    ACTIVE_ENV_CONFIG,
    get_active_config,
    render_connection_status,
)

# Configure module logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# =============================================================================
# Session State Keys
# =============================================================================

_SELECTED_ENDPOINT = "ts_explorer_endpoint"
_SELECTED_ASSERTION = "ts_explorer_assertion"
_LOADED_TIMESERIES = "ts_explorer_timeseries"
_DATA_SOURCE = "ts_explorer_data_source"
_FETCH_CANCELLED = "ts_explorer_fetch_cancelled"
_FETCH_IN_PROGRESS = "ts_explorer_fetch_in_progress"
_MONITOR_URN_FOR_ASSERTION = "ts_explorer_monitor_urn"
_RAW_EVENTS_DF = "ts_explorer_raw_events"
_SELECTED_MONITOR = "ts_explorer_monitor"

# Thread-safe cancel flag (threading.Event is thread-safe)
_cancel_event = threading.Event()

# Filter options for assertion type and field metric (Metric Cube Browser, etc.)
# Label -> API value; None means "All"
ASSERTION_TYPE_FILTER_OPTIONS: dict[str, str | None] = {
    "All Types": None,
    "Volume": "VOLUME",
    "Field": "FIELD",
    "SQL": "SQL",
    "Freshness": "FRESHNESS",
}
FIELD_METRIC_FILTER_OPTIONS: dict[str, str | None] = {
    "All Metrics": None,
    "Null Count": "NULL_COUNT",
    "Null Percentage": "NULL_PERCENTAGE",
    "Unique Count": "UNIQUE_COUNT",
    "Unique Percentage": "UNIQUE_PERCENTAGE",
    "Min": "MIN",
    "Max": "MAX",
    "Mean": "MEAN",
    "Median": "MEDIAN",
    "Std Dev": "STD_DEV",
}


def set_explorer_context(
    hostname: str,
    assertion_urn: str,
    monitor_urn: str | None = None,
) -> None:
    """Set session state for downstream pages (Preprocessing, Model Training, etc.).

    Call this when navigating from Metric Cube Browser or similar so that
    model_explorer pages receive the same hostname and assertion. Uses both
    the shared constants and legacy keys for backward compatibility.
    """
    st.session_state[_SELECTED_ENDPOINT] = hostname
    st.session_state[_SELECTED_ASSERTION] = assertion_urn
    if monitor_urn is not None:
        st.session_state[_MONITOR_URN_FOR_ASSERTION] = monitor_urn
    st.session_state["current_hostname"] = hostname
    st.session_state["selected_assertion_urn"] = assertion_urn


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class FetchConfig:
    """Configuration for API fetch operation.

    This config uses monitors as the primary entity for data fetching.
    Metric cubes provide timeseries data, and monitor anomaly events
    provide anomaly detection data.
    """

    lookback_days: int = 30
    monitor_urns: list[str] = field(default_factory=list)  # Optional filter
    monitor_aspects: list[str] = field(default_factory=lambda: ["monitorAnomalyEvent"])
    metric_cube_aspects: list[str] = field(
        default_factory=lambda: ["dataHubMetricCubeEvent"]
    )
    alias: str = ""
    sync_mode: str = "full"
    max_workers: int = 30  # Concurrency for parallel API requests
    include_active_monitors: bool = True  # Include monitors with ACTIVE status
    include_inactive_monitors: bool = (
        True  # Include monitors with INACTIVE/PAUSED status
    )
    fetch_inference_data: bool = True  # Fetch model configs and training evals


# =============================================================================
# Helper Functions
# =============================================================================


def _normalize_hostname_for_comparison(host: str) -> str:
    """Normalize hostname for endpoint vs connection comparison (lowercase, strip default ports)."""
    if not host:
        return ""
    host = host.strip().lower()
    for default_port in (":443", ":80"):
        if host.endswith(default_port):
            host = host[: -len(default_port)]
            break
    return host


def connection_matches_endpoint(endpoint_hostname: str) -> tuple[bool, str | None]:
    """Check whether the active DataHub connection matches the selected endpoint.

    When they differ, API calls (e.g. assertion type enrichment) would hit the wrong
    server and fail or return no data.

    Returns:
        (True, None) if the active connection matches the endpoint.
        (False, message) if no config, or connection and endpoint differ; message is
        suitable for st.warning().
    """
    config = get_active_config()
    if not config:
        return (
            False,
            "No DataHub connection configured. Assertion type info cannot be fetched. "
            "Configure your connection (e.g. Data Source or ~/.datahubenv).",
        )
    config_host = url_to_hostname(config.server)
    if _normalize_hostname_for_comparison(
        config_host
    ) != _normalize_hostname_for_comparison(endpoint_hostname):
        return (
            False,
            f"The active DataHub connection ({config.server}) does not match the selected "
            f"endpoint ({endpoint_hostname}). Assertion type info cannot be fetched. "
            "Switch your connection to this endpoint or load data from it.",
        )
    return (True, None)


def _extract_hash_from_run_id(run_id: str) -> str:
    """Extract the hash portion from a run ID.

    Supports formats used by inference_v2 and anomaly comparison:
    - {model_key}__{base_id}__{hash}
    - auto_v2_{hash}
    - auto_v2_eval__auto_v2_{hash}
    - auto_v2::{prefixed} (anomaly comparison prefixed key)

    Returns:
        Hash portion (up to 8 characters), or empty string if format doesn't match.
    """
    if not run_id:
        return ""
    # Anomaly comparison uses prefixed keys: auto_v2::auto_v2_{hash}
    if run_id.startswith("auto_v2::"):
        return _extract_hash_from_run_id(run_id[len("auto_v2::") :])

    # Format: auto_v2_eval__auto_v2_{hash}
    if run_id.startswith("auto_v2_eval__"):
        remaining = run_id[len("auto_v2_eval__") :]
        if remaining.startswith("auto_v2_"):
            hash_part = remaining[len("auto_v2_") :]
            return hash_part[:8] if len(hash_part) >= 8 else hash_part
        return remaining[:8] if len(remaining) >= 8 else remaining

    if run_id.startswith("auto_v2_"):
        hash_part = run_id[len("auto_v2_") :]
        return hash_part[:8] if len(hash_part) >= 8 else hash_part

    parts = run_id.split("__")
    if len(parts) >= 3:
        return parts[-1]
    return ""


def _shorten_urn(urn: str, max_length: int = 60) -> str:
    """Shorten a URN by truncating the end with ellipsis.

    Args:
        urn: The URN to shorten
        max_length: Maximum length before truncation

    Returns:
        Truncated URN with ... at the end if too long
    """
    if not urn:
        return ""

    if len(urn) <= max_length:
        return urn

    # Truncate from end with ellipsis
    return urn[: max_length - 3] + "..."


def _hex_to_rgba(hex_color: str, alpha: float) -> tuple:
    """Convert hex color to RGBA tuple.

    Args:
        hex_color: Hex color string (e.g., "#ff7f0e" or "ff7f0e")
        alpha: Alpha transparency value (0.0 to 1.0)

    Returns:
        Tuple of (r, g, b, alpha)
    """
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return (r, g, b, alpha)


def _render_urn_with_link(
    label: str,
    urn: str | None,
    hostname: str | None = None,
    assertee_urn: str | None = None,
    assertion_urn: str | None = None,
    max_display_length: int = 50,
) -> None:
    """Render a URN with optional link and copyable code block.

    Args:
        label: Label to show before the URN (e.g., "URN", "Entity")
        urn: The full URN
        hostname: DataHub endpoint hostname for generating links
        assertee_urn: For assertion URNs, the entity the assertion is for
        assertion_urn: For monitor URNs, the associated assertion to link to
        max_display_length: Max characters to display before truncating (unused, kept for compatibility)
    """
    if not urn:
        st.markdown(f"**{label}:** -")
        return

    # Get URL if available
    url = _get_datahub_url(
        urn, hostname, assertee_urn=assertee_urn, assertion_urn=assertion_urn
    )

    # Show label with optional link
    if url:
        st.markdown(f"**{label}:** [🔗 Open in DataHub]({url})")
    else:
        st.markdown(f"**{label}:**")

    # Always show full URN in copyable code block
    st.code(urn, language=None)


def _get_datahub_url(
    urn: str,
    hostname: str | None = None,
    assertee_urn: str | None = None,
    assertion_urn: str | None = None,
) -> str | None:
    """Generate a DataHub web URL for a given URN.

    Args:
        urn: The URN to link to (e.g., urn:li:dataset:..., urn:li:assertion:...)
        hostname: The DataHub endpoint hostname. If None, uses session state.
        assertee_urn: For assertion URNs, the entity (dataset) the assertion is for.
        assertion_urn: For monitor URNs, the associated assertion to link to.

    Returns:
        The full URL to the entity in DataHub web UI, or None if invalid.

    URL format:
        - Dataset: http://localhost:9002/dataset/{urn}
        - Assertion: http://localhost:9002/dataset/{assertee_urn}/Quality/List?assertion_urn={encoded_urn}
        - Monitor (with assertion): links to the assertion page
        - Monitor (without assertion): links to entity's Quality/Assertions tab
        - localhost uses http://localhost:9002
        - other hosts use https://{hostname}

    Examples:
        - Dataset: http://localhost:9002/dataset/urn:li:dataset:(...)
        - Assertion: http://localhost:9002/dataset/{assertee}/Quality/List?assertion_urn=...
    """
    from urllib.parse import quote

    if not urn:
        return None

    # Get hostname from session state if not provided
    if hostname is None:
        hostname = st.session_state.get("selected_endpoint")

    if not hostname:
        return None

    # Build base URL
    if "localhost" in hostname.lower():
        base_url = "http://localhost:9002"
    else:
        # Strip any protocol/path from hostname
        clean_host = hostname.replace("https://", "").replace("http://", "")
        clean_host = clean_host.split("/")[0]  # Remove any path
        base_url = f"https://{clean_host}"

    # Handle assertion URNs specially - they link to dataset Quality tab
    if urn.startswith("urn:li:assertion:"):
        if not assertee_urn:
            return None
        encoded_assertion = quote(urn, safe="")
        return f"{base_url}/dataset/{assertee_urn}/Quality/List?assertion_urn={encoded_assertion}"

    # Handle monitor URNs - link to assertion if provided, otherwise entity's Quality tab
    if urn.startswith("urn:li:monitor:"):
        # Extract entity URN from monitor URN
        # Format: urn:li:monitor:(urn:li:dataset:(...),monitor_type)
        from .run_event_extractor import extract_entity_from_monitor_urn

        entity_urn = extract_entity_from_monitor_urn(urn)
        if not entity_urn or not entity_urn.startswith("urn:li:dataset:"):
            return None

        # If we have an associated assertion, link directly to it
        if assertion_urn and assertee_urn:
            encoded_assertion = quote(assertion_urn, safe="")
            return f"{base_url}/dataset/{assertee_urn}/Quality/List?assertion_urn={encoded_assertion}"

        # Otherwise link to the entity's Quality/Assertions tab
        return f"{base_url}/dataset/{entity_urn}/Quality/Assertions"

    # Determine entity type from URN
    entity_type = None
    if urn.startswith("urn:li:dataset:"):
        entity_type = "dataset"
    elif urn.startswith("urn:li:dataJob:"):
        entity_type = "tasks"
    elif urn.startswith("urn:li:dataFlow:"):
        entity_type = "pipelines"
    elif urn.startswith("urn:li:chart:"):
        entity_type = "chart"
    elif urn.startswith("urn:li:dashboard:"):
        entity_type = "dashboard"
    elif urn.startswith("urn:li:corpuser:"):
        entity_type = "user"
    elif urn.startswith("urn:li:corpGroup:"):
        entity_type = "group"

    if entity_type is None:
        return None

    return f"{base_url}/{entity_type}/{urn}"


def _make_urn_link(
    urn: str,
    hostname: str | None = None,
    max_length: int = 40,
    show_full_on_hover: bool = True,
    assertee_urn: str | None = None,
    assertion_urn: str | None = None,
) -> str:
    """Create a clickable markdown link for a URN.

    Args:
        urn: The URN to link to
        hostname: The DataHub endpoint hostname
        max_length: Maximum display length for the shortened URN
        show_full_on_hover: Whether to show full URN on hover (title attribute)
        assertee_urn: For assertion URNs, the entity (dataset) the assertion is for.
        assertion_urn: For monitor URNs, the associated assertion to link to.

    Returns:
        Markdown string with clickable link, or shortened URN if no link possible.
    """
    if not urn:
        return ""

    short_urn = _shorten_urn(urn, max_length)
    url = _get_datahub_url(
        urn, hostname, assertee_urn=assertee_urn, assertion_urn=assertion_urn
    )

    if url:
        if show_full_on_hover:
            # Use HTML link with title for hover
            return f'<a href="{url}" target="_blank" title="{urn}">{short_urn}</a>'
        return f"[{short_urn}]({url})"
    return short_urn


def _check_cancelled() -> bool:
    """Check if fetch has been cancelled (thread-safe)."""
    return _cancel_event.is_set()


def _set_cancelled(value: bool) -> None:
    """Set the fetch cancelled flag (thread-safe)."""
    if value:
        _cancel_event.set()
    else:
        _cancel_event.clear()


# =============================================================================
# Model Hyperparameter Extraction
# =============================================================================


def get_model_hyperparameters(model: object) -> dict[str, object]:
    """Extract hyperparameters from a trained model (observe-models).

    Uses get_hyperparameters() from observe-models which returns only actual
    model hyperparameters, not metadata or training statistics.

    Args:
        model: The trained model object (forecast or anomaly model)

    Returns:
        Dict of hyperparameters, or dict with "note" key if unavailable
    """
    if model is None:
        return {"note": "Model not available"}

    # Primary: Use get_hyperparameters() from observe-models
    # This is the preferred method - returns clean hyperparameters
    if hasattr(model, "get_hyperparameters"):
        try:
            hyperparams = model.get_hyperparameters()
            if hyperparams:
                return dict(hyperparams)
        except Exception:
            pass

    # Fallback: Check best_params from hyperparameter tuning (grid search)
    if hasattr(model, "best_params") and model.best_params:
        return dict(model.best_params)

    # Last resort: get_config_dict for legacy models
    if hasattr(model, "get_config_dict"):
        try:
            config = model.get_config_dict(include_training_params=False)
            if config:
                return dict(config)
        except Exception:
            pass

    return {"note": "No hyperparameters available"}


def init_explorer_state():
    """Initialize explorer state in session state.

    Uses the default endpoint from the registry if no endpoint is selected.
    """
    if _SELECTED_ENDPOINT not in st.session_state:
        st.session_state[_SELECTED_ENDPOINT] = None
    if _SELECTED_ASSERTION not in st.session_state:
        st.session_state[_SELECTED_ASSERTION] = None
    if _LOADED_TIMESERIES not in st.session_state:
        st.session_state[_LOADED_TIMESERIES] = None
    if _DATA_SOURCE not in st.session_state:
        st.session_state[_DATA_SOURCE] = "cache"

    # If no endpoint selected, try to use the default endpoint
    if st.session_state[_SELECTED_ENDPOINT] is None:
        loader = DataLoader()
        default_endpoint = loader.registry.get_default_endpoint()
        if default_endpoint:
            # Verify the endpoint still exists
            endpoints = loader.list_endpoints()
            if any(ep.hostname == default_endpoint for ep in endpoints):
                st.session_state[_SELECTED_ENDPOINT] = default_endpoint


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Logger
    "logger",
    # Session state keys
    "_SELECTED_ENDPOINT",
    "_SELECTED_ASSERTION",
    "_LOADED_TIMESERIES",
    "_DATA_SOURCE",
    "_FETCH_CANCELLED",
    "_FETCH_IN_PROGRESS",
    "_MONITOR_URN_FOR_ASSERTION",
    "_RAW_EVENTS_DF",
    "_SELECTED_MONITOR",
    # Filter options (assertion type / field metric)
    "ASSERTION_TYPE_FILTER_OPTIONS",
    "FIELD_METRIC_FILTER_OPTIONS",
    "set_explorer_context",
    # Cancel event
    "_cancel_event",
    # Config
    "FetchConfig",
    # Helper functions
    "_shorten_urn",
    "_hex_to_rgba",
    "_render_urn_with_link",
    "_get_datahub_url",
    "_make_urn_link",
    "_check_cancelled",
    "_set_cancelled",
    "get_model_hyperparameters",
    "init_explorer_state",
    # Re-exported from shared_config (external script)
    "ACTIVE_ENV_CONFIG",
    "get_active_config",
    "render_connection_status",
]
