# ruff: noqa: INP001
"""
Centralized management for auto inference_v2 runs across Streamlit pages.

This module provides a unified API for managing auto inference_v2 runs,
ensuring consistent behavior across Model Training, Time Series Comparison,
and Anomaly Comparison pages.

Key principles:
- Cache is authoritative source of truth
- Auto runs stored separately from training runs
- Complete cleanup of all related data (main run, eval run, session state)
"""

from __future__ import annotations

from typing import Any, MutableMapping, Optional

from .data_loaders import DataLoader

# Session state key for auto inference_v2 runs (separate from training runs)
_AUTO_INFERENCE_V2_RUNS_KEY = "auto_inference_v2_runs"

# UI detail state key prefix (for backward compatibility during transition)
_AUTO_V2_RUN_DETAILS_KEY_PREFIX = "model_training__auto_v2_details__"


def get_auto_inference_v2_runs(
    session_state: MutableMapping[str, Any],
) -> dict[str, dict[str, Any]]:
    """Get auto inference_v2 runs from session state.

    Returns:
        dict of run_id -> run_data (raw dict from cache)
    """
    runs = session_state.get(_AUTO_INFERENCE_V2_RUNS_KEY)
    if not isinstance(runs, dict):
        return {}
    return runs


def load_auto_inference_v2_runs(
    hostname: str,
    assertion_urn: Optional[str],
    session_state: MutableMapping[str, Any],
) -> dict[str, dict[str, Any]]:
    """Load auto inference_v2 runs from cache into session state.

    Only loads runs that actually exist in cache (verifies directory exists).
    Stores in _AUTO_INFERENCE_V2_RUNS_KEY session state key.

    Args:
        hostname: DataHub endpoint hostname
        assertion_urn: Optional assertion URN to filter runs
        session_state: Streamlit session state

    Returns:
        dict of run_id -> run_data (raw dict from cache)
    """
    loader = DataLoader()
    endpoint_cache = loader.cache.get_endpoint_cache(hostname)

    # Get list of runs from cache (filtered by assertion when provided)
    try:
        auto_runs_meta = endpoint_cache.list_saved_auto_inference_v2_runs(
            assertion_urn=assertion_urn
        )
    except Exception:
        auto_runs_meta = []

    # Replace session state with only runs for this assertion so we don't show
    # runs from a previously selected assertion when switching assertions.
    runs: dict[str, dict[str, Any]] = {}
    session_state[_AUTO_INFERENCE_V2_RUNS_KEY] = runs

    # Clear legacy detail state for previous assertion's runs
    for key in list(session_state.keys()):
        if key.startswith(_AUTO_V2_RUN_DETAILS_KEY_PREFIX):
            session_state.pop(key, None)

    # Load each run, verifying it exists in cache
    for auto_meta in auto_runs_meta or []:
        auto_run_id = auto_meta.get("run_id")
        if not auto_run_id or not isinstance(auto_run_id, str):
            continue

        # Try to load the run data - if it fails, the run was deleted from cache
        auto_run_data = endpoint_cache.load_auto_inference_v2_run(auto_run_id)
        if not isinstance(auto_run_data, dict):
            # Run was deleted from cache, skip it
            continue

        # Store in session state
        runs[auto_run_id] = auto_run_data

        # Also store in detail state for backward compatibility
        session_state[f"{_AUTO_V2_RUN_DETAILS_KEY_PREFIX}{auto_run_id}"] = auto_run_data

    return runs


def delete_auto_inference_v2_run_complete(
    run_id: str,
    hostname: str,
    session_state: MutableMapping[str, Any],
) -> bool:
    """Delete auto inference_v2 run and ALL related data.

    Deletes:
    - Main run from auto_inference_v2_runs_dir
    - Eval run from training_runs_dir (auto_v2_eval__{run_id})
    - From session state (_AUTO_INFERENCE_V2_RUNS_KEY)
    - From training runs session state (if it was added there for compatibility)
    - UI detail state (_AUTO_V2_RUN_DETAILS_KEY_PREFIX)

    Args:
        run_id: The auto inference_v2 run ID to delete
        hostname: DataHub endpoint hostname
        session_state: Streamlit session state

    Returns:
        True if all deletions succeeded, False otherwise
    """
    eval_run_id = f"auto_v2_eval__{run_id}"

    # Delete from cache FIRST (before removing from session state)
    loader = DataLoader()
    endpoint_cache = loader.cache.get_endpoint_cache(hostname)

    # Delete main run from cache
    deleted_main = endpoint_cache.delete_auto_inference_v2_run(run_id)

    # Verify the run directory is actually gone
    run_dir = endpoint_cache.auto_inference_v2_runs_dir / run_id
    if run_dir.exists():
        # Delete failed - try again with force (ignore_errors=True never raises)
        import shutil

        shutil.rmtree(run_dir, ignore_errors=True)
        deleted_main = not run_dir.exists()

    # Delete eval run from cache (best effort - might not exist)
    try:
        deleted_eval = endpoint_cache.delete_training_run(eval_run_id)
        # Eval run is optional: "not found" is success (nothing to delete)
        if not deleted_eval:
            deleted_eval = True
    except Exception:
        deleted_eval = False

    # Remove from auto runs session state
    auto_runs = session_state.get(_AUTO_INFERENCE_V2_RUNS_KEY)
    if isinstance(auto_runs, dict):
        auto_runs.pop(run_id, None)

    # Remove from training runs session state (for backward compatibility)
    # Check if there's a _TRAINING_RUNS_KEY that might contain this run
    training_runs_key = "training_runs"
    training_runs = session_state.get(training_runs_key)
    if isinstance(training_runs, dict):
        training_runs.pop(run_id, None)
        training_runs.pop(eval_run_id, None)

    # Clear UI detail state
    session_state.pop(f"{_AUTO_V2_RUN_DETAILS_KEY_PREFIX}{run_id}", None)

    return deleted_main and deleted_eval


def clear_all_auto_inference_v2_runs(
    hostname: str,
    assertion_urn: Optional[str],
    session_state: MutableMapping[str, Any],
) -> int:
    """Clear all auto inference_v2 runs for assertion.

    Uses delete_auto_inference_v2_run_complete for each run.

    Args:
        hostname: DataHub endpoint hostname
        assertion_urn: Optional assertion URN to filter runs
        session_state: Streamlit session state

    Returns:
        Count of runs deleted
    """
    loader = DataLoader()
    endpoint_cache = loader.cache.get_endpoint_cache(hostname)

    # Get list of runs to delete
    try:
        auto_runs_meta = endpoint_cache.list_saved_auto_inference_v2_runs(
            assertion_urn=assertion_urn
        )
    except Exception:
        auto_runs_meta = []

    deleted_count = 0
    for meta in auto_runs_meta or []:
        run_id = meta.get("run_id") if isinstance(meta, dict) else None
        if not isinstance(run_id, str) or not run_id:
            continue

        if delete_auto_inference_v2_run_complete(
            run_id=run_id,
            hostname=hostname,
            session_state=session_state,
        ):
            deleted_count += 1

    return deleted_count


__all__ = [
    "_AUTO_INFERENCE_V2_RUNS_KEY",
    "_AUTO_V2_RUN_DETAILS_KEY_PREFIX",
    "get_auto_inference_v2_runs",
    "load_auto_inference_v2_runs",
    "delete_auto_inference_v2_run_complete",
    "clear_all_auto_inference_v2_runs",
]
