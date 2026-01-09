# ruff: noqa: INP001
"""
Time Series Explorer module.

Provides Streamlit pages for exploring, visualizing, and browsing
time series data from assertion run events.
"""

import streamlit as st

# Import page rendering functions
from .assertion_browser import render_assertion_browser_page
from .data_source import render_data_source_page
from .monitor_browser import (
    _create_anomaly_event_rest,
    _delete_anomaly_event,
    _publish_single_anomaly,
    render_monitor_browser_page,
)

# =============================================================================
# Page Registration
# =============================================================================

data_source_page = st.Page(
    render_data_source_page,
    title="Data Source",
    icon="📥",
    url_path="explorer_source",
)

metric_cube_browser_page = st.Page(
    render_assertion_browser_page,
    title="Metric Cube Events",
    icon="📈",
    url_path="explorer_metrics",
)

monitor_browser_page = st.Page(
    render_monitor_browser_page,
    title="Monitor Browser",
    icon="📊",
    url_path="explorer_monitors",
)

# All timeseries explorer pages
TIMESERIES_EXPLORER_PAGES = [
    data_source_page,
    metric_cube_browser_page,
    monitor_browser_page,
]

__all__ = [
    # Page functions
    "render_data_source_page",
    "render_assertion_browser_page",
    "render_monitor_browser_page",
    # Page objects
    "data_source_page",
    "metric_cube_browser_page",
    "monitor_browser_page",
    # Monitor browser utilities (for tests)
    "_delete_anomaly_event",
    "_publish_single_anomaly",
    "_create_anomaly_event_rest",
    # Page list
    "TIMESERIES_EXPLORER_PAGES",
]
