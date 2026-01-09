# ruff: noqa: INP001
"""
Streamlit application modules.

This package contains the Streamlit-based tools for the DataHub Executor:
- common: Shared utilities, cache management, and data loading
- timeseries_explorer: Time series browsing and visualization
- model_explorer: Model training, comparison, and anomaly detection
- documentation: Data model and workflow documentation
"""

import streamlit as st

# Import page lists from submodules
from .documentation import render_documentation_page
from .model_explorer import MODEL_EXPLORER_PAGES
from .timeseries_explorer import TIMESERIES_EXPLORER_PAGES

# Documentation page
documentation_page = st.Page(
    render_documentation_page,
    title="Documentation",
    icon="📖",
    url_path="documentation",
)

HELP_PAGES = [documentation_page]

# Combined page export for the main app
EXPLORER_PAGES = {
    "Time Series Explorer": TIMESERIES_EXPLORER_PAGES,
    "Model Explorer": MODEL_EXPLORER_PAGES,
    "Help": HELP_PAGES,
}

__all__ = [
    "EXPLORER_PAGES",
    "TIMESERIES_EXPLORER_PAGES",
    "MODEL_EXPLORER_PAGES",
    "HELP_PAGES",
    "documentation_page",
    "render_documentation_page",
]
