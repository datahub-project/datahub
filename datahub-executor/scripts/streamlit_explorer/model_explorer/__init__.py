# ruff: noqa: INP001
"""
Model Explorer module.

Provides Streamlit pages for model training, comparison,
and anomaly detection analysis.
"""

import streamlit as st

# Import page rendering functions
from .anomaly_comparison import render_anomaly_comparison_page
from .model_training import render_model_training_page
from .preprocessing import render_preprocessing_page
from .timeseries_comparison import render_timeseries_comparison_page

# =============================================================================
# Page Registration
# =============================================================================

preprocessing_page = st.Page(
    render_preprocessing_page,
    title="Preprocessing",
    icon="⚙️",
    url_path="explorer_preprocessing",
)

model_training_page = st.Page(
    render_model_training_page,
    title="Model Training",
    icon="🏋️",
    url_path="explorer_training",
)

timeseries_comparison_page = st.Page(
    render_timeseries_comparison_page,
    title="Time Series Comparison",
    icon="📊",
    url_path="explorer_ts_comparison",
)

anomaly_comparison_page = st.Page(
    render_anomaly_comparison_page,
    title="Anomaly Comparison",
    icon="🔴",
    url_path="explorer_anomaly_comparison",
)

# All model explorer pages
MODEL_EXPLORER_PAGES = [
    preprocessing_page,
    model_training_page,
    timeseries_comparison_page,
    anomaly_comparison_page,
]

__all__ = [
    # Page functions
    "render_preprocessing_page",
    "render_model_training_page",
    "render_timeseries_comparison_page",
    "render_anomaly_comparison_page",
    # Page objects
    "preprocessing_page",
    "model_training_page",
    "timeseries_comparison_page",
    "anomaly_comparison_page",
    # Page list
    "MODEL_EXPLORER_PAGES",
]
