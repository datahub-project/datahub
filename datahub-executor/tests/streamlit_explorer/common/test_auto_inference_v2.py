from __future__ import annotations

import pytest


def test_suggest_preprocessing_preset_cumulative_volume_maps_to_volume_pipeline():
    defaults_mod = pytest.importorskip(
        "datahub_executor.common.monitor.inference_v2.observe_adapter.defaults"
    )
    auto_mod = pytest.importorskip(
        "scripts.streamlit_explorer.common.auto_inference_v2"
    )

    InputDataContext = defaults_mod.InputDataContext

    ctx = InputDataContext(
        assertion_category="volume",
        is_dataframe_cumulative=True,
        is_delta=False,
    )
    preset = auto_mod.suggest_preprocessing_preset(ctx)

    assert preset.pipeline_name == "volume"
    assert preset.config_overrides.get("convert_cumulative") is True


def test_suggest_preprocessing_preset_non_cumulative_volume_preserves_frequency_and_is_delta():
    defaults_mod = pytest.importorskip(
        "datahub_executor.common.monitor.inference_v2.observe_adapter.defaults"
    )
    auto_mod = pytest.importorskip(
        "scripts.streamlit_explorer.common.auto_inference_v2"
    )

    InputDataContext = defaults_mod.InputDataContext

    ctx = InputDataContext(
        assertion_category="volume",
        is_dataframe_cumulative=False,
        is_delta=None,
    )
    preset = auto_mod.suggest_preprocessing_preset(ctx)

    assert preset.pipeline_name == "volume"
    # Inference_v2 defaults: volume is not delta by default.
    assert preset.config_overrides.get("is_delta") is False


def test_build_input_data_context_from_session_uses_volume_assertion_type_total() -> (
    None
):
    defaults_mod = pytest.importorskip(
        "datahub_executor.common.monitor.inference_v2.observe_adapter.defaults"
    )
    auto_mod = pytest.importorskip(
        "scripts.streamlit_explorer.common.auto_inference_v2"
    )
    st = pytest.importorskip("streamlit")

    InputDataContext = defaults_mod.InputDataContext

    st.session_state.clear()
    st.session_state["current_assertion_type"] = "volume"
    st.session_state["current_volume_assertion_type"] = "ROW_COUNT_TOTAL"

    ctx = auto_mod.build_input_data_context_from_session()
    assert isinstance(ctx, InputDataContext)
    assert ctx.assertion_category == "volume"
    assert ctx.is_dataframe_cumulative is True
    assert ctx.is_delta is False


def test_build_input_data_context_from_session_uses_volume_assertion_type_change() -> (
    None
):
    defaults_mod = pytest.importorskip(
        "datahub_executor.common.monitor.inference_v2.observe_adapter.defaults"
    )
    auto_mod = pytest.importorskip(
        "scripts.streamlit_explorer.common.auto_inference_v2"
    )
    st = pytest.importorskip("streamlit")

    InputDataContext = defaults_mod.InputDataContext

    st.session_state.clear()
    st.session_state["current_assertion_type"] = "volume"
    st.session_state["current_volume_assertion_type"] = "ROW_COUNT_CHANGE"

    ctx = auto_mod.build_input_data_context_from_session()
    assert isinstance(ctx, InputDataContext)
    assert ctx.assertion_category == "volume"
    assert ctx.is_dataframe_cumulative is False
    assert ctx.is_delta is True


def test_parse_pairings_csv_round_trips_names():
    auto_mod = pytest.importorskip(
        "scripts.streamlit_explorer.common.auto_inference_v2"
    )

    pairings = auto_mod.parse_pairings_csv(
        "prophet_adaptive_band,datahub_datahub_forecast_anomaly"
    )
    assert [p.name for p in pairings] == [
        "prophet_adaptive_band",
        "datahub_datahub_forecast_anomaly",
    ]
