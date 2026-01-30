"""
Streamlit helpers to mirror inference_v2 "auto" behavior.

This module is intentionally thin: it reuses inference_v2's public interfaces
(`ObserveDefaultsBuilder`, `ModelFactory`, `ObserveAdapter`) and only adds UI-friendly
wrappers for:
- resolving preprocessing presets to a concrete Streamlit preprocessing pipeline
- parsing/overriding model pairings for combination evaluation
- running the full inference_v2 training pipeline from Streamlit
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Sequence, Union

if TYPE_CHECKING:
    from datahub_executor.common.monitor.inference_v2.types import ProgressHook

import pandas as pd
import streamlit as st


@dataclass(frozen=True)
class AutoPreprocessingPreset:
    pipeline_name: str
    config_overrides: dict[str, object]
    explanation: str
    # When set, UI uses this exact PreprocessingConfig (e.g. volume with frequency="auto")
    # instead of building from registry overrides, so display and apply match executor.
    resolved_config: Optional[Any] = None


@dataclass(frozen=True)
class AutoTrainingRunResult:
    training_result: Any
    pairings_used: list[str]


def _get_assertion_category_from_session() -> str:
    """
    Best-effort assertion category inference from Streamlit session state.

    The inference_v2 pipeline uses lower-case category strings like:
    "volume", "freshness", "rate", "statistic", "length".
    """
    raw = st.session_state.get("current_assertion_type")
    if isinstance(raw, str) and raw.strip():
        return raw.strip().lower()

    # In some codepaths this may be an AssertionCategory enum from observe-models.
    try:
        from datahub_observe.assertions.types import (
            AssertionCategory,  # type: ignore[import-untyped]
        )

        if isinstance(raw, AssertionCategory):
            return str(raw.value).lower()
    except Exception:
        pass

    return "volume"


def build_input_data_context_from_session(
    *,
    sensitivity_level: Optional[int] = None,
    metric_type: Optional[str] = None,
    is_delta: Optional[bool] = None,
) -> Any:
    """
    Build inference_v2 InputDataContext from Streamlit session state.
    """
    from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
        AdjustmentSettings,
        InputDataContext,
    )

    assertion_category = _get_assertion_category_from_session()
    # Prefer enriched assertion subtype (when available) and delegate mapping
    # to inference_v2 helpers (avoid duplicating logic in Streamlit).
    inferred_is_dataframe_cumulative = bool(
        st.session_state.get("is_dataframe_cumulative", False)
    )
    inferred_is_delta: Optional[bool] = None
    if assertion_category == "volume":
        raw_volume_type = st.session_state.get("current_volume_assertion_type")
        if raw_volume_type is not None:
            from datahub_executor.common.monitor.inference_v2.volume_semantics import (
                resolve_volume_series_semantics,
            )

            inferred_is_dataframe_cumulative, inferred_is_delta = (
                resolve_volume_series_semantics(raw_volume_type)
            )

    adjustment_settings = None
    if sensitivity_level is not None:
        adjustment_settings = AdjustmentSettings(
            sensitivity_level=int(sensitivity_level)
        )

    return InputDataContext(
        assertion_category=assertion_category,
        is_dataframe_cumulative=inferred_is_dataframe_cumulative,
        is_delta=is_delta if is_delta is not None else inferred_is_delta,
        adjustment_settings=adjustment_settings,
        metric_type=metric_type,
    )


def parse_pairings_csv(raw: str) -> list[Any]:
    """
    Parse a comma-separated list of inference_v2 pairing identifiers.

    Accepted tokens match inference_v2 `parse_pairing_identifier`, e.g.:
    - prophet_adaptive_band
    - prophet@0.1.0_adaptive_band@0.2.0
    """
    from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
        ModelPairing,
        parse_pairing_identifier,
    )

    tokens = [t.strip() for t in (raw or "").split(",") if t.strip()]
    pairings: list[ModelPairing] = []
    for token in tokens:
        pairings.append(parse_pairing_identifier(token))
    return pairings


def get_default_pairing_identifiers() -> list[str]:
    from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
        get_default_pairings,
    )

    return [p.name for p in get_default_pairings()]


def get_all_pairing_identifiers() -> list[str]:
    from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
        get_all_pairings,
    )

    return [p.name for p in get_all_pairings()]


def suggest_preprocessing_preset(context: Any) -> AutoPreprocessingPreset:
    """
    Resolve inference_v2 defaults into a concrete Streamlit preprocessing pipeline choice.

    The output is used by Streamlit's preprocessing UI to materialize a preprocessed
    DataFrame that corresponds to the "auto" choices inference_v2 would make.
    """
    from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
        get_defaults_for_context,
    )

    # Compute defaults and extract all values that should be used by Streamlit.
    # This ensures Streamlit uses the same logic as the core inference_v2 pipeline.
    defaults = get_defaults_for_context(context)
    raw_cfg = defaults.preprocessing_config()
    from datahub_observe.algorithms.preprocessing.preprocessor import (
        PreprocessingConfig,
    )
    from datahub_observe.assertions.config import AssertionPreprocessingConfig

    preprocessing_cfg: PreprocessingConfig = (
        raw_cfg.to_preprocessing_config()
        if isinstance(raw_cfg, AssertionPreprocessingConfig)
        else raw_cfg
    )

    category = str(getattr(context, "assertion_category", "volume")).lower()

    # Volume: extract all semantic flags from computed defaults.
    if category == "volume":
        from datahub_observe.algorithms.preprocessing.transformers import (
            DifferenceConfig,
            ValueFilterConfig,
        )

        # Extract convert_cumulative: check if differencing is enabled in computed config
        convert_cumulative = False
        for transformer in preprocessing_cfg.darts_transformers:
            if isinstance(transformer, DifferenceConfig) and transformer.enabled:
                convert_cumulative = True
                break

        # Extract is_delta: use same logic as defaults (explicit override or default False)
        raw_is_delta = getattr(context, "is_delta", None)
        is_delta = bool(raw_is_delta) if raw_is_delta is not None else False
        # If cumulative, defaults force is_delta=False
        if getattr(context, "is_dataframe_cumulative", False):
            is_delta = False

        # Extract strict_validation from defaults: find ValueFilterConfig with min_value=0
        strict_validation = False  # Default fallback
        for pandas_transformer in preprocessing_cfg.pandas_transformers:
            if (
                isinstance(pandas_transformer, ValueFilterConfig)
                and pandas_transformer.min_value == 0
            ):
                strict_validation = bool(pandas_transformer.strict)
                break

        # Extract all semantic flags from computed defaults to ensure Streamlit
        # uses the same logic as the core inference_v2 pipeline.
        overrides: dict[str, object] = {
            "convert_cumulative": convert_cumulative,
            "is_delta": is_delta,
            "strict_validation": strict_validation,
        }

        return AutoPreprocessingPreset(
            pipeline_name="volume",
            config_overrides=overrides,
            explanation=(
                "Resolved to the volume pipeline using inference_v2 semantics "
                f"(input: cumulative={convert_cumulative}, is_delta={is_delta}). "
                "After preprocessing, differencing produces a delta series used for resampling/oversampling."
            ),
            resolved_config=preprocessing_cfg,
        )

    # Otherwise, map primarily by category. We keep overrides minimal and let the
    # pipeline defaults do the heavy lifting, but preserve key inference_v2 fields
    # when they exist (frequency/is_delta).
    pipeline_name = category if category in {"volume", "field"} else "custom"

    overrides = {}
    # Keep these from context when present.
    for key in ("is_delta", "metric_type"):
        if hasattr(context, key):
            value = getattr(context, key)
            if value is not None:
                overrides[key] = value
    # For field pipelines, include metric_type from context when available.
    if pipeline_name == "field" and getattr(context, "metric_type", None) is not None:
        overrides.setdefault("metric_type", context.metric_type)

    return AutoPreprocessingPreset(
        pipeline_name=pipeline_name,
        config_overrides=overrides,
        explanation=(
            f"Resolved based on inference_v2 defaults for category '{category}' "
            f"(pipeline='{pipeline_name}')."
        ),
    )


def run_auto_training_pipeline(
    *,
    df: pd.DataFrame,
    context: Any,
    num_intervals: int,
    interval_hours: int,
    sensitivity_level: int,
    existing_model_config: Optional[Any] = None,
    ground_truth: Optional[pd.DataFrame] = None,
    model_pairings: Optional[list[Any]] = None,
    eval_train_ratio: Optional[float] = None,
    progress_hooks: Optional[Union["ProgressHook", Sequence["ProgressHook"]]] = None,
) -> AutoTrainingRunResult:
    """
    Execute the full inference_v2 ObserveAdapter training pipeline.

    If model_pairings is None, inference_v2 will use env override/defaults.
    """
    from datahub_executor.common.monitor.inference_v2.observe_adapter import (
        ObserveAdapter,
    )
    from datahub_executor.common.monitor.inference_v2.observe_adapter.evaluator.combinations import (
        get_pairings_from_env_or_default,
    )

    pairings_used: list[str] = []
    if model_pairings is None:
        # Record what inference_v2 will use by default (env override or defaults).
        try:
            pairings_used = [p.name for p in get_pairings_from_env_or_default()]
        except Exception:
            pairings_used = []
    else:
        pairings_used = [p.name for p in model_pairings]

    adapter = ObserveAdapter()
    training_result = adapter.run_training_pipeline(
        df=df,
        input_data_context=context,
        num_intervals=num_intervals,
        interval_hours=interval_hours,
        sensitivity_level=int(sensitivity_level),
        ground_truth=ground_truth,
        existing_model_config=existing_model_config,
        model_combinations=model_pairings,
        eval_train_ratio=eval_train_ratio,
        progress_hooks=progress_hooks,
    )
    return AutoTrainingRunResult(
        training_result=training_result, pairings_used=pairings_used
    )
