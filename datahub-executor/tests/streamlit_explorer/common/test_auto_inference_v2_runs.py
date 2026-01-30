"""Tests for the auto_inference_v2_runs module."""

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd

from scripts.streamlit_explorer.common.auto_inference_v2_runs import (
    _AUTO_INFERENCE_V2_RUNS_KEY,
    _AUTO_V2_RUN_DETAILS_KEY_PREFIX,
    clear_all_auto_inference_v2_runs,
    delete_auto_inference_v2_run_complete,
    get_auto_inference_v2_runs,
    load_auto_inference_v2_runs,
)
from scripts.streamlit_explorer.common.cache_manager import EndpointCache


class TestGetAutoInferenceV2Runs:
    """Tests for get_auto_inference_v2_runs function."""

    def test_returns_empty_dict_when_key_not_present(self):
        session_state: dict[str, Any] = {}
        runs = get_auto_inference_v2_runs(session_state)
        assert runs == {}

    def test_returns_empty_dict_when_key_is_not_dict(self):
        session_state: dict[str, Any] = {_AUTO_INFERENCE_V2_RUNS_KEY: "not a dict"}
        runs = get_auto_inference_v2_runs(session_state)
        assert runs == {}

    def test_returns_runs_from_session_state(self):
        run_data = {"run_id": "test_run", "metadata": {"preprocessing_id": "daily"}}
        session_state: dict[str, Any] = {
            _AUTO_INFERENCE_V2_RUNS_KEY: {"test_run": run_data}
        }
        runs = get_auto_inference_v2_runs(session_state)
        assert runs == {"test_run": run_data}


class TestLoadAutoInferenceV2Runs:
    """Tests for load_auto_inference_v2_runs function."""

    def test_loads_runs_from_cache_into_session_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save a test run
            train_df = pd.DataFrame(
                {"ds": pd.date_range("2024-01-01", periods=10), "y": range(10)}
            )
            pred_df = pd.DataFrame({"timestamp_ms": [1000, 2000], "yhat": [5.0, 6.0]})
            cache.save_auto_inference_v2_run(
                run_id="test_run_1",
                assertion_urn="urn:li:assertion:test",
                preprocessing_id="daily",
                train_df=train_df,
                prediction_df=pred_df,
                model_config_dict={"forecast_score": 0.5},
                pairings_used=[],
                sensitivity_level=None,
                interval_hours=24,
                num_intervals=7,
            )

            session_state: dict[str, Any] = {}
            loader_mock = MagicMock()
            loader_mock.cache.get_endpoint_cache.return_value = cache
            with patch(
                "scripts.streamlit_explorer.common.auto_inference_v2_runs.DataLoader",
                return_value=loader_mock,
            ):
                load_auto_inference_v2_runs(
                    hostname="test.example.com",
                    assertion_urn="urn:li:assertion:test",
                    session_state=session_state,
                )

            runs = get_auto_inference_v2_runs(session_state)
            assert "test_run_1" in runs
            assert isinstance(runs["test_run_1"], dict)
            assert runs["test_run_1"]["metadata"]["run_id"] == "test_run_1"

    def test_skips_runs_not_in_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Don't save any runs, but list_saved will return metadata for non-existent run
            session_state: dict[str, Any] = {}

            # Mock list_saved to return a run that doesn't exist in cache
            with patch.object(
                cache,
                "list_saved_auto_inference_v2_runs",
                return_value=[{"run_id": "nonexistent_run"}],
            ):
                with patch.object(
                    cache,
                    "load_auto_inference_v2_run",
                    return_value=None,  # Run doesn't exist
                ):
                    loader_mock = MagicMock()
                    loader_mock.cache.get_endpoint_cache.return_value = cache

                    with patch(
                        "scripts.streamlit_explorer.common.auto_inference_v2_runs.DataLoader",
                        return_value=loader_mock,
                    ):
                        load_auto_inference_v2_runs(
                            hostname="test.example.com",
                            assertion_urn="urn:li:assertion:test",
                            session_state=session_state,
                        )

            runs = get_auto_inference_v2_runs(session_state)
            # Should not have loaded the nonexistent run
            assert "nonexistent_run" not in runs

    def test_stores_detail_state_for_backward_compatibility(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            train_df = pd.DataFrame(
                {"ds": pd.date_range("2024-01-01", periods=10), "y": range(10)}
            )
            pred_df = pd.DataFrame({"timestamp_ms": [1000], "yhat": [5.0]})
            cache.save_auto_inference_v2_run(
                run_id="test_run_2",
                assertion_urn="urn:li:assertion:test",
                preprocessing_id="daily",
                train_df=train_df,
                prediction_df=pred_df,
                model_config_dict={},
                pairings_used=[],
                sensitivity_level=None,
                interval_hours=24,
                num_intervals=7,
            )

            session_state: dict[str, Any] = {}
            loader_mock = MagicMock()
            loader_mock.cache.get_endpoint_cache.return_value = cache
            with patch(
                "scripts.streamlit_explorer.common.auto_inference_v2_runs.DataLoader",
                return_value=loader_mock,
            ):
                load_auto_inference_v2_runs(
                    hostname="test.example.com",
                    assertion_urn="urn:li:assertion:test",
                    session_state=session_state,
                )

            # Should also store in detail state key
            detail_key = f"{_AUTO_V2_RUN_DETAILS_KEY_PREFIX}test_run_2"
            assert detail_key in session_state
            assert isinstance(session_state[detail_key], dict)


class TestDeleteAutoInferenceV2RunComplete:
    """Tests for delete_auto_inference_v2_run_complete function."""

    def test_deletes_from_cache_and_session_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save a test run
            train_df = pd.DataFrame(
                {"ds": pd.date_range("2024-01-01", periods=10), "y": range(10)}
            )
            pred_df = pd.DataFrame({"timestamp_ms": [1000], "yhat": [5.0]})
            cache.save_auto_inference_v2_run(
                run_id="test_delete",
                assertion_urn="urn:li:assertion:test",
                preprocessing_id="daily",
                train_df=train_df,
                prediction_df=pred_df,
                model_config_dict={},
                pairings_used=[],
                sensitivity_level=None,
                interval_hours=24,
                num_intervals=7,
            )

            # Also save eval run
            eval_train_df = train_df.head(8)
            eval_test_df = train_df.tail(2)
            eval_forecast_df = pd.DataFrame(
                {"ds": eval_test_df["ds"], "yhat": [5.0, 6.0]}
            )
            cache.save_training_run(
                run_id="auto_v2_eval__test_delete",
                model_key="auto_inference_v2",
                model_name="Auto (inference_v2)",
                preprocessing_id="daily",
                train_df=eval_train_df,
                test_df=eval_test_df,
                forecast=eval_forecast_df,
                metrics={},
                color="",
                dash=None,
            )

            session_state: dict[str, Any] = {
                _AUTO_INFERENCE_V2_RUNS_KEY: {
                    "test_delete": {"metadata": {"run_id": "test_delete"}},
                },
                "training_runs": {
                    "test_delete": MagicMock(),  # Old format, should be cleaned up
                    "auto_v2_eval__test_delete": MagicMock(),
                },
                f"{_AUTO_V2_RUN_DETAILS_KEY_PREFIX}test_delete": {"metadata": {}},
            }

            loader_mock = MagicMock()
            loader_mock.cache.get_endpoint_cache.return_value = cache
            with patch(
                "scripts.streamlit_explorer.common.auto_inference_v2_runs.DataLoader",
                return_value=loader_mock,
            ):
                result = delete_auto_inference_v2_run_complete(
                    run_id="test_delete",
                    hostname="test.example.com",
                    session_state=session_state,
                )

            assert result is True
            auto_runs = session_state.get(_AUTO_INFERENCE_V2_RUNS_KEY) or {}
            assert isinstance(auto_runs, dict)
            assert "test_delete" not in auto_runs
            training_runs = session_state.get("training_runs") or {}
            assert isinstance(training_runs, dict)
            assert "test_delete" not in training_runs
            assert "auto_v2_eval__test_delete" not in training_runs
            assert f"{_AUTO_V2_RUN_DETAILS_KEY_PREFIX}test_delete" not in session_state

            # Verify cache deletion
            assert not (cache.auto_inference_v2_runs_dir / "test_delete").exists()
            assert not cache.training_run_exists("auto_v2_eval__test_delete")

    def test_handles_missing_eval_run_gracefully(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save only main run, no eval run
            train_df = pd.DataFrame(
                {"ds": pd.date_range("2024-01-01", periods=10), "y": range(10)}
            )
            pred_df = pd.DataFrame({"timestamp_ms": [1000], "yhat": [5.0]})
            cache.save_auto_inference_v2_run(
                run_id="test_no_eval",
                assertion_urn="urn:li:assertion:test",
                preprocessing_id="daily",
                train_df=train_df,
                prediction_df=pred_df,
                model_config_dict={},
                pairings_used=[],
                sensitivity_level=None,
                interval_hours=24,
                num_intervals=7,
            )

            session_state: dict[str, Any] = {
                _AUTO_INFERENCE_V2_RUNS_KEY: {
                    "test_no_eval": {"metadata": {"run_id": "test_no_eval"}},
                },
            }

            loader_mock = MagicMock()
            loader_mock.cache.get_endpoint_cache.return_value = cache
            with patch(
                "scripts.streamlit_explorer.common.auto_inference_v2_runs.DataLoader",
                return_value=loader_mock,
            ):
                result = delete_auto_inference_v2_run_complete(
                    run_id="test_no_eval",
                    hostname="test.example.com",
                    session_state=session_state,
                )

            # Should still succeed even without eval run
            assert result is True
            assert "test_no_eval" not in session_state.get(
                _AUTO_INFERENCE_V2_RUNS_KEY, {}
            )


class TestClearAllAutoInferenceV2Runs:
    """Tests for clear_all_auto_inference_v2_runs function."""

    def test_clears_all_runs_for_assertion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            # Save multiple runs
            train_df = pd.DataFrame(
                {"ds": pd.date_range("2024-01-01", periods=10), "y": range(10)}
            )
            pred_df = pd.DataFrame({"timestamp_ms": [1000], "yhat": [5.0]})

            for run_id in ["run1", "run2"]:
                cache.save_auto_inference_v2_run(
                    run_id=run_id,
                    assertion_urn="urn:li:assertion:test",
                    preprocessing_id="daily",
                    train_df=train_df,
                    prediction_df=pred_df,
                    model_config_dict={},
                    pairings_used=[],
                    sensitivity_level=None,
                    interval_hours=24,
                    num_intervals=7,
                )

            session_state: dict[str, Any] = {
                _AUTO_INFERENCE_V2_RUNS_KEY: {
                    "run1": {"metadata": {"run_id": "run1"}},
                    "run2": {"metadata": {"run_id": "run2"}},
                },
            }

            loader_mock = MagicMock()
            loader_mock.cache.get_endpoint_cache.return_value = cache
            with patch(
                "scripts.streamlit_explorer.common.auto_inference_v2_runs.DataLoader",
                return_value=loader_mock,
            ):
                deleted_count = clear_all_auto_inference_v2_runs(
                    hostname="test.example.com",
                    assertion_urn="urn:li:assertion:test",
                    session_state=session_state,
                )

            assert deleted_count == 2
            assert len(session_state.get(_AUTO_INFERENCE_V2_RUNS_KEY, {})) == 0

    def test_handles_empty_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))
            session_state: dict[str, Any] = {}

            loader_mock = MagicMock()
            loader_mock.cache.get_endpoint_cache.return_value = cache
            with patch(
                "scripts.streamlit_explorer.common.auto_inference_v2_runs.DataLoader",
                return_value=loader_mock,
            ):
                deleted_count = clear_all_auto_inference_v2_runs(
                    hostname="test.example.com",
                    assertion_urn="urn:li:assertion:test",
                    session_state=session_state,
                )

            assert deleted_count == 0

    def test_filters_by_assertion_urn(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = EndpointCache("test.example.com", Path(tmpdir))

            train_df = pd.DataFrame(
                {"ds": pd.date_range("2024-01-01", periods=10), "y": range(10)}
            )
            pred_df = pd.DataFrame({"timestamp_ms": [1000], "yhat": [5.0]})

            # Save runs for different assertions
            cache.save_auto_inference_v2_run(
                run_id="run_for_assertion_a",
                assertion_urn="urn:li:assertion:a",
                preprocessing_id="daily",
                train_df=train_df,
                prediction_df=pred_df,
                model_config_dict={},
                pairings_used=[],
                sensitivity_level=None,
                interval_hours=24,
                num_intervals=7,
            )
            cache.save_auto_inference_v2_run(
                run_id="run_for_assertion_b",
                assertion_urn="urn:li:assertion:b",
                preprocessing_id="daily",
                train_df=train_df,
                prediction_df=pred_df,
                model_config_dict={},
                pairings_used=[],
                sensitivity_level=None,
                interval_hours=24,
                num_intervals=7,
            )

            session_state: dict[str, Any] = {
                _AUTO_INFERENCE_V2_RUNS_KEY: {
                    "run_for_assertion_a": {
                        "metadata": {"run_id": "run_for_assertion_a"}
                    },
                    "run_for_assertion_b": {
                        "metadata": {"run_id": "run_for_assertion_b"}
                    },
                },
            }

            loader_mock = MagicMock()
            loader_mock.cache.get_endpoint_cache.return_value = cache
            with patch(
                "scripts.streamlit_explorer.common.auto_inference_v2_runs.DataLoader",
                return_value=loader_mock,
            ):
                # Clear only for assertion a
                deleted_count = clear_all_auto_inference_v2_runs(
                    hostname="test.example.com",
                    assertion_urn="urn:li:assertion:a",
                    session_state=session_state,
                )

            assert deleted_count == 1
            assert "run_for_assertion_a" not in session_state.get(
                _AUTO_INFERENCE_V2_RUNS_KEY, {}
            )
            # run_for_assertion_b should still be in session_state since we only listed/deleted runs for assertion a
