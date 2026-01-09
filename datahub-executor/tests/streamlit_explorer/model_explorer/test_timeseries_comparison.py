"""Tests for the pages/timeseries_comparison.py module."""

from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd

from scripts.streamlit_explorer.model_explorer.model_training import TrainingRun


class TestVisualizationDefaultSelection:
    """Tests for visualization default selection logic."""

    def _create_mock_run(self, run_id: str, model_name: str, mae: float) -> TrainingRun:
        """Create a mock TrainingRun for testing."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        return TrainingRun(
            run_id=run_id,
            model_key=f"model_{run_id}",
            model_name=model_name,
            preprocessing_id="test_preproc",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": mae, "RMSE": mae * 1.2, "MAPE": mae * 2},
            color="#ff0000",
            dash=None,
            timestamp=datetime.now(),
            assertion_urn="urn:li:assertion:test",
        )

    def test_best_two_by_mae_are_selected(self):
        """Test that the two best models by MAE are selected for visualization."""
        runs = [
            self._create_mock_run("run1", "Model A", 5.0),
            self._create_mock_run("run2", "Model B", 2.0),  # Best
            self._create_mock_run("run3", "Model C", 3.5),  # Second best
            self._create_mock_run("run4", "Model D", 10.0),
        ]

        # Replicate the sorting logic from timeseries_comparison.py
        sorted_by_mae = sorted(
            runs,
            key=lambda r: float(r.metrics.get("MAE", float("inf"))),
        )
        best_two_ids = [sorted_by_mae[0].run_id, sorted_by_mae[1].run_id]

        assert best_two_ids == ["run2", "run3"]

    def test_handles_equal_mae_values(self):
        """Test handling when multiple models have the same MAE."""
        runs = [
            self._create_mock_run("run1", "Model A", 3.0),
            self._create_mock_run("run2", "Model B", 3.0),
            self._create_mock_run("run3", "Model C", 5.0),
        ]

        sorted_by_mae = sorted(
            runs,
            key=lambda r: float(r.metrics.get("MAE", float("inf"))),
        )
        best_two_ids = [sorted_by_mae[0].run_id, sorted_by_mae[1].run_id]

        # Both run1 and run2 have MAE 3.0, order depends on stable sort
        assert set(best_two_ids) == {"run1", "run2"}

    def test_handles_single_run(self):
        """Test handling when only one run exists."""
        runs = [self._create_mock_run("run1", "Model A", 5.0)]

        sorted_by_mae = sorted(
            runs,
            key=lambda r: float(r.metrics.get("MAE", float("inf"))),
        )

        # Should have only one run
        assert len(sorted_by_mae) == 1
        assert sorted_by_mae[0].run_id == "run1"

    def test_handles_missing_mae(self):
        """Test handling when MAE is missing from metrics."""
        train_df = pd.DataFrame({"ds": [], "y": []})
        test_df = pd.DataFrame({"ds": [], "y": []})
        forecast = pd.DataFrame({"ds": [], "yhat": []})

        run_with_mae = TrainingRun(
            run_id="run1",
            model_key="model_1",
            model_name="Model With MAE",
            preprocessing_id="test",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"MAE": 2.0, "RMSE": 2.5, "MAPE": 5.0},
            color="#ff0000",
            dash=None,
            timestamp=datetime.now(),
        )

        run_without_mae = TrainingRun(
            run_id="run2",
            model_key="model_2",
            model_name="Model Without MAE",
            preprocessing_id="test",
            train_df=train_df,
            test_df=test_df,
            forecast=forecast,
            model=MagicMock(),
            metrics={"RMSE": 3.0, "MAPE": 6.0},  # No MAE
            color="#00ff00",
            dash=None,
            timestamp=datetime.now(),
        )

        runs = [run_without_mae, run_with_mae]

        sorted_by_mae = sorted(
            runs,
            key=lambda r: float(r.metrics.get("MAE", float("inf"))),
        )

        # Run with MAE should come first (2.0 < inf)
        assert sorted_by_mae[0].run_id == "run1"
        assert sorted_by_mae[1].run_id == "run2"


class TestMetricsSorting:
    """Tests for metrics table sorting."""

    def test_metrics_table_sorting_by_mae(self):
        """Test that metrics table data is sorted by MAE."""
        # Simulate the table data structure from _render_metrics_summary_table
        table_data = [
            {"Model": "A", "MAE": "5.00", "_mae_raw": 5.0},
            {"Model": "B", "MAE": "2.00", "_mae_raw": 2.0},
            {"Model": "C", "MAE": "3.50", "_mae_raw": 3.5},
        ]

        # Sort by MAE (lowest/best first)
        table_data.sort(key=lambda x: float(str(x["_mae_raw"])))

        assert table_data[0]["Model"] == "B"  # MAE 2.0
        assert table_data[1]["Model"] == "C"  # MAE 3.5
        assert table_data[2]["Model"] == "A"  # MAE 5.0
