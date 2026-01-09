"""Tests for the pages/anomaly_comparison.py module."""

import numpy as np
import pandas as pd

from scripts.streamlit_explorer.model_explorer.anomaly_comparison import (
    _compute_classification_metrics,
)


class TestClassificationMetrics:
    """Tests for classification metrics computation."""

    def test_perfect_detection(self):
        """Test metrics when model detects all ground truth anomalies."""
        model_detected = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 10:00", "2024-01-01 12:00"]),
            }
        )
        ground_truth = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 10:00", "2024-01-01 12:00"]),
                "status": ["confirmed", "confirmed"],
            }
        )

        metrics = _compute_classification_metrics(model_detected, ground_truth)

        assert metrics["TP"] == 2
        assert metrics["FP"] == 0
        assert metrics["FN"] == 0
        assert metrics["Precision"] == 1.0
        assert metrics["Recall"] == 1.0
        assert metrics["F1"] == 1.0

    def test_no_detections(self):
        """Test metrics when model detects nothing."""
        model_detected = pd.DataFrame({"ds": pd.Series([], dtype="datetime64[ns]")})
        ground_truth = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 10:00", "2024-01-01 12:00"]),
                "status": ["confirmed", "confirmed"],
            }
        )

        metrics = _compute_classification_metrics(model_detected, ground_truth)

        assert metrics["TP"] == 0
        assert metrics["FP"] == 0
        assert metrics["FN"] == 2
        assert metrics["Precision"] == 0.0
        assert metrics["Recall"] == 0.0
        assert metrics["F1"] == 0.0

    def test_all_false_positives(self):
        """Test metrics when all detections are false positives."""
        model_detected = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 10:00", "2024-01-01 12:00"]),
            }
        )
        ground_truth = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-02 10:00"]),  # Different day
                "status": ["confirmed"],
            }
        )

        metrics = _compute_classification_metrics(model_detected, ground_truth)

        assert metrics["TP"] == 0
        assert metrics["FP"] == 2
        assert metrics["FN"] == 1
        assert metrics["Precision"] == 0.0

    def test_tolerates_time_difference(self):
        """Test that detection within tolerance window counts as TP."""
        # Model detects 30 minutes after ground truth (within 1 hour default tolerance)
        model_detected = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 10:30"]),
            }
        )
        ground_truth = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 10:00"]),
                "status": ["confirmed"],
            }
        )

        metrics = _compute_classification_metrics(
            model_detected, ground_truth, tolerance_seconds=3600
        )

        assert metrics["TP"] == 1
        assert metrics["FP"] == 0
        assert metrics["FN"] == 0

    def test_outside_tolerance_counts_as_fp_fn(self):
        """Test that detection outside tolerance is FP and ground truth is FN."""
        # Model detects 2 hours after ground truth (outside 1 hour tolerance)
        model_detected = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 12:00"]),
            }
        )
        ground_truth = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 10:00"]),
                "status": ["confirmed"],
            }
        )

        metrics = _compute_classification_metrics(
            model_detected, ground_truth, tolerance_seconds=3600
        )

        assert metrics["TP"] == 0
        assert metrics["FP"] == 1
        assert metrics["FN"] == 1

    def test_ignores_rejected_ground_truth(self):
        """Test that rejected anomalies are not counted as ground truth."""
        model_detected = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 10:00"]),
            }
        )
        ground_truth = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 10:00", "2024-01-01 12:00"]),
                "status": ["confirmed", "rejected"],  # Second is rejected
            }
        )

        metrics = _compute_classification_metrics(model_detected, ground_truth)

        # Only confirmed anomalies count as ground truth
        assert metrics["TP"] == 1
        assert metrics["FP"] == 0
        assert metrics["FN"] == 0

    def test_empty_ground_truth(self):
        """Test metrics when no ground truth anomalies exist."""
        model_detected = pd.DataFrame(
            {
                "ds": pd.to_datetime(["2024-01-01 10:00"]),
            }
        )
        ground_truth = pd.DataFrame({"ds": [], "status": []})

        metrics = _compute_classification_metrics(model_detected, ground_truth)

        assert metrics["TP"] == 0
        assert metrics["FP"] == 1
        assert metrics["FN"] == 0
        assert metrics["Precision"] == 0.0
        assert np.isnan(metrics["Recall"])  # Can't compute recall with no ground truth

    def test_f1_score_calculation(self):
        """Test F1 score is computed correctly."""
        # 2 TP, 1 FP, 1 FN
        # Precision = 2/3, Recall = 2/3, F1 = 2/3
        model_detected = pd.DataFrame(
            {
                "ds": pd.to_datetime(
                    [
                        "2024-01-01 10:00",
                        "2024-01-01 12:00",
                        "2024-01-01 14:00",  # FP
                    ]
                ),
            }
        )
        ground_truth = pd.DataFrame(
            {
                "ds": pd.to_datetime(
                    [
                        "2024-01-01 10:00",
                        "2024-01-01 12:00",
                        "2024-01-01 16:00",  # FN - not detected
                    ]
                ),
                "status": ["confirmed", "confirmed", "confirmed"],
            }
        )

        metrics = _compute_classification_metrics(model_detected, ground_truth)

        assert metrics["TP"] == 2
        assert metrics["FP"] == 1
        assert metrics["FN"] == 1
        assert abs(metrics["Precision"] - 2 / 3) < 0.01
        assert abs(metrics["Recall"] - 2 / 3) < 0.01
        assert abs(metrics["F1"] - 2 / 3) < 0.01
