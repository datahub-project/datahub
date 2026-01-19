"""
Integration tests for inference_utils module.

These tests verify that the inference utilities correctly persist, update, and delete
assertion inference data via DataHub. The data model is:

1. AssertionInferenceDetails - A standalone aspect on Assertion entities
   - Stores: model config, hyperparameters, preprocessing config, training evals

2. AssertionEvaluationContext (with embeddedAssertions) - Nested within MonitorInfo aspect
   on Monitor entities:
   - MonitorInfo.assertionMonitor.assertions[].context.embeddedAssertions
   - Stores: prediction points (timestamps, y, yhat, bounds, anomaly info)

These tests require:
- A running DataHub instance
- The datahub-executor package installed
- The observe-models package installed
"""

import logging
import time
import uuid
from typing import Generator, List

import pandas as pd
import pytest
from datahub_executor.common.monitor.inference_v2.inference_utils import (
    OBSERVE_MODELS_VERSION,
    AnomalyAssertions,
    # Assertion conversion namespaces
    ForecastAssertions,
    ModelConfig,
    build_evaluation_context,
    # Context builders
    build_inference_details,
    parse_inference_details,
)

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, MonitorUrn
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns

logger = logging.getLogger(__name__)

# Test constants - unique ID per test run to avoid collisions
TEST_UNIQUE_ID = str(uuid.uuid4())[:8]
TEST_DATASET_URN = make_dataset_urn(
    platform="postgres", name=f"inference_utils_test_{TEST_UNIQUE_ID}"
)
TEST_MONITOR_ID = f"inference-test-monitor-{TEST_UNIQUE_ID}"
TEST_MONITOR_URN = MonitorUrn(TEST_DATASET_URN, TEST_MONITOR_ID).urn()
TEST_ASSERTION_URN = AssertionUrn(f"inference-test-assertion-{TEST_UNIQUE_ID}").urn()

# Default preprocessing config JSON (required by ModelConfig)
DEFAULT_PREPROCESSING_JSON = (
    '{"type": "volume", "freq": "1D", "_schemaVersion": "1.0.0"}'
)

# Track all URNs created during tests for cleanup
generated_urns: List[str] = []


def _cleanup_urns(graph_client: DataHubGraph, urns: List[str]) -> None:
    """Clean up URNs using the standard utility, with logging."""
    if urns:
        logger.info(f"Cleaning up {len(urns)} test URNs")
        delete_urns(graph_client, urns)
        wait_for_writes_to_sync()


@pytest.fixture(scope="module")
def test_dataset(graph_client: DataHubGraph) -> Generator[str, None, None]:
    """Create a test dataset for the assertions to reference."""
    # Pre-delete for idempotency (in case previous test run failed)
    _cleanup_urns(graph_client, [TEST_DATASET_URN])

    logger.info(f"Creating test dataset: {TEST_DATASET_URN}")
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=TEST_DATASET_URN, aspect=models.StatusClass(removed=False)
    )
    graph_client.emit(mcpw)
    wait_for_writes_to_sync()
    generated_urns.append(TEST_DATASET_URN)

    yield TEST_DATASET_URN

    # Cleanup handled by module-level fixture


@pytest.fixture(scope="module")
def test_assertion(
    graph_client: DataHubGraph, test_dataset: str
) -> Generator[str, None, None]:
    """Create a test assertion entity."""
    # Pre-delete for idempotency
    _cleanup_urns(graph_client, [TEST_ASSERTION_URN])

    logger.info(f"Creating test assertion: {TEST_ASSERTION_URN}")
    assertion_info = models.AssertionInfoClass(
        type=models.AssertionTypeClass.VOLUME,
        volumeAssertion=models.VolumeAssertionInfoClass(
            type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
            entity=test_dataset,
        ),
        source=models.AssertionSourceClass(
            type=models.AssertionSourceTypeClass.INFERRED
        ),
    )
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=TEST_ASSERTION_URN, aspect=assertion_info
    )
    graph_client.emit(mcpw)
    wait_for_writes_to_sync()
    generated_urns.append(TEST_ASSERTION_URN)

    yield TEST_ASSERTION_URN

    # Cleanup handled by module-level fixture


@pytest.fixture(scope="module")
def test_monitor(
    graph_client: DataHubGraph, test_dataset: str, test_assertion: str
) -> Generator[str, None, None]:
    """Create a test monitor entity."""
    # Pre-delete for idempotency
    _cleanup_urns(graph_client, [TEST_MONITOR_URN])

    logger.info(f"Creating test monitor: {TEST_MONITOR_URN}")
    monitor_info = models.MonitorInfoClass(
        type=models.MonitorTypeClass.ASSERTION,
        status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
        assertionMonitor=models.AssertionMonitorClass(
            assertions=[
                models.AssertionEvaluationSpecClass(
                    assertion=test_assertion,
                    schedule=models.CronScheduleClass(cron="0 0 * * *", timezone="UTC"),
                )
            ]
        ),
    )
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=TEST_MONITOR_URN, aspect=monitor_info
    )
    graph_client.emit(mcpw)
    wait_for_writes_to_sync()
    generated_urns.append(TEST_MONITOR_URN)

    yield TEST_MONITOR_URN

    # Cleanup handled by module-level fixture


@pytest.fixture(scope="module", autouse=True)
def cleanup_all_urns(graph_client: DataHubGraph):
    """
    Module-level autouse fixture that cleans up all generated URNs after all tests complete.

    This follows the pattern from other smoke tests (e.g., test_forms.py) where URNs
    are tracked in a list and cleaned up at the end.
    """
    yield
    logger.info("Module cleanup: removing all generated test URNs")
    _cleanup_urns(graph_client, generated_urns)
    generated_urns.clear()


class TestAssertionInferenceDetailsPersistence:
    """Tests for AssertionInferenceDetails persistence on Assertion entities."""

    def test_create_inference_details(
        self, graph_client: DataHubGraph, test_assertion: str
    ) -> None:
        """Test creating new inference details on an assertion."""
        generated_at = int(time.time() * 1000)

        # Create model configuration
        forecast_evals_json = '{"_schemaVersion": "1.0.0", "runs": [{"mae": 0.05, "rmse": 0.07, "mape": 0.02, "train_start_millis": 1704067200000, "train_end_millis": 1706745600000}]}'
        model_config = ModelConfig(
            forecast_model_name="prophet",
            forecast_model_version="0.1.0",
            forecast_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"seasonality_mode": "additive"}}',
            forecast_evals_json=forecast_evals_json,
            preprocessing_config_json=DEFAULT_PREPROCESSING_JSON,
            confidence=0.95,
            generated_at=generated_at,
        )

        # Build and persist inference details
        inference_details = build_inference_details(model_config=model_config)
        mcpw = MetadataChangeProposalWrapper(
            entityUrn=test_assertion, aspect=inference_details
        )
        graph_client.emit(mcpw)
        wait_for_writes_to_sync()

        # Read back and verify
        read_details = graph_client.get_aspect(
            test_assertion, models.AssertionInferenceDetailsClass
        )

        assert read_details is not None
        assert read_details.modelId == "observe-models"
        assert read_details.modelVersion == OBSERVE_MODELS_VERSION

        parsed_config = parse_inference_details(read_details)
        assert parsed_config is not None
        assert parsed_config.forecast_model_name == "prophet"
        assert parsed_config.forecast_model_version == "0.1.0"
        assert parsed_config.confidence == 0.95

        logger.info("Create inference details test passed")

    def test_update_inference_details(
        self, graph_client: DataHubGraph, test_assertion: str
    ) -> None:
        """Test updating existing inference details on an assertion."""
        generated_at = int(time.time() * 1000)

        # Update with new model version
        model_config = ModelConfig(
            forecast_model_name="prophet",
            forecast_model_version="0.2.0",  # Updated version
            forecast_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"seasonality_mode": "multiplicative"}}',
            forecast_evals_json='{"_schemaVersion": "1.0.0", "runs": [{"mae": 0.03, "rmse": 0.05, "mape": 0.01}]}',
            preprocessing_config_json='{"type": "volume", "freq": "1H", "_schemaVersion": "1.0.0"}',
            confidence=0.98,  # Updated confidence
            generated_at=generated_at,
        )

        inference_details = build_inference_details(model_config=model_config)
        mcpw = MetadataChangeProposalWrapper(
            entityUrn=test_assertion, aspect=inference_details
        )
        graph_client.emit(mcpw)
        wait_for_writes_to_sync()

        # Read back and verify update
        read_details = graph_client.get_aspect(
            test_assertion, models.AssertionInferenceDetailsClass
        )

        assert read_details is not None
        parsed_config = parse_inference_details(read_details)
        assert parsed_config is not None
        assert parsed_config.forecast_model_version == "0.2.0"
        assert parsed_config.confidence == 0.98

        logger.info("Update inference details test passed")

    def test_delete_entity_with_inference_details(
        self, graph_client: DataHubGraph, test_dataset: str
    ) -> None:
        """Test that deleting an assertion entity also removes its inference details.

        Note: DataHubGraph doesn't support aspect-level deletion directly.
        To delete an individual aspect, use the GraphQL deleteAspect mutation.
        This test verifies entity-level deletion removes all aspects.
        """
        # Create a separate assertion for delete test
        delete_assertion_urn = AssertionUrn(
            f"delete-test-assertion-{TEST_UNIQUE_ID}"
        ).urn()

        # Pre-delete for idempotency
        _cleanup_urns(graph_client, [delete_assertion_urn])

        # Create assertion
        assertion_info = models.AssertionInfoClass(
            type=models.AssertionTypeClass.VOLUME,
            volumeAssertion=models.VolumeAssertionInfoClass(
                type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity=test_dataset,
            ),
            source=models.AssertionSourceClass(
                type=models.AssertionSourceTypeClass.INFERRED
            ),
        )
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=delete_assertion_urn, aspect=assertion_info
            )
        )
        wait_for_writes_to_sync()

        # Add inference details
        model_config = ModelConfig(
            forecast_model_name="test_model",
            forecast_model_version="1.0.0",
            preprocessing_config_json=DEFAULT_PREPROCESSING_JSON,
            confidence=0.90,
            generated_at=int(time.time() * 1000),
        )
        inference_details = build_inference_details(model_config=model_config)
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=delete_assertion_urn, aspect=inference_details
            )
        )
        wait_for_writes_to_sync()

        # Verify it exists
        read_details = graph_client.get_aspect(
            delete_assertion_urn, models.AssertionInferenceDetailsClass
        )
        assert read_details is not None

        # Delete the entire assertion entity using standard utility
        delete_urns(graph_client, [delete_assertion_urn])
        wait_for_writes_to_sync()

        # Verify entity and its aspects are gone
        read_details = graph_client.get_aspect(
            delete_assertion_urn, models.AssertionInferenceDetailsClass
        )
        assert read_details is None

        read_info = graph_client.get_aspect(
            delete_assertion_urn, models.AssertionInfoClass
        )
        assert read_info is None

        logger.info("Delete entity with inference details test passed")


class TestEmbeddedAssertionsPersistence:
    """Tests for embedded assertions persistence via Monitor entities.

    AssertionEvaluationContext (containing embeddedAssertions) is nested within:
    MonitorInfo.assertionMonitor.assertions[].context
    """

    def test_create_forecast_embedded_assertions(
        self, graph_client: DataHubGraph, test_monitor: str, test_assertion: str
    ) -> None:
        """Test persisting forecast embedded assertions in MonitorInfo."""
        # Create forecast data
        timestamps = [1704067200000 + i * 86400000 for i in range(5)]
        forecast_df = pd.DataFrame(
            {
                "timestamp_ms": timestamps,
                "y": [100.0, 105.0, 110.0, 108.0, 112.0],
                "yhat": [101.0, 104.0, 109.0, 110.0, 111.0],
                "yhat_lower": [95.0, 98.0, 103.0, 104.0, 105.0],
                "yhat_upper": [107.0, 110.0, 115.0, 116.0, 117.0],
            }
        )

        # Convert to embedded assertions
        embedded_assertions = ForecastAssertions.from_df(
            df=forecast_df,
            entity_urn=TEST_DATASET_URN,
        )

        # Build model config
        model_config = ModelConfig(
            forecast_model_name="prophet",
            forecast_model_version="0.1.0",
            preprocessing_config_json=DEFAULT_PREPROCESSING_JSON,
            confidence=0.95,
            generated_at=int(time.time() * 1000),
        )

        # Build evaluation context
        eval_context = build_evaluation_context(
            model_config=model_config,
            embedded_assertions=embedded_assertions,
        )

        # Update MonitorInfo with context containing embedded assertions
        monitor_info = models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=test_assertion,
                        schedule=models.CronScheduleClass(
                            cron="0 0 * * *", timezone="UTC"
                        ),
                        context=eval_context,
                    )
                ]
            ),
        )

        mcpw = MetadataChangeProposalWrapper(
            entityUrn=test_monitor, aspect=monitor_info
        )
        graph_client.emit(mcpw)
        wait_for_writes_to_sync()

        # Read back MonitorInfo and verify embedded assertions
        read_monitor_info = graph_client.get_aspect(
            test_monitor, models.MonitorInfoClass
        )

        assert read_monitor_info is not None
        assert read_monitor_info.assertionMonitor is not None
        assert len(read_monitor_info.assertionMonitor.assertions) == 1

        read_context = read_monitor_info.assertionMonitor.assertions[0].context
        assert read_context is not None
        assert read_context.embeddedAssertions is not None
        assert len(read_context.embeddedAssertions) == 5

        # Convert back to DataFrame and verify values
        result_df = ForecastAssertions.to_df(read_context.embeddedAssertions)
        assert len(result_df) == 5

        # Verify timestamps
        for i, ts in enumerate(timestamps):
            assert result_df.iloc[i]["timestamp_ms"] == ts

        # Verify y and yhat values
        for col in ["y", "yhat", "yhat_lower", "yhat_upper"]:
            orig = forecast_df[col].values
            result = result_df[col].values
            for i, (o, r) in enumerate(zip(orig, result, strict=False)):
                assert abs(o - r) < 0.001, (
                    f"Column {col} mismatch at index {i}: {o} vs {r}"
                )

        logger.info("Create forecast embedded assertions test passed")

    def test_update_embedded_assertions(
        self, graph_client: DataHubGraph, test_monitor: str, test_assertion: str
    ) -> None:
        """Test updating embedded assertions in MonitorInfo."""
        # Create new forecast data (different from create test)
        timestamps = [1704067200000 + i * 3600000 for i in range(3)]  # 3 hours
        forecast_df = pd.DataFrame(
            {
                "timestamp_ms": timestamps,
                "y": [200.0, 210.0, 220.0],  # Different values
                "yhat": [201.0, 211.0, 221.0],
                "yhat_lower": [195.0, 205.0, 215.0],
                "yhat_upper": [207.0, 217.0, 227.0],
            }
        )

        embedded_assertions = ForecastAssertions.from_df(
            df=forecast_df,
            entity_urn=TEST_DATASET_URN,
        )

        model_config = ModelConfig(
            forecast_model_name="prophet",
            forecast_model_version="0.2.0",  # Updated version
            preprocessing_config_json=DEFAULT_PREPROCESSING_JSON,
            confidence=0.98,
            generated_at=int(time.time() * 1000),
        )

        eval_context = build_evaluation_context(
            model_config=model_config,
            embedded_assertions=embedded_assertions,
        )

        monitor_info = models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=test_assertion,
                        schedule=models.CronScheduleClass(
                            cron="0 0 * * *", timezone="UTC"
                        ),
                        context=eval_context,
                    )
                ]
            ),
        )

        graph_client.emit(
            MetadataChangeProposalWrapper(entityUrn=test_monitor, aspect=monitor_info)
        )
        wait_for_writes_to_sync()

        # Read back and verify update
        read_monitor_info = graph_client.get_aspect(
            test_monitor, models.MonitorInfoClass
        )
        assert read_monitor_info is not None
        assert read_monitor_info.assertionMonitor is not None

        read_context = read_monitor_info.assertionMonitor.assertions[0].context
        assert read_context is not None
        assert read_context.embeddedAssertions is not None
        assert len(read_context.embeddedAssertions) == 3  # Updated from 5 to 3

        result_df = ForecastAssertions.to_df(read_context.embeddedAssertions)
        assert result_df.iloc[0]["y"] == 200.0  # Verify new values

        logger.info("Update embedded assertions test passed")

    def test_create_anomaly_embedded_assertions(
        self, graph_client: DataHubGraph, test_dataset: str
    ) -> None:
        """Test persisting anomaly detection embedded assertions in MonitorInfo."""
        # Create separate monitor for anomaly test
        anomaly_monitor_urn = MonitorUrn(
            test_dataset, f"anomaly-monitor-{TEST_UNIQUE_ID}"
        ).urn()
        anomaly_assertion_urn = AssertionUrn(
            f"anomaly-assertion-{TEST_UNIQUE_ID}"
        ).urn()

        # Pre-delete for idempotency
        _cleanup_urns(graph_client, [anomaly_monitor_urn, anomaly_assertion_urn])

        # Track for cleanup
        generated_urns.extend([anomaly_monitor_urn, anomaly_assertion_urn])

        # Create assertion for anomaly
        assertion_info = models.AssertionInfoClass(
            type=models.AssertionTypeClass.VOLUME,
            volumeAssertion=models.VolumeAssertionInfoClass(
                type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity=test_dataset,
            ),
            source=models.AssertionSourceClass(
                type=models.AssertionSourceTypeClass.INFERRED
            ),
        )
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=anomaly_assertion_urn, aspect=assertion_info
            )
        )
        wait_for_writes_to_sync()

        # Create anomaly data with an anomaly point
        timestamps = [1704067200000 + i * 86400000 for i in range(5)]
        anomaly_df = pd.DataFrame(
            {
                "timestamp_ms": timestamps,
                "y": [100.0, 105.0, 200.0, 108.0, 112.0],  # 200 is anomaly
                "yhat": [101.0, 104.0, 109.0, 110.0, 111.0],
                "yhat_lower": [95.0, 98.0, 103.0, 104.0, 105.0],
                "yhat_upper": [107.0, 110.0, 115.0, 116.0, 117.0],
                "detection_band_lower": [90.0, 93.0, 98.0, 99.0, 100.0],
                "detection_band_upper": [112.0, 115.0, 120.0, 121.0, 122.0],
                "anomaly_score": [0.1, 0.15, 0.95, 0.12, 0.08],
                "is_anomaly": [False, False, True, False, False],
            }
        )

        embedded_assertions = AnomalyAssertions.from_df(
            df=anomaly_df,
            entity_urn=test_dataset,
        )

        model_config = ModelConfig(
            anomaly_model_name="datahub_forecast_anomaly",
            anomaly_model_version="0.1.0",
            preprocessing_config_json=DEFAULT_PREPROCESSING_JSON,
            confidence=0.95,
            generated_at=int(time.time() * 1000),
        )

        eval_context = build_evaluation_context(
            model_config=model_config,
            embedded_assertions=embedded_assertions,
        )

        monitor_info = models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=anomaly_assertion_urn,
                        schedule=models.CronScheduleClass(
                            cron="0 0 * * *", timezone="UTC"
                        ),
                        context=eval_context,
                    )
                ]
            ),
        )

        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=anomaly_monitor_urn, aspect=monitor_info
            )
        )
        wait_for_writes_to_sync()

        # Read back and verify
        read_monitor_info = graph_client.get_aspect(
            anomaly_monitor_urn, models.MonitorInfoClass
        )

        assert read_monitor_info is not None
        assert read_monitor_info.assertionMonitor is not None
        read_context = read_monitor_info.assertionMonitor.assertions[0].context
        assert read_context is not None
        assert read_context.embeddedAssertions is not None
        assert len(read_context.embeddedAssertions) == 5

        # Convert back and verify anomaly detection
        result_df = AnomalyAssertions.to_df(read_context.embeddedAssertions)
        assert len(result_df) == 5

        # Verify anomaly columns
        assert "detection_band_lower" in result_df.columns
        assert "detection_band_upper" in result_df.columns
        assert "anomaly_score" in result_df.columns
        assert "is_anomaly" in result_df.columns

        # Verify the anomaly was round-tripped correctly
        anomaly_rows = result_df[result_df["is_anomaly"] == True]  # noqa: E712
        assert len(anomaly_rows) == 1
        assert anomaly_rows.iloc[0]["y"] == 200.0

        logger.info("Create anomaly embedded assertions test passed")

    def test_delete_embedded_assertions(
        self, graph_client: DataHubGraph, test_monitor: str, test_assertion: str
    ) -> None:
        """Test clearing embedded assertions from MonitorInfo context."""
        # Update MonitorInfo without context (clears embedded assertions)
        monitor_info = models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=test_assertion,
                        schedule=models.CronScheduleClass(
                            cron="0 0 * * *", timezone="UTC"
                        ),
                        # No context - effectively deletes embedded assertions
                    )
                ]
            ),
        )

        graph_client.emit(
            MetadataChangeProposalWrapper(entityUrn=test_monitor, aspect=monitor_info)
        )
        wait_for_writes_to_sync()

        # Read back and verify context is None
        read_monitor_info = graph_client.get_aspect(
            test_monitor, models.MonitorInfoClass
        )
        assert read_monitor_info is not None
        assert read_monitor_info.assertionMonitor is not None
        assert read_monitor_info.assertionMonitor.assertions[0].context is None

        logger.info("Delete embedded assertions test passed")


class TestFullPipelineRoundTrip:
    """Integration test for complete inference pipeline with both aspects."""

    def test_complete_forecast_pipeline(
        self, graph_client: DataHubGraph, test_dataset: str
    ) -> None:
        """Test complete pipeline: persist inference details AND embedded assertions."""
        # Create dedicated entities for this test
        pipeline_assertion_urn = AssertionUrn(
            f"pipeline-assertion-{TEST_UNIQUE_ID}"
        ).urn()
        pipeline_monitor_urn = MonitorUrn(
            test_dataset, f"pipeline-monitor-{TEST_UNIQUE_ID}"
        ).urn()

        # Pre-delete for idempotency
        _cleanup_urns(graph_client, [pipeline_monitor_urn, pipeline_assertion_urn])

        # Track for cleanup
        generated_urns.extend([pipeline_monitor_urn, pipeline_assertion_urn])

        # Step 1: Create assertion with inference details
        assertion_info = models.AssertionInfoClass(
            type=models.AssertionTypeClass.VOLUME,
            volumeAssertion=models.VolumeAssertionInfoClass(
                type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity=test_dataset,
            ),
            source=models.AssertionSourceClass(
                type=models.AssertionSourceTypeClass.INFERRED
            ),
        )
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=pipeline_assertion_urn, aspect=assertion_info
            )
        )

        # Full model configuration
        model_config = ModelConfig(
            forecast_model_name="prophet",
            forecast_model_version="0.1.0",
            anomaly_model_name="datahub_forecast_anomaly",
            anomaly_model_version="0.1.0",
            forecast_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"seasonality_mode": "additive"}}',
            anomaly_config_json='{"_schemaVersion": "1.0.0", "hyperparameters": {"deviation_threshold": 1.8}}',
            forecast_evals_json='{"_schemaVersion": "1.0.0", "runs": [{"mae": 0.03, "rmse": 0.05, "mape": 0.01}]}',
            anomaly_evals_json='{"_schemaVersion": "1.0.0", "runs": [{"precision": 0.85, "recall": 0.90, "f1_score": 0.87}]}',
            preprocessing_config_json='{"type": "volume", "freq": "1D", "fill_method": "ffill", "_schemaVersion": "1.0.0"}',
            confidence=0.95,
            generated_at=int(time.time() * 1000),
        )

        # Persist inference details to assertion
        inference_details = build_inference_details(model_config=model_config)
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=pipeline_assertion_urn, aspect=inference_details
            )
        )

        # Step 2: Create forecast data and persist via monitor
        timestamps = [1704067200000 + i * 3600000 for i in range(10)]
        forecast_df = pd.DataFrame(
            {
                "timestamp_ms": timestamps,
                "y": [100.0 + i * 2 for i in range(10)],
                "yhat": [101.0 + i * 2 for i in range(10)],
                "yhat_lower": [95.0 + i * 2 for i in range(10)],
                "yhat_upper": [107.0 + i * 2 for i in range(10)],
            }
        )

        embedded_assertions = ForecastAssertions.from_df(
            df=forecast_df,
            entity_urn=test_dataset,
        )

        eval_context = build_evaluation_context(
            model_config=model_config,
            embedded_assertions=embedded_assertions,
        )

        monitor_info = models.MonitorInfoClass(
            type=models.MonitorTypeClass.ASSERTION,
            status=models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE),
            assertionMonitor=models.AssertionMonitorClass(
                assertions=[
                    models.AssertionEvaluationSpecClass(
                        assertion=pipeline_assertion_urn,
                        schedule=models.CronScheduleClass(
                            cron="0 * * * *", timezone="UTC"
                        ),
                        context=eval_context,
                    )
                ]
            ),
        )

        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=pipeline_monitor_urn, aspect=monitor_info
            )
        )
        wait_for_writes_to_sync()

        # Step 3: Verify both aspects persisted correctly
        # Check assertion inference details
        read_inference = graph_client.get_aspect(
            pipeline_assertion_urn, models.AssertionInferenceDetailsClass
        )
        assert read_inference is not None
        assert read_inference.modelId == "observe-models"
        assert read_inference.modelVersion == OBSERVE_MODELS_VERSION

        parsed_config = parse_inference_details(read_inference)
        assert parsed_config is not None
        assert parsed_config.forecast_model_name == "prophet"
        assert parsed_config.anomaly_model_name == "datahub_forecast_anomaly"

        # Check monitor embedded assertions
        read_monitor = graph_client.get_aspect(
            pipeline_monitor_urn, models.MonitorInfoClass
        )
        assert read_monitor is not None
        assert read_monitor.assertionMonitor is not None
        read_context = read_monitor.assertionMonitor.assertions[0].context
        assert read_context is not None
        assert read_context.embeddedAssertions is not None
        assert len(read_context.embeddedAssertions) == 10

        # Verify data round-trip
        result_df = ForecastAssertions.to_df(read_context.embeddedAssertions)
        assert len(result_df) == 10
        assert result_df.iloc[0]["y"] == 100.0
        assert result_df.iloc[9]["y"] == 118.0

        logger.info("Complete forecast pipeline round-trip test passed")
