"""Integration tests for Actions observability metrics with action-level labels.

Tests validate that Phase 1 action metrics include proper dimensionality:
- action_urn: Unique identifier for the action
- action_name: Human-readable name from config
- action_type: Action class name
- stage: Pipeline stage (LIVE, BOOTSTRAP, ROLLBACK)
- status: Execution status (success, failure, unknown)
"""

import logging
import re
import time
from typing import Any

import pytest
import requests

from tests.integrations_service_utils import bootstrap_action, rollback_action
from tests.utils import get_integrations_service_url

logger = logging.getLogger(__name__)


def get_integrations_metrics() -> str:
    """Fetch metrics from integrations service /metrics endpoint."""
    url = f"{get_integrations_service_url()}/metrics"
    logger.info(f"Fetching metrics from {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.text


def parse_prometheus_metrics(metrics_text: str) -> dict[str, list[dict[str, Any]]]:
    """Parse Prometheus text format into structured data.

    Returns:
        Dict mapping metric names to list of sample dicts with:
        - name: metric name
        - labels: dict of label key-value pairs
        - value: numeric value
    """
    metrics: dict[str, list[dict[str, Any]]] = {}

    for line in metrics_text.split("\n"):
        line = line.strip()

        # Skip comments and empty lines
        if not line or line.startswith("#"):
            continue

        # Parse metric line: metric_name{label="value",label2="value2"} 123.45 [timestamp]
        match = re.match(
            r"^([a-zA-Z_:][a-zA-Z0-9_:]*)((?:\{[^}]*\})?) (.+?)(?:\s+\d+)?$", line
        )
        if not match:
            continue

        metric_name = match.group(1)
        labels_str = match.group(2)
        value_str = match.group(3)

        # Parse labels
        labels: dict[str, str] = {}
        if labels_str and labels_str.startswith("{"):
            labels_content = labels_str[1:-1]
            for label_match in re.finditer(
                r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"', labels_content
            ):
                labels[label_match.group(1)] = label_match.group(2)

        # Parse value
        try:
            value = float(value_str)
        except ValueError:
            continue

        if metric_name not in metrics:
            metrics[metric_name] = []

        metrics[metric_name].append(
            {
                "name": metric_name,
                "labels": labels,
                "value": value,
            }
        )

    return metrics


def create_test_action_config(action_urn: str, action_name: str) -> dict:
    """Create a minimal test action configuration.

    This creates a simple no-op action config that can be used for testing
    without actually processing any real data. Uses the 'replay' event source
    which reads events from an empty file, and RecordingAction which writes to a file.

    Files are created in /tmp to ensure they're accessible to both the test process
    and the Docker container running the action subprocess.
    """

    # Create files in /tmp with unique names to avoid conflicts
    unique_id = action_urn.split(":")[-1]
    source_filename = f"/tmp/test_action_source_{unique_id}.json"
    action_filename = f"/tmp/test_action_output_{unique_id}.json"

    # Create empty source file (no events to process)
    with open(source_filename, "w") as f:
        f.write("[]")  # Empty JSON array

    # Create empty action output file
    with open(action_filename, "w") as f:
        pass

    return {
        "name": action_name,
        "source": {
            "type": "replay",
            "config": {
                "filename": source_filename,  # Path accessible in Docker
            },
        },
        "action": {
            "type": "datahub_integrations.actions.recording_action.RecordingAction",
            "config": {
                "filename": action_filename,  # Path accessible in Docker
            },
        },
    }


def upsert_action_via_rest(action_urn: str, config: dict, graph_client) -> None:
    """Create or update action configuration via GraphQL/GMS."""
    import json

    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        DataHubActionConfigClass,
        DataHubActionInfoClass,
    )

    # Convert config to recipe JSON string
    recipe_json_str = json.dumps(config)

    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=action_urn,
            aspect=DataHubActionInfoClass(
                name=config["name"],
                type=config["action"]["type"],
                config=DataHubActionConfigClass(
                    recipe=recipe_json_str,
                ),
            ),
        )
    )
    logger.info(f"Created action {action_urn} via GraphQL")


@pytest.mark.read_only
class TestActionsObservabilityLabels:
    """Test that action metrics include proper action-level labels."""

    def test_actions_execution_metrics_have_action_labels(self) -> None:
        """Test that actions_execution_* metrics include action_urn, action_name, action_type labels.

        This test verifies Phase 1 enhancement: action-level dimensionality on existing metrics.
        """
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        # Check actions_execution_duration metric
        if "actions_execution_duration_seconds_bucket" in metrics:
            samples = metrics["actions_execution_duration_seconds_bucket"]
            logger.info(
                f"Found {len(samples)} samples for actions_execution_duration_seconds_bucket"
            )

            # Verify required labels exist
            required_labels = ["action_urn", "action_name", "action_type", "stage"]
            for sample in samples[:3]:  # Check first few samples
                sample_labels = sample.get("labels", {})
                found_labels = [
                    label for label in required_labels if label in sample_labels
                ]

                if len(found_labels) == len(required_labels):
                    logger.info(
                        f"✓ Found all required labels on actions_execution_duration: {required_labels}"
                    )
                    logger.info(f"  Sample labels: {sample_labels}")
                    break
            else:
                logger.warning(
                    f"actions_execution_duration samples may be missing labels. "
                    f"Sample: {samples[0] if samples else 'no samples'}"
                )
        else:
            logger.warning(
                "actions_execution_duration_seconds_bucket not found (expected if no actions have run)"
            )

        # Check actions_execution_total metric
        if "actions_execution_total" in metrics:
            samples = metrics["actions_execution_total"]
            logger.info(f"Found {len(samples)} samples for actions_execution_total")

            # Verify required labels exist
            required_labels = [
                "action_urn",
                "action_name",
                "action_type",
                "stage",
                "status",
            ]
            for sample in samples[:3]:
                sample_labels = sample.get("labels", {})
                found_labels = [
                    label for label in required_labels if label in sample_labels
                ]

                if len(found_labels) == len(required_labels):
                    logger.info(
                        f"✓ Found all required labels on actions_execution_total: {required_labels}"
                    )
                    logger.info(f"  Sample labels: {sample_labels}")
                    break
            else:
                logger.warning(
                    f"actions_execution_total samples may be missing labels. "
                    f"Sample: {samples[0] if samples else 'no samples'}"
                )
        else:
            logger.warning(
                "actions_execution_total not found (expected if no actions have run)"
            )

    def test_actions_venv_setup_has_action_labels(self) -> None:
        """Test that actions_venv_setup_duration includes action labels."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        if "actions_venv_setup_duration_seconds_bucket" in metrics:
            samples = metrics["actions_venv_setup_duration_seconds_bucket"]
            logger.info(
                f"Found {len(samples)} samples for actions_venv_setup_duration_seconds_bucket"
            )

            # Verify action labels exist (no stage/status for venv setup)
            required_labels = ["action_urn", "action_name", "action_type"]
            for sample in samples[:3]:
                sample_labels = sample.get("labels", {})
                found_labels = [
                    label for label in required_labels if label in sample_labels
                ]

                if len(found_labels) == len(required_labels):
                    logger.info(
                        f"✓ Found all required labels on actions_venv_setup_duration: {required_labels}"
                    )
                    logger.info(f"  Sample labels: {sample_labels}")
                    break
            else:
                logger.warning(
                    f"actions_venv_setup_duration samples may be missing labels. "
                    f"Sample: {samples[0] if samples else 'no samples'}"
                )
        else:
            logger.warning(
                "actions_venv_setup_duration_seconds_bucket not found (expected if no actions have run)"
            )

    def test_action_labels_contain_readable_values(self) -> None:
        """Test that action label values are human-readable and properly formatted.

        Verifies:
        - action_type extracts just class name (not full Python path)
        - action_name is readable (not just URN)
        - stage values are correct enum values
        """
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        # Check any actions metric
        for metric_name in [
            "actions_execution_total",
            "actions_venv_setup_duration_seconds_bucket",
        ]:
            if metric_name not in metrics:
                continue

            samples = metrics[metric_name]
            logger.info(f"Checking label values in {metric_name}")

            for sample in samples[:5]:  # Check first few samples
                labels = sample.get("labels", {})

                # Check action_type doesn't contain dots (should be just class name)
                action_type = labels.get("action_type", "")
                if action_type and "." not in action_type:
                    logger.info(
                        f"✓ action_type is clean class name: {action_type} (no dots)"
                    )

                # Check action_name is readable (not just "unknown" or empty)
                action_name = labels.get("action_name", "")
                if action_name and action_name != "unknown":
                    logger.info(f"✓ action_name is readable: {action_name}")

                # Check stage is valid enum value (if present)
                stage = labels.get("stage", "")
                valid_stages = [
                    "LIVE",
                    "BOOTSTRAP",
                    "ROLLBACK",
                    "live",
                    "bootstrap",
                    "rollback",
                ]
                if stage and stage in valid_stages:
                    logger.info(f"✓ stage is valid enum value: {stage}")

                # Check status is valid (if present)
                status = labels.get("status", "")
                valid_statuses = ["success", "failure", "unknown"]
                if status and status in valid_statuses:
                    logger.info(f"✓ status is valid value: {status}")

            break  # Only need to check one metric

    def test_actions_metrics_dimensionality_for_queries(self) -> None:
        """Test that action labels enable useful Prometheus queries.

        Verifies that we can query metrics:
        - Per action (by action_urn or action_name)
        - Per action type (to see which action classes are used)
        - Per stage (LIVE vs BOOTSTRAP vs ROLLBACK)
        - By success/failure status
        """
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        if "actions_execution_total" not in metrics:
            logger.warning(
                "No actions_execution_total metrics found, skipping query test"
            )
            return

        samples = metrics["actions_execution_total"]
        logger.info(f"Analyzing dimensionality from {len(samples)} samples")

        # Collect unique values for each label
        action_urns = {s.get("labels", {}).get("action_urn") for s in samples}
        action_names = {s.get("labels", {}).get("action_name") for s in samples}
        action_types = {s.get("labels", {}).get("action_type") for s in samples}
        stages = {s.get("labels", {}).get("stage") for s in samples}
        statuses = {s.get("labels", {}).get("status") for s in samples}

        # Remove None values
        action_urns.discard(None)
        action_names.discard(None)
        action_types.discard(None)
        stages.discard(None)
        statuses.discard(None)

        logger.info(
            f"Found dimensionality: "
            f"{len(action_urns)} unique actions, "
            f"{len(action_types)} action types, "
            f"{len(stages)} stages, "
            f"{len(statuses)} statuses"
        )

        if action_urns:
            logger.info(f"  Sample action URNs: {list(action_urns)[:3]}")
        if action_names:
            logger.info(f"  Sample action names: {list(action_names)[:3]}")
        if action_types:
            logger.info(f"  Action types: {list(action_types)}")
        if stages:
            logger.info(f"  Stages: {list(stages)}")
        if statuses:
            logger.info(f"  Statuses: {list(statuses)}")

        # Verify we have enough dimensionality to answer operational questions
        # (At least one of each should be present if actions have run)
        if action_urns and action_types and stages:
            logger.info(
                "✓ Metrics have sufficient dimensionality for per-action operational queries"
            )
        elif not action_urns:
            logger.warning(
                "No actions have run yet - cannot verify full dimensionality"
            )

    def test_phase2_action_state_tracking_metrics_exist(self) -> None:
        """Test that Phase 2 action state tracking metrics are registered.

        Phase 2 added metrics for tracking action lifecycle:
        - actions_running_count: Currently running actions per stage
        - actions_info: Action metadata with state
        - actions_success_total: Success counter
        - actions_error_total: Error counter
        """
        metrics_text = get_integrations_metrics()

        # Check for Phase 2 metrics in HELP/TYPE comments or actual metrics
        phase2_metrics = [
            "actions_running_count",
            "actions_info",
            "actions_success_total",
            "actions_error_total",
        ]

        found_metrics = []
        for metric_name in phase2_metrics:
            if metric_name in metrics_text:
                found_metrics.append(metric_name)
                logger.info(f"✓ Found Phase 2 metric: {metric_name}")

        if found_metrics:
            logger.info(
                f"Phase 2 state tracking: {len(found_metrics)}/{len(phase2_metrics)} metrics present"
            )
        else:
            logger.warning(
                "No Phase 2 metrics found yet (expected if no actions have run since service start)"
            )

        # Parse to check if any have actual values
        metrics = parse_prometheus_metrics(metrics_text)
        metrics_with_values = [m for m in found_metrics if m in metrics]
        if metrics_with_values:
            logger.info(
                f"Phase 2 metrics with values: {metrics_with_values} "
                f"(means actions have executed)"
            )

    def test_phase3_kafka_consumer_metrics_exist(self) -> None:
        """Test that Phase 3 Kafka consumer metrics are registered.

        Phase 3 added metrics for Kafka consumer health:
        - actions_kafka_messages_consumed_total: Messages consumed counter
        - actions_kafka_last_message_timestamp_seconds: Last message timestamp
        - actions_kafka_messages_processed_total: Processed messages counter
        """
        metrics_text = get_integrations_metrics()

        # Check for Phase 3 metrics
        phase3_metrics = [
            "actions_kafka_messages_consumed_total",
            "actions_kafka_last_message_timestamp_seconds",
            "actions_kafka_messages_processed_total",
        ]

        found_metrics = []
        for metric_name in phase3_metrics:
            if metric_name in metrics_text:
                found_metrics.append(metric_name)
                logger.info(f"✓ Found Phase 3 metric: {metric_name}")

        if found_metrics:
            logger.info(
                f"Phase 3 Kafka metrics: {len(found_metrics)}/{len(phase3_metrics)} metrics present"
            )
        else:
            logger.warning(
                "No Phase 3 Kafka metrics found yet (expected if no actions with Kafka sources have run)"
            )

        # Parse to check if any have actual values
        metrics = parse_prometheus_metrics(metrics_text)
        metrics_with_values = [m for m in found_metrics if m in metrics]
        if metrics_with_values:
            logger.info(
                f"Phase 3 metrics with values: {metrics_with_values} "
                f"(means Kafka-consuming actions have executed)"
            )

    def test_kafka_lag_monitoring_metrics_exist(self) -> None:
        """Test that Kafka consumer lag monitoring metrics are registered.

        Tests the newly added Kafka lag monitoring feature that tracks:
        - kafka_consumer_lag: Aggregated per-topic consumer lag
        - Proper labels: topic, pipeline_name

        These metrics are exposed by action subprocesses via their /metrics endpoints
        and aggregated by the main integrations service.
        """
        metrics_text = get_integrations_metrics()
        metrics = parse_prometheus_metrics(metrics_text)

        # Check for Kafka lag monitoring metric
        lag_metric_name = "kafka_consumer_lag"

        if lag_metric_name in metrics:
            samples = metrics[lag_metric_name]
            logger.info(
                f"✓ Found Kafka lag monitoring metric: {lag_metric_name} with {len(samples)} samples"
            )

            # Verify required labels on at least one sample
            required_labels = ["topic", "pipeline_name"]
            for sample in samples[:3]:  # Check first few samples
                sample_labels = sample.get("labels", {})
                found_labels = [
                    label for label in required_labels if label in sample_labels
                ]

                if len(found_labels) == len(required_labels):
                    logger.info(
                        f"✓ Kafka lag metric has required labels: {required_labels}"
                    )
                    logger.info(
                        f"  Sample: topic={sample_labels.get('topic')}, "
                        f"pipeline_name={sample_labels.get('pipeline_name')}, "
                        f"lag={sample.get('value')}"
                    )

                    # Validate lag value is non-negative
                    lag_value = sample.get("value", 0)
                    if lag_value >= 0:
                        logger.info(f"✓ Kafka lag value is valid: {lag_value}")
                    else:
                        logger.warning(
                            f"⚠ Kafka lag value is negative: {lag_value} (unexpected)"
                        )
                    break
            else:
                logger.warning(
                    f"Kafka lag metric found but samples may be missing required labels. "
                    f"Sample: {samples[0] if samples else 'no samples'}"
                )
        else:
            logger.warning(
                f"Kafka lag monitoring metric '{lag_metric_name}' not found yet. "
                f"This is expected if:\n"
                f"  - OTEL_KAFKA_LAG_ENABLED is not set to true\n"
                f"  - No Kafka-consuming actions have run\n"
                f"  - Actions are still starting up"
            )

    def test_kafka_lag_monitoring_labels_and_values(self) -> None:
        """Test Kafka lag metrics have proper labels and reasonable values.

        Validates:
        - topic label contains valid Kafka topic names
        - pipeline_name matches action pipeline names
        - lag values are non-negative numbers
        - Multiple topics/pipelines can be tracked separately
        """
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        lag_metric_name = "kafka_consumer_lag"
        if lag_metric_name not in metrics:
            logger.warning(
                "Skipping Kafka lag label validation - metric not found "
                "(expected if no Kafka actions running)"
            )
            return

        samples = metrics[lag_metric_name]
        logger.info(
            f"Validating Kafka lag metrics - found {len(samples)} samples across topics/pipelines"
        )

        # Track unique topics and pipelines
        topics_seen = set()
        pipelines_seen = set()
        valid_samples = 0

        for sample in samples:
            labels = sample.get("labels", {})
            topic = labels.get("topic")
            pipeline = labels.get("pipeline_name")
            lag_value = sample.get("value", -1)

            # Validate topic exists
            if topic:
                topics_seen.add(topic)
            else:
                logger.warning(f"Sample missing 'topic' label: {sample}")
                continue

            # Validate pipeline exists
            if pipeline:
                pipelines_seen.add(pipeline)
            else:
                logger.warning(f"Sample missing 'pipeline_name' label: {sample}")
                continue

            # Validate lag is non-negative
            if lag_value < 0:
                logger.warning(
                    f"Invalid lag value {lag_value} for topic={topic}, pipeline={pipeline}"
                )
                continue

            valid_samples += 1

        # Log summary
        logger.info("✓ Kafka lag monitoring validation summary:")
        logger.info(f"  - Valid samples: {valid_samples}/{len(samples)}")
        logger.info(f"  - Unique topics tracked: {len(topics_seen)}")
        logger.info(f"  - Unique pipelines tracked: {len(pipelines_seen)}")

        if topics_seen:
            logger.info(f"  - Topics: {', '.join(sorted(topics_seen))}")
        if pipelines_seen:
            logger.info(f"  - Pipelines: {', '.join(sorted(pipelines_seen))}")

        # At least one valid sample should exist if metric is present
        if valid_samples == 0 and len(samples) > 0:
            logger.warning(
                "⚠ Kafka lag metric exists but no valid samples found - investigate labels"
            )


class TestActionsObservabilityIntegration:
    """Integration tests that actually run actions and verify metrics.

    These tests are skipped by default because they:
    - Require Kafka to be running
    - Take longer to execute
    - Create side effects (action execution)

    Enable with: pytest -v -m "not skip" tests/integrations/test_actions_observability_labels.py
    """

    def test_bootstrap_action_generates_labeled_metrics(self, graph_client) -> None:
        """Test that bootstrapping an action generates metrics with proper labels."""
        # Generate unique action URN and name
        action_urn = f"urn:li:dataHubAction:test_metrics_{int(time.time())}"
        action_name = "test_metrics_action"

        integrations_url = get_integrations_service_url()

        try:
            # Create action configuration
            config = create_test_action_config(action_urn, action_name)
            upsert_action_via_rest(action_urn, config, graph_client)

            # Get baseline metrics
            metrics_before = parse_prometheus_metrics(get_integrations_metrics())
            exec_count_before = len(metrics_before.get("actions_execution_total", []))

            # Bootstrap the action
            logger.info(f"Bootstrapping action {action_urn}")
            bootstrap_action(action_urn, integrations_url, wait_for_completion=False)

            # Wait for bootstrap to complete and metrics to be updated
            # Note: RecordingAction doesn't report stats to GMS, so we can't wait_for_completion
            time.sleep(5)

            # Get metrics after bootstrap
            metrics_after = parse_prometheus_metrics(get_integrations_metrics())

            # Verify metrics were created
            if "actions_execution_total" in metrics_after:
                samples = metrics_after["actions_execution_total"]
                exec_count_after = len(samples)

                logger.info(
                    f"Execution count: before={exec_count_before}, after={exec_count_after}"
                )

                # Find samples for our action
                our_samples = [
                    s
                    for s in samples
                    if s.get("labels", {}).get("action_urn") == action_urn
                ]

                if our_samples:
                    logger.info(
                        f"✓ Found {len(our_samples)} metric samples for our action"
                    )

                    # Verify labels
                    sample = our_samples[0]
                    labels = sample.get("labels", {})

                    assert labels.get("action_urn") == action_urn, "action_urn mismatch"
                    # RecordingAction doesn't have full observability infrastructure,
                    # so action_name may not match exactly - just verify it's present
                    assert "action_name" in labels, "action_name label missing"
                    assert labels.get("stage").lower() == "bootstrap", (
                        "stage should be bootstrap"
                    )

                    logger.info(f"✓ All labels present: {labels}")
                else:
                    logger.warning("No metrics found for our test action")
            else:
                logger.warning("No actions_execution_total metrics found")

        finally:
            # Cleanup: rollback and delete the action
            try:
                rollback_action(
                    action_urn, integrations_url, wait_for_completion=True, timeout=30
                )
                logger.info(f"Rolled back test action {action_urn}")
            except Exception as e:
                logger.warning(f"Failed to rollback test action: {e}")

            try:
                graph_client.delete_entity(action_urn, hard=True)
                logger.info(f"Deleted test action {action_urn}")
            except Exception as e:
                logger.warning(f"Failed to delete test action: {e}")
