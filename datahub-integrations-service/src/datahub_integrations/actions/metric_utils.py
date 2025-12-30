"""Utility functions for action metrics and observability."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datahub_integrations.actions.stats_util import Stage
from datahub_integrations.observability.config import ObservabilityConfig

if TYPE_CHECKING:
    from datahub_integrations.actions.actions_manager import LiveActionSpec


def get_kafka_lag_monitoring_env_vars(config: ObservabilityConfig) -> dict[str, str]:
    """Get Kafka lag monitoring environment variables from observability config.

    These environment variables configure Kafka consumer lag monitoring in action
    subprocesses. The lag monitor runs as a background daemon thread and reports
    metrics via the subprocess /metrics endpoint.
    """
    env_vars = {}

    if config.kafka_lag_enabled:
        env_vars["DATAHUB_ACTIONS_KAFKA_LAG_ENABLED"] = "true"
        env_vars["DATAHUB_ACTIONS_KAFKA_LAG_INTERVAL_SECONDS"] = str(
            config.kafka_lag_interval_seconds
        )
        env_vars["DATAHUB_ACTIONS_KAFKA_LAG_TIMEOUT_SECONDS"] = str(
            config.kafka_lag_timeout_seconds
        )
    else:
        env_vars["DATAHUB_ACTIONS_KAFKA_LAG_ENABLED"] = "false"

    return env_vars


def extract_action_metadata(action_spec: LiveActionSpec) -> dict[str, str]:
    """Extract action metadata labels from the action spec for observability."""
    config = action_spec.action_run.unresolved_config
    action_name = config.get("name", "unknown")
    action_type = config.get("action", {}).get("type", "unknown")

    # Extract just the class name from the full Python path
    # e.g., "datahub_integrations.propagation.doc.doc_propagation_action.DocPropagationAction" -> "DocPropagationAction"
    if "." in action_type:
        action_type = action_type.split(".")[-1]

    return {
        "action_urn": action_spec.urn,
        "action_name": action_name,
        "action_type": action_type,
    }


def make_stage_label_extractors() -> dict:
    """Create label extractors for pipeline stage metrics."""

    def extract_action_urn(
        result: Any, self: Any, stage: Stage, action_spec: LiveActionSpec, **kwargs: Any
    ) -> str:
        return extract_action_metadata(action_spec)["action_urn"]

    def extract_action_name(
        result: Any, self: Any, stage: Stage, action_spec: LiveActionSpec, **kwargs: Any
    ) -> str:
        return extract_action_metadata(action_spec)["action_name"]

    def extract_action_type(
        result: Any, self: Any, stage: Stage, action_spec: LiveActionSpec, **kwargs: Any
    ) -> str:
        return extract_action_metadata(action_spec)["action_type"]

    def extract_stage(
        result: Any, self: Any, stage: Stage, action_spec: LiveActionSpec, **kwargs: Any
    ) -> str:
        return stage.value

    def extract_status(
        result: Any, self: Any, stage: Stage, action_spec: LiveActionSpec, **kwargs: Any
    ) -> str:
        # Import here to avoid circular dependency
        from datahub_integrations.actions.actions_manager import ActionStatus

        if action_spec.action_run.status == ActionStatus.SUCCEEDED:
            return "success"
        elif action_spec.action_run.status == ActionStatus.FAILED:
            return "failure"
        else:
            return "unknown"

    return {
        "action_urn": extract_action_urn,
        "action_name": extract_action_name,
        "action_type": extract_action_type,
        "stage": extract_stage,
        "status": extract_status,
    }


def make_venv_label_extractors() -> dict:
    """Create label extractors for venv setup metrics."""

    def extract_action_urn(result, self, urn: str, config: dict, **kwargs) -> str:
        return urn

    def extract_action_name(result, self, urn: str, config: dict, **kwargs) -> str:
        return config.get("name", "unknown")

    def extract_action_type(result, self, urn: str, config: dict, **kwargs) -> str:
        action_type = config.get("action", {}).get("type", "unknown")
        if "." in action_type:
            action_type = action_type.split(".")[-1]
        return action_type

    return {
        "action_urn": extract_action_urn,
        "action_name": extract_action_name,
        "action_type": extract_action_type,
    }
