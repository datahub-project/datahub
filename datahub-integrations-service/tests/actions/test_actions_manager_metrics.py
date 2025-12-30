"""Unit tests for actions_manager observability label extractors."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from datahub_integrations.actions.actions_manager import (
    ActionRun,
    ActionStatus,
    LiveActionSpec,
)
from datahub_integrations.actions.metric_utils import (
    extract_action_metadata,
    make_stage_label_extractors,
    make_venv_label_extractors,
)
from datahub_integrations.actions.stats_util import Stage


@pytest.fixture
def sample_config():
    """Sample action configuration."""
    return {
        "name": "test_action",
        "source": {"type": "kafka", "config": {}},
        "action": {
            "type": "datahub_integrations.propagation.doc.doc_propagation_action.DocPropagationAction",
            "config": {},
        },
    }


@pytest.fixture
def action_run(sample_config):
    """Create a sample ActionRun."""
    logs_mock = MagicMock()
    return ActionRun(
        urn="urn:li:dataHubAction:test",
        unresolved_config=sample_config,
        executor_id="default",
        logs=logs_mock,
        started_at=datetime.now(tz=timezone.utc),
        status=ActionStatus.INIT,
    )


@pytest.fixture
def live_action_spec(action_run):
    """Create a sample LiveActionSpec."""
    runner_mock = MagicMock()
    venv_mock = MagicMock()
    return LiveActionSpec(
        action_run=action_run,
        runner=runner_mock,
        venv=venv_mock,
        port=10000,
    )


def test_extract_action_metadata(live_action_spec):
    """Test action metadata extraction from LiveActionSpec."""
    metadata = extract_action_metadata(live_action_spec)

    assert metadata["action_urn"] == "urn:li:dataHubAction:test"
    assert metadata["action_name"] == "test_action"
    assert metadata["action_type"] == "DocPropagationAction"


def test_extract_action_metadata_extracts_class_name(sample_config):
    """Test that action_type extracts just the class name from full path."""
    logs_mock = MagicMock()
    action_run = ActionRun(
        urn="urn:li:dataHubAction:test",
        unresolved_config=sample_config,
        executor_id="default",
        logs=logs_mock,
    )
    runner_mock = MagicMock()
    venv_mock = MagicMock()
    spec = LiveActionSpec(
        action_run=action_run,
        runner=runner_mock,
        venv=venv_mock,
        port=10000,
    )

    metadata = extract_action_metadata(spec)

    # Should extract just "DocPropagationAction" from the full path
    assert metadata["action_type"] == "DocPropagationAction", (
        "Should extract just class name"
    )
    assert "." not in metadata["action_type"], "Should not contain dots"


def test_extract_action_metadata_handles_missing_fields():
    """Test action metadata extraction with missing config fields."""
    logs_mock = MagicMock()
    action_run = ActionRun(
        urn="urn:li:dataHubAction:test",
        unresolved_config={},  # Empty config
        executor_id="default",
        logs=logs_mock,
    )
    runner_mock = MagicMock()
    venv_mock = MagicMock()
    spec = LiveActionSpec(
        action_run=action_run,
        runner=runner_mock,
        venv=venv_mock,
        port=10000,
    )

    metadata = extract_action_metadata(spec)

    assert metadata["action_urn"] == "urn:li:dataHubAction:test"
    assert metadata["action_name"] == "unknown"
    assert metadata["action_type"] == "unknown"


def test_stage_label_extractors_action_urn(live_action_spec):
    """Test stage label extractors - action_urn."""
    extractors = make_stage_label_extractors()

    action_urn = extractors["action_urn"](
        result=None,
        self=None,
        stage=Stage.LIVE,
        action_spec=live_action_spec,
    )

    assert action_urn == "urn:li:dataHubAction:test"


def test_stage_label_extractors_action_name(live_action_spec):
    """Test stage label extractors - action_name."""
    extractors = make_stage_label_extractors()

    action_name = extractors["action_name"](
        result=None,
        self=None,
        stage=Stage.LIVE,
        action_spec=live_action_spec,
    )

    assert action_name == "test_action"


def test_stage_label_extractors_action_type(live_action_spec):
    """Test stage label extractors - action_type."""
    extractors = make_stage_label_extractors()

    action_type = extractors["action_type"](
        result=None,
        self=None,
        stage=Stage.LIVE,
        action_spec=live_action_spec,
    )

    assert action_type == "DocPropagationAction"


def test_stage_label_extractors_stage(live_action_spec):
    """Test stage label extractors - stage."""
    extractors = make_stage_label_extractors()

    # Test all stage values
    for stage in [Stage.LIVE, Stage.BOOTSTRAP, Stage.ROLLBACK]:
        stage_value = extractors["stage"](
            result=None,
            self=None,
            stage=stage,
            action_spec=live_action_spec,
        )
        assert stage_value == stage.value


def test_stage_label_extractors_status_success(live_action_spec):
    """Test stage label extractors - status success."""
    live_action_spec.action_run.status = ActionStatus.SUCCEEDED
    extractors = make_stage_label_extractors()

    status = extractors["status"](
        result=None,
        self=None,
        stage=Stage.LIVE,
        action_spec=live_action_spec,
    )

    assert status == "success"


def test_stage_label_extractors_status_failure(live_action_spec):
    """Test stage label extractors - status failure."""
    live_action_spec.action_run.status = ActionStatus.FAILED
    extractors = make_stage_label_extractors()

    status = extractors["status"](
        result=None,
        self=None,
        stage=Stage.LIVE,
        action_spec=live_action_spec,
    )

    assert status == "failure"


def test_stage_label_extractors_status_unknown(live_action_spec):
    """Test stage label extractors - status unknown."""
    live_action_spec.action_run.status = ActionStatus.INIT
    extractors = make_stage_label_extractors()

    status = extractors["status"](
        result=None,
        self=None,
        stage=Stage.LIVE,
        action_spec=live_action_spec,
    )

    assert status == "unknown"


def test_venv_label_extractors_action_urn(sample_config):
    """Test venv label extractors - action_urn."""
    extractors = make_venv_label_extractors()

    action_urn = extractors["action_urn"](
        result=None,
        self=None,
        urn="urn:li:dataHubAction:test",
        config=sample_config,
    )

    assert action_urn == "urn:li:dataHubAction:test"


def test_venv_label_extractors_action_name(sample_config):
    """Test venv label extractors - action_name."""
    extractors = make_venv_label_extractors()

    action_name = extractors["action_name"](
        result=None,
        self=None,
        urn="urn:li:dataHubAction:test",
        config=sample_config,
    )

    assert action_name == "test_action"


def test_venv_label_extractors_action_type(sample_config):
    """Test venv label extractors - action_type."""
    extractors = make_venv_label_extractors()

    action_type = extractors["action_type"](
        result=None,
        self=None,
        urn="urn:li:dataHubAction:test",
        config=sample_config,
    )

    assert action_type == "DocPropagationAction"


def test_venv_label_extractors_handles_missing_fields():
    """Test venv label extractors with missing config fields."""
    extractors = make_venv_label_extractors()

    action_name = extractors["action_name"](
        result=None,
        self=None,
        urn="urn:li:dataHubAction:test",
        config={},  # Empty config
    )
    action_type = extractors["action_type"](
        result=None,
        self=None,
        urn="urn:li:dataHubAction:test",
        config={},  # Empty config
    )

    assert action_name == "unknown"
    assert action_type == "unknown"
