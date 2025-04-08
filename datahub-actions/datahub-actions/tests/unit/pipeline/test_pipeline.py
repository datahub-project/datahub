# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os

import pytest
from pydantic import ValidationError

from datahub_actions.pipeline.pipeline import Pipeline, PipelineException
from datahub_actions.pipeline.pipeline_config import FailureMode
from datahub_actions.plugin.transform.filter.filter_transformer import FilterTransformer
from tests.unit.test_helpers import TestAction, TestEventSource, TestTransformer


def test_create():
    # Test successfully pipeline creation.
    valid_config = _build_valid_pipeline_config()
    valid_pipeline = Pipeline.create(valid_config)

    # Validate Pipeline is initialized
    assert valid_pipeline.name is not None
    assert valid_pipeline.source is not None
    assert isinstance(valid_pipeline.source, TestEventSource)
    assert valid_pipeline.transforms is not None
    assert len(valid_pipeline.transforms) == 2  # Filter + Custom
    assert isinstance(valid_pipeline.transforms[0], FilterTransformer)
    assert isinstance(valid_pipeline.transforms[1], TestTransformer)
    assert valid_pipeline.action is not None
    assert isinstance(valid_pipeline.action, TestAction)
    assert valid_pipeline._shutdown is False
    assert valid_pipeline._stats is not None
    assert valid_pipeline._retry_count == 3
    assert valid_pipeline._failure_mode == FailureMode.CONTINUE
    assert valid_pipeline._failed_events_dir == "/tmp/datahub/test"

    # Test invalid pipeline creation.
    invalid_config = _build_invalid_pipeline_config()
    with pytest.raises(ValidationError, match="name"):
        Pipeline.create(invalid_config)


def test_run():
    valid_config = _build_basic_pipeline_config()
    valid_pipeline = Pipeline.create(valid_config)

    # Run the pipeline
    valid_pipeline.run()

    # Confirm that the pipeline has run as expected.
    assert valid_pipeline._shutdown is False

    # Verify pipeline statistics
    assert valid_pipeline.stats().failed_event_count == 0
    assert valid_pipeline.stats().failed_ack_count == 0
    assert valid_pipeline.stats().success_count == 3
    assert (
        valid_pipeline.stats()
        .get_transformer_stats(valid_pipeline.transforms[0])
        .exception_count
        == 0
    )
    assert (
        valid_pipeline.stats()
        .get_transformer_stats(valid_pipeline.transforms[0])
        .processed_count
        == 3
    )
    assert (
        valid_pipeline.stats()
        .get_transformer_stats(valid_pipeline.transforms[0])
        .filtered_count
        == 0
    )
    assert valid_pipeline.stats().action_stats.exception_count == 0
    assert valid_pipeline.stats().action_stats.success_count == 3

    # Verify that the test action processed the correct events (via counters)
    assert valid_pipeline.action.total_event_count == 3  # type: ignore
    assert valid_pipeline.action.mcl_count == 1  # type: ignore
    assert valid_pipeline.action.ece_count == 1  # type: ignore
    assert valid_pipeline.action.skipped_count == 1  # type: ignore
    assert (
        valid_pipeline.action.smiley_count == 3  # type: ignore
    )  # Confirms that events were transformed.

    # Verify that the event source received ack calls on all events
    assert valid_pipeline.source.ack_count == 3  # type: ignore


def test_stop():
    # Configure a pipeline with a long-running event source
    stoppable_pipeline_config = _build_stoppable_pipeline_config()
    stopable_pipeline = Pipeline.create(stoppable_pipeline_config)

    # Start in async mode.
    # TODO: This test should be rewritten to use async correctly, needs type ignore
    stopable_pipeline.start()  # type: ignore

    # Stop the pipeline.
    stopable_pipeline.stop()

    # Verify that the pipeline has stopped
    assert stopable_pipeline._shutdown is True


def test_failed_events_continue_mode():
    # First, test a transformer that throws on invocation.
    throwing_transformer_config = _build_throwing_transformer_pipeline_config(
        failure_mode="CONTINUE"
    )
    throwing_transformer_pipeline = Pipeline.create(throwing_transformer_config)
    throwing_transformer_pipeline.run()
    # Ensure that the message was acked.
    assert throwing_transformer_pipeline.source.ack_count == 3  # type: ignore

    # Next, test an action that throws on invocation.
    throwing_action_config = _build_throwing_action_pipeline_config(
        failure_mode="CONTINUE"
    )
    throwing_action_pipeline = Pipeline.create(throwing_action_config)
    throwing_action_pipeline.run()
    # Ensure that the message was acked.
    assert throwing_action_pipeline.source.ack_count == 3  # type: ignore


def test_failed_events_throw_mode():
    # First, test a transformer that throws on invocation.
    throwing_transformer_config = _build_throwing_transformer_pipeline_config(
        failure_mode="THROW"
    )
    throwing_transformer_pipeline = Pipeline.create(throwing_transformer_config)
    with pytest.raises(
        PipelineException, match="Failed to process event after maximum retries"
    ):
        throwing_transformer_pipeline.run()
    # Ensure that the message was NOT acked.
    assert throwing_transformer_pipeline.source.ack_count == 0  # type: ignore

    # Next, test an action that throws on invocation.
    throwing_action_config = _build_throwing_action_pipeline_config(
        failure_mode="THROW"
    )
    throwing_action_pipeline = Pipeline.create(throwing_action_config)
    with pytest.raises(
        PipelineException, match="Failed to process event after maximum retries"
    ):
        throwing_action_pipeline.run()
    # Ensure that the message was NOT acked.
    assert throwing_action_pipeline.source.ack_count == 0  # type: ignore


# Test Dead Letter Queue
def test_failed_events_file():
    failed_events_file_path = (
        "/tmp/datahub/test/test_failed_events_file/failed_events.log"
    )
    try:
        os.remove(failed_events_file_path)
    except OSError:
        pass

    throwing_action_config = _build_throwing_action_pipeline_config(
        pipeline_name="test_failed_events_file", failure_mode="CONTINUE"
    )
    throwing_action_pipeline = Pipeline.create(throwing_action_config)
    throwing_action_pipeline.run()

    # Ensure that the file was written, and ensure that the first line is equivalent to the serialized
    # version of the EnvelopedEvent.
    index = 0
    for line in open(failed_events_file_path, "r").readlines():
        # Simply verify the event can be loaded.
        assert json.loads(line.strip())
        index = index + 1
    os.remove(failed_events_file_path)


def _build_valid_pipeline_config() -> dict:
    return {
        "name": "sample-pipeline",
        "source": {"type": "test_source", "config": {}},
        "filter": {"event_type": "MetadataChangeLogEvent_v1"},
        "transform": [{"type": "test_transformer", "config": {"config1": "value1"}}],
        "action": {"type": "test_action", "config": {"config1": "value1"}},
        "options": {
            "retry_count": 3,
            "failure_mode": "CONTINUE",
            "failed_events_dir": "/tmp/datahub/test",
        },
    }


def _build_basic_pipeline_config() -> dict:
    return {
        "name": "sample-pipeline",
        "source": {"type": "test_source", "config": {}},
        "transform": [{"type": "test_transformer", "config": {"config1": "value1"}}],
        "action": {"type": "test_action", "config": {"config1": "value1"}},
        "options": {
            "retry_count": 3,
            "failure_mode": "CONTINUE",
            "failed_events_dir": "/tmp/datahub/test",
        },
    }


def _build_stoppable_pipeline_config() -> dict:
    return {
        "name": "stoppable-pipeline",
        "source": {"type": "stoppable_event_source", "config": {}},
        "transform": [{"type": "test_transformer", "config": {"config1": "value1"}}],
        "action": {"type": "test_action", "config": {"config1": "value1"}},
        "options": {
            "retry_count": 3,
            "failure_mode": "CONTINUE",
            "failed_events_dir": "/tmp/datahub/test",
        },
    }


def _build_throwing_transformer_pipeline_config(failure_mode: str = "CONTINUE") -> dict:
    return {
        "name": "sample-pipeline",
        "source": {"type": "test_source", "config": {}},
        "transform": [
            {"type": "throwing_test_transformer", "config": {"config1": "value1"}}
        ],
        "action": {"type": "test_action", "config": {"config1": "value1"}},
        "options": {
            "retry_count": 3,
            "failure_mode": failure_mode,
            "failed_events_dir": "/tmp/datahub/test",
        },
    }


def _build_throwing_action_pipeline_config(
    pipeline_name: str = "throwing-action-pipeline", failure_mode: str = "CONTINUE"
) -> dict:
    return {
        "name": pipeline_name,
        "source": {"type": "test_source", "config": {}},
        "transform": [{"type": "test_transformer", "config": {"config1": "value1"}}],
        "action": {"type": "throwing_test_action", "config": {"config1": "value1"}},
        "options": {
            "retry_count": 3,
            "failure_mode": failure_mode,
            "failed_events_dir": "/tmp/datahub/test",
        },
    }


def _build_invalid_pipeline_config() -> dict:
    # No name field
    return {
        "source": {"type": "test_source", "config": {"to_upper": False}},
        "action": {"type": "test_action", "config": {"config1": "value1"}},
    }
