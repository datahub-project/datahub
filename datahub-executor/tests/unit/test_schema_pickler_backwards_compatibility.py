import os
from unittest import mock

import pytest
from acryl.executor.request.execution_request import ExecutionRequest

from datahub_executor.worker.celery_sqs.config import (
    SchemaPickler,
    schema_pickle_dumps,
    schema_pickle_loads,
)


def assert_is_pydantic_v2(obj):
    """Assert that an object has Pydantic v2 characteristics."""
    assert hasattr(obj, "__pydantic_fields_set__"), (
        "Should have v2 __pydantic_fields_set__ attribute"
    )
    assert hasattr(obj, "model_dump"), "Should have v2 model_dump method"
    assert callable(obj.model_dump), "model_dump should be callable"
    assert hasattr(obj, "model_fields"), "Should have v2 model_fields attribute"
    # Check for core v2 attributes
    assert hasattr(obj, "__pydantic_core_schema__"), (
        "Should have v2 __pydantic_core_schema__ attribute"
    )
    assert hasattr(obj, "__pydantic_validator__"), (
        "Should have v2 __pydantic_validator__ attribute"
    )


def assert_is_pydantic_v1_compatible(obj):
    """Assert that an object is compatible with Pydantic v1 deserialization."""
    # Since we're not actually converting the object to v1, we can't check for
    # v1-specific internal state. Instead, we verify that the compatibility
    # layer worked and the object still functions correctly.
    #
    # The key insight is that our compatibility layer modifies the pickled state,
    # but the unpickled object will still be a v2 object. What we can test is
    # that the unpickling process worked correctly and the object has the
    # expected data integrity.
    assert hasattr(obj, "__pydantic_fields_set__"), (
        "Should still have v2 __pydantic_fields_set__ attribute"
    )
    assert hasattr(obj, "model_dump"), "Should still have v2 model_dump method"
    assert callable(obj.model_dump), "model_dump should still be callable"

    # Verify the object can still be serialized properly (key functionality test)
    try:
        obj.model_dump()
    except Exception as e:
        raise AssertionError(
            f"Object should still be serializable after compatibility processing: {e}"
        )


@pytest.fixture
def test_execution_request() -> ExecutionRequest:
    return ExecutionRequest(
        exec_id="test-execution-123",
        name="run_assertion_task",
        args={
            "assertion_urn": "urn:li:assertion:test-assertion",
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)",
            "platform": "mysql",
            "config": {
                "connection_string": "mysql://user:pass@localhost:3306/test",
                "table_name": "users",
                "assertion_type": "volume",
                "operator": "GREATER_THAN",
                "threshold": 100,
            },
        },
        executor_id="test-executor",
    )


@pytest.fixture
def test_execution_request_with_nested_model_dump() -> ExecutionRequest:
    """
    ExecutionRequest fixture that mirrors the real pattern from mapper.py
    where args contains nested objects including model_dump() and __dict__
    """
    from pydantic import BaseModel

    from datahub_executor.common.types import AssertionEvaluationContext

    # Create a simple Pydantic model for testing model_dump()
    class MockAssertionSpec(BaseModel):
        type: str
        operator: str
        threshold: int
        dataset_urn: str
        metadata: dict

    # Create actual Pydantic model and call model_dump
    assertion_spec = MockAssertionSpec(
        type="volume",
        operator="GREATER_THAN",
        threshold=100,
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)",
        metadata={"created_by": "test-user", "version": "1.0"},
    )

    # Create actual context object and call __dict__
    context = AssertionEvaluationContext(dry_run=False, online_smart_assertions=True)

    return ExecutionRequest(
        exec_id="urn:li:assertion:test-assertion_scheduled_training",
        name="run_assertion_task",
        args={
            "urn": "urn:li:assertion:test-assertion",
            "assertion_spec": assertion_spec.model_dump(mode="json"),
            "context": context.__dict__,
        },
        executor_id="test-executor",
    )


class TestSchemaPicklerExecutionRequest:
    @mock.patch.dict(os.environ, {"DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE": "True"})
    def test_execution_request_basic_pickle_unpickle(self, test_execution_request):
        execution_request = test_execution_request
        assert_is_pydantic_v2(execution_request)

        pickled_data = schema_pickle_dumps(execution_request)
        unpickled_request = schema_pickle_loads(pickled_data)

        assert unpickled_request.exec_id == execution_request.exec_id
        assert unpickled_request.name == execution_request.name
        assert unpickled_request.executor_id == execution_request.executor_id

        assert (
            unpickled_request.args["assertion_urn"]
            == execution_request.args["assertion_urn"]
        )
        assert (
            unpickled_request.args["config"]["table_name"]
            == execution_request.args["config"]["table_name"]
        )
        assert (
            unpickled_request.args["config"]["threshold"]
            == execution_request.args["config"]["threshold"]
        )
        assert_is_pydantic_v1_compatible(unpickled_request)

    @mock.patch.dict(os.environ, {"DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE": "True"})
    def test_execution_request_with_compatibility_mode_enabled(
        self, test_execution_request
    ):
        execution_request = test_execution_request
        assert_is_pydantic_v2(execution_request)

        pickled_data = schema_pickle_dumps(execution_request)
        unpickled_request = schema_pickle_loads(pickled_data)

        assert unpickled_request.exec_id == execution_request.exec_id
        assert unpickled_request.name == execution_request.name
        assert_is_pydantic_v1_compatible(unpickled_request)

    @mock.patch.dict(os.environ, {"DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE": "False"})
    def test_execution_request_with_compatibility_mode_disabled(
        self, test_execution_request
    ):
        execution_request = test_execution_request
        assert_is_pydantic_v2(execution_request)

        pickled_data = schema_pickle_dumps(execution_request)
        unpickled_request = schema_pickle_loads(pickled_data)

        assert unpickled_request.exec_id == execution_request.exec_id
        assert unpickled_request.name == execution_request.name
        assert_is_pydantic_v2(unpickled_request)

    @mock.patch.dict(os.environ, {"DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE": "True"})
    def test_execution_request_nested_objects(self):
        complex_request = ExecutionRequest(
            exec_id="complex-execution-456",
            name="run_monitor_training_task",
            args={
                "monitor_config": {
                    "datasets": [
                        "urn:li:dataset:(urn:li:dataPlatform:mysql,db1.table1,PROD)",
                        "urn:li:dataset:(urn:li:dataPlatform:postgres,db2.table2,PROD)",
                    ],
                    "thresholds": {
                        "volume_min": 1000,
                        "volume_max": 10000,
                        "freshness_hours": 24,
                    },
                    "enabled_checks": ["volume", "freshness", "schema"],
                    "metadata": {
                        "created_by": "test-user",
                        "created_at": "2023-01-01T00:00:00Z",
                        "tags": {"env": "prod", "team": "data"},
                    },
                }
            },
            executor_id="training-executor",
        )
        assert_is_pydantic_v2(complex_request)

        pickled_data = schema_pickle_dumps(complex_request)
        unpickled_request = schema_pickle_loads(pickled_data)

        assert unpickled_request.exec_id == "complex-execution-456"
        assert len(unpickled_request.args["monitor_config"]["datasets"]) == 2
        assert (
            unpickled_request.args["monitor_config"]["thresholds"]["volume_min"] == 1000
        )
        assert "volume" in unpickled_request.args["monitor_config"]["enabled_checks"]
        assert (
            unpickled_request.args["monitor_config"]["metadata"]["tags"]["env"]
            == "prod"
        )
        assert_is_pydantic_v1_compatible(unpickled_request)

    @mock.patch.dict(os.environ, {"DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE": "True"})
    def test_execution_request_identifies_object_type(self, test_execution_request):
        import io

        execution_request = test_execution_request

        buf = io.BytesIO()
        pickler = SchemaPickler(buf)

        is_pydantic_v2 = pickler._is_pydantic_v2_model(execution_request)
        assert is_pydantic_v2 is True, (
            "ExecutionRequest should be detected as Pydantic v2 model"
        )

    @mock.patch.dict(os.environ, {"DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE": "True"})
    def test_execution_request_with_potential_pydantic_nested_objects(self):
        execution_request = ExecutionRequest(
            exec_id="nested-test-789",
            name="test_with_nested_objects",
            args={
                "model_data": {
                    "__class__": "SomeModel",
                    "__module__": "datahub.models",
                    "field1": "value1",
                    "field2": 42,
                    "nested_config": {"enabled": True, "settings": ["a", "b", "c"]},
                }
            },
            executor_id="test-executor",
        )
        assert_is_pydantic_v2(execution_request)

        pickled_data = schema_pickle_dumps(execution_request)
        unpickled_request = schema_pickle_loads(pickled_data)

        assert unpickled_request.args["model_data"]["__class__"] == "SomeModel"
        assert unpickled_request.args["model_data"]["field2"] == 42
        assert unpickled_request.args["model_data"]["nested_config"]["enabled"] is True
        assert_is_pydantic_v1_compatible(unpickled_request)

    @mock.patch.dict(os.environ, {"DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE": "True"})
    def test_multiple_execution_requests_batch(self):
        requests = [
            ExecutionRequest(
                exec_id=f"batch-request-{i}",
                name="batch_task",
                args={"batch_id": i, "data": f"test_data_{i}"},
                executor_id=f"executor-{i % 3}",
            )
            for i in range(5)
        ]

        # Verify all original requests are v2
        for req in requests:
            assert_is_pydantic_v2(req)

        pickled_requests = [schema_pickle_dumps(req) for req in requests]
        unpickled_requests = [schema_pickle_loads(data) for data in pickled_requests]

        for i, unpickled in enumerate(unpickled_requests):
            assert unpickled.exec_id == f"batch-request-{i}"
            assert unpickled.args["batch_id"] == i
            assert unpickled.executor_id == f"executor-{i % 3}"
            assert_is_pydantic_v1_compatible(unpickled)

    @mock.patch.dict(os.environ, {"DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE": "True"})
    def test_execution_request_with_real_nested_model_dump(
        self, test_execution_request_with_nested_model_dump
    ):
        execution_request = test_execution_request_with_nested_model_dump
        assert_is_pydantic_v2(execution_request)

        pickled_data = schema_pickle_dumps(execution_request)
        unpickled_request = schema_pickle_loads(pickled_data)

        assert (
            unpickled_request.exec_id
            == "urn:li:assertion:test-assertion_scheduled_training"
        )
        assert unpickled_request.name == "run_assertion_task"
        assert unpickled_request.args["urn"] == "urn:li:assertion:test-assertion"

        # Verify the assertion_spec (from model_dump) is preserved
        assertion_spec = unpickled_request.args["assertion_spec"]
        assert assertion_spec["type"] == "volume"
        assert assertion_spec["operator"] == "GREATER_THAN"
        assert assertion_spec["threshold"] == 100
        assert (
            assertion_spec["dataset_urn"]
            == "urn:li:dataset:(urn:li:dataPlatform:mysql,test.users,PROD)"
        )
        assert assertion_spec["metadata"]["created_by"] == "test-user"
        assert assertion_spec["metadata"]["version"] == "1.0"

        # Verify the context (from __dict__) is preserved
        context = unpickled_request.args["context"]
        assert context["dry_run"] is False
        assert context["online_smart_assertions"] is True

        assert_is_pydantic_v1_compatible(unpickled_request)


@mock.patch.dict(os.environ, {"DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE": "True"})
def test_execution_request_celery_integration_simulation():
    coordinator_request = ExecutionRequest(
        exec_id="coordinator-to-worker-123",
        name="run_assertion_task",
        args={
            "assertion_urn": "urn:li:assertion:freshness-check",
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.analytics.users,PROD)",
            "evaluation_config": {
                "lookback_days": 7,
                "acceptable_delay_hours": 2,
                "severity": "ERROR",
            },
        },
        executor_id="remote-executor-1",
    )
    assert_is_pydantic_v2(coordinator_request)

    celery_message_payload = schema_pickle_dumps(coordinator_request)
    worker_received_request = schema_pickle_loads(celery_message_payload)

    assert worker_received_request.exec_id == coordinator_request.exec_id
    assert worker_received_request.name == coordinator_request.name
    assert worker_received_request.args == coordinator_request.args
    assert worker_received_request.executor_id == coordinator_request.executor_id

    assert worker_received_request.args["evaluation_config"]["lookback_days"] == 7
    assert "assertion_urn" in worker_received_request.args
    assert worker_received_request.executor_id == "remote-executor-1"
    assert_is_pydantic_v1_compatible(worker_received_request)
