import sys
import types
from typing import Any, Dict

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.metadata.schema_classes import GenericAspectClass

import datahub_executor.worker.celery_sqs.config as config
from datahub_executor.common.types import (
    Monitor,
)
from datahub_executor.worker.celery_sqs.config import (
    schema_pickle_dumps,
    schema_pickle_loads,
)

TEST_EXECUTION_REQUEST = ExecutionRequest(
    name="test-execution-request",
    args={
        "test_arg_1": GenericAspectClass(b"test", "text/plain"),
        "test_arg_2": "urn:li:test_request_0001",
    },
)


class TestModuleRename:
    pass


def test_compat_enabled(monkeypatch) -> None:
    monkeypatch.setattr(
        config, "DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE", True, raising=False
    )

    serialized = schema_pickle_dumps(TEST_EXECUTION_REQUEST)
    deserialized = schema_pickle_loads(serialized)

    assert deserialized.model_dump() == TEST_EXECUTION_REQUEST.model_dump()


def test_compat_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        config, "DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE", False, raising=False
    )

    serialized = schema_pickle_dumps(TEST_EXECUTION_REQUEST)
    deserialized = schema_pickle_loads(serialized)

    assert deserialized.model_dump() == TEST_EXECUTION_REQUEST.model_dump()


def test_module_name_translation(monkeypatch, caplog):
    monkeypatch.setattr(
        config, "DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE", True, raising=False
    )

    fake_old_mod = "acryl_datahub_cloud.something.sub"
    fake_new_mod = "datahub.something.sub"

    pkg_datahub = types.ModuleType("datahub")
    pkg_something = types.ModuleType("datahub.something")
    pkg_sub = types.ModuleType(fake_new_mod)

    pkg_datahub.__path__ = []
    pkg_something.__path__ = []
    pkg_sub.__path__ = []

    sys.modules["datahub"] = pkg_datahub
    sys.modules["datahub.something"] = pkg_something
    sys.modules[fake_new_mod] = pkg_sub

    old_mod = types.ModuleType(fake_old_mod)
    sys.modules[fake_old_mod] = old_mod

    TestModuleRename.__module__ = fake_old_mod
    old_mod.TestModuleRename = TestModuleRename  # type: ignore
    pkg_sub.TestModuleRename = TestModuleRename  # type: ignore

    caplog.set_level("DEBUG")

    obj = TestModuleRename()
    data = schema_pickle_dumps(obj)

    assert any("Translated module name" in rec.message for rec in caplog.records)
    assert b"datahub.something.sub" in data
    assert b"acryl_datahub_cloud" not in data
    assert obj.__module__ == fake_old_mod

    unpickled = schema_pickle_loads(data)
    assert isinstance(unpickled, TestModuleRename)


def test_private_field_translation_compat_enabled(monkeypatch):
    monkeypatch.setattr(
        config, "DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE", True, raising=False
    )

    serialized = schema_pickle_dumps(TEST_EXECUTION_REQUEST)

    assert b"__private_attribute_values__" in serialized
    assert b"__fields_set__" in serialized
    assert b"__pydantic_private__" in serialized
    assert b"__pydantic_extra__" in serialized


def test_private_field_translation_compat_disabled(monkeypatch):
    monkeypatch.setattr(
        config, "DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE", False, raising=False
    )

    serialized = schema_pickle_dumps(TEST_EXECUTION_REQUEST)

    assert b"__private_attribute_values__" not in serialized
    assert b"__fields_set__" not in serialized
    assert b"__pydantic_private__" in serialized
    assert b"__pydantic_extra__" in serialized


# Tests for bound state leak fix


def test_multiple_instances_no_state_leak(monkeypatch):
    """
    Test that multiple ExecutionRequest objects with different args
    maintain their own state when pickled sequentially.
    """
    monkeypatch.setattr(
        config, "DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE", True, raising=False
    )

    # Create three distinct ExecutionRequest objects with different data
    request1 = ExecutionRequest(
        name="request-1",
        args={
            "arg1": GenericAspectClass(b"data-1", "text/plain"),
            "urn": "urn:li:test:001",
        },
    )

    request2 = ExecutionRequest(
        name="request-2",
        args={
            "arg1": GenericAspectClass(b"data-2", "text/plain"),
            "urn": "urn:li:test:002",
        },
    )

    request3 = ExecutionRequest(
        name="request-3",
        args={
            "arg1": GenericAspectClass(b"data-3", "text/plain"),
            "urn": "urn:li:test:003",
        },
    )

    # Pickle them sequentially
    serialized1 = schema_pickle_dumps(request1)
    serialized2 = schema_pickle_dumps(request2)
    serialized3 = schema_pickle_dumps(request3)

    # Deserialize them
    deserialized1 = schema_pickle_loads(serialized1)
    deserialized2 = schema_pickle_loads(serialized2)
    deserialized3 = schema_pickle_loads(serialized3)

    # Verify each maintains its own unique state (no cross-contamination)
    assert deserialized1.name == "request-1"
    assert deserialized1.args["urn"] == "urn:li:test:001"
    assert deserialized1.args["arg1"].value == b"data-1"

    assert deserialized2.name == "request-2"
    assert deserialized2.args["urn"] == "urn:li:test:002"
    assert deserialized2.args["arg1"].value == b"data-2"

    assert deserialized3.name == "request-3"
    assert deserialized3.args["urn"] == "urn:li:test:003"
    assert deserialized3.args["arg1"].value == b"data-3"


def _create_monitor_dict(monitor_id: str, assertion_urns: list) -> Dict[str, Any]:
    """Helper to create monitor test data with specific assertion URNs."""
    return {
        "urn": f"urn:li:monitor:{monitor_id}",
        "info": {
            "type": "ASSERTION",
            "status": {"mode": "ACTIVE"},
            "executorId": "test-executor",
            "assertionMonitor": {
                "assertions": [
                    {
                        "assertion": {
                            "urn": urn,
                            "type": "VOLUME",
                            "info": {
                                "type": "VOLUME",
                                "source": {"type": "NATIVE"},
                                "volumeAssertion": {
                                    "type": "ROW_COUNT_TOTAL",
                                    "rowCountTotal": {
                                        "operator": "GREATER_THAN",
                                        "parameters": {
                                            "value": {
                                                "value": str(100 + i * 10),
                                                "type": "NUMBER",
                                            }
                                        },
                                    },
                                },
                            },
                        },
                        "schedule": {"cron": "*/15 * * * *", "timezone": "UTC"},
                        "parameters": {
                            "type": "DATASET_VOLUME",
                            "datasetVolumeParameters": {"sourceType": "QUERY"},
                        },
                    }
                    for i, urn in enumerate(assertion_urns)
                ]
            },
        },
        "entity": {
            "urn": f"urn:li:dataset:{monitor_id}",
            "platform": {
                "urn": "urn:li:dataPlatform:snowflake",
                "properties": {"displayName": "Snowflake"},
            },
            "properties": {
                "name": f"table_{monitor_id}",
                "qualifiedName": f"test_db.test_schema.table_{monitor_id}",
            },
            "subTypes": {"typeNames": ["TABLE"]},
            "exists": True,
        },
    }


def test_nested_pydantic_models_no_state_leak(monkeypatch):
    """
    Test that multiple Monitor objects with different nested AssertionMonitor
    and Assertion objects maintain their own state when pickled.
    This is the real-world scenario where state leakage was occurring.
    """
    monkeypatch.setattr(
        config, "DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE", True, raising=False
    )

    # Create three monitors with different assertion URNs
    monitor1_dict = _create_monitor_dict(
        "monitor-1", ["urn:li:assertion:assertion-1a", "urn:li:assertion:assertion-1b"]
    )
    monitor2_dict = _create_monitor_dict(
        "monitor-2", ["urn:li:assertion:assertion-2a", "urn:li:assertion:assertion-2b"]
    )
    monitor3_dict = _create_monitor_dict(
        "monitor-3", ["urn:li:assertion:assertion-3a", "urn:li:assertion:assertion-3b"]
    )

    # Create Monitor pydantic models
    monitor1 = Monitor(**monitor1_dict)
    monitor2 = Monitor(**monitor2_dict)
    monitor3 = Monitor(**monitor3_dict)

    # Pickle them sequentially (simulating the real-world queue scenario)
    serialized1 = schema_pickle_dumps(monitor1)
    serialized2 = schema_pickle_dumps(monitor2)
    serialized3 = schema_pickle_dumps(monitor3)

    # Deserialize them
    deserialized1 = schema_pickle_loads(serialized1)
    deserialized2 = schema_pickle_loads(serialized2)
    deserialized3 = schema_pickle_loads(serialized3)

    # Verify each monitor maintains its unique nested assertion data
    assert deserialized1.urn == "urn:li:monitor:monitor-1"
    assert len(deserialized1.assertion_monitor.assertions) == 2
    assert (
        deserialized1.assertion_monitor.assertions[0].assertion.urn
        == "urn:li:assertion:assertion-1a"
    )
    assert (
        deserialized1.assertion_monitor.assertions[1].assertion.urn
        == "urn:li:assertion:assertion-1b"
    )

    assert deserialized2.urn == "urn:li:monitor:monitor-2"
    assert len(deserialized2.assertion_monitor.assertions) == 2
    assert (
        deserialized2.assertion_monitor.assertions[0].assertion.urn
        == "urn:li:assertion:assertion-2a"
    )
    assert (
        deserialized2.assertion_monitor.assertions[1].assertion.urn
        == "urn:li:assertion:assertion-2b"
    )

    assert deserialized3.urn == "urn:li:monitor:monitor-3"
    assert len(deserialized3.assertion_monitor.assertions) == 2
    assert (
        deserialized3.assertion_monitor.assertions[0].assertion.urn
        == "urn:li:assertion:assertion-3a"
    )
    assert (
        deserialized3.assertion_monitor.assertions[1].assertion.urn
        == "urn:li:assertion:assertion-3b"
    )


def test_getstate_restored_after_pickling(monkeypatch):
    """
    Test that after pickling a Pydantic v2 object, its __getstate__
    attribute is restored to the original method (not left with the patched version).
    """
    monkeypatch.setattr(
        config, "DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE", True, raising=False
    )

    request = ExecutionRequest(
        name="test-request",
        args={
            "arg1": GenericAspectClass(b"test-data", "text/plain"),
        },
    )

    # Capture the original __getstate__ method
    original_getstate = request.__getstate__

    # Pickle the object
    schema_pickle_dumps(request)

    # Verify the __getstate__ method is restored to the original
    assert request.__getstate__ == original_getstate

    # Verify it can be pickled again successfully (no side effects)
    serialized2 = schema_pickle_dumps(request)
    deserialized2 = schema_pickle_loads(serialized2)

    assert deserialized2.name == "test-request"
    assert deserialized2.args["arg1"].value == b"test-data"


def test_same_object_pickled_multiple_times(monkeypatch):
    """
    Test that the same object can be pickled multiple times without
    side effects or state corruption.
    """
    monkeypatch.setattr(
        config, "DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE", True, raising=False
    )

    request = ExecutionRequest(
        name="test-request-multi",
        args={
            "arg1": GenericAspectClass(b"multi-pickle-data", "text/plain"),
            "urn": "urn:li:test:multi",
        },
    )

    # Pickle the same object multiple times
    serialized1 = schema_pickle_dumps(request)
    serialized2 = schema_pickle_dumps(request)
    serialized3 = schema_pickle_dumps(request)

    # All serialized versions should deserialize to the same data
    deserialized1 = schema_pickle_loads(serialized1)
    deserialized2 = schema_pickle_loads(serialized2)
    deserialized3 = schema_pickle_loads(serialized3)

    # Verify all produce identical results
    assert deserialized1.name == "test-request-multi"
    assert deserialized2.name == "test-request-multi"
    assert deserialized3.name == "test-request-multi"

    assert deserialized1.args["urn"] == "urn:li:test:multi"
    assert deserialized2.args["urn"] == "urn:li:test:multi"
    assert deserialized3.args["urn"] == "urn:li:test:multi"

    assert deserialized1.args["arg1"].value == b"multi-pickle-data"
    assert deserialized2.args["arg1"].value == b"multi-pickle-data"
    assert deserialized3.args["arg1"].value == b"multi-pickle-data"


def test_concurrent_pickling_simulation(monkeypatch):
    """
    Test rapid sequential pickling of multiple different objects
    to simulate the real-world queue scenario where multiple monitors
    are being pickled in quick succession.
    """
    monkeypatch.setattr(
        config, "DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE", True, raising=False
    )

    # Create a list of diverse objects
    objects = []
    for i in range(10):
        request = ExecutionRequest(
            name=f"request-{i}",
            args={
                "arg1": GenericAspectClass(f"data-{i}".encode(), "text/plain"),
                "urn": f"urn:li:test:{i:03d}",
                "index": i,
            },
        )
        objects.append(request)

    # Pickle all objects rapidly in succession
    serialized_objects = [schema_pickle_dumps(obj) for obj in objects]

    # Deserialize all objects
    deserialized_objects = [schema_pickle_loads(s) for s in serialized_objects]

    # Verify each object maintains its unique state
    for i, deserialized in enumerate(deserialized_objects):
        assert deserialized.name == f"request-{i}", f"Object {i} has wrong name"
        assert deserialized.args["urn"] == f"urn:li:test:{i:03d}", (
            f"Object {i} has wrong urn"
        )
        assert deserialized.args["arg1"].value == f"data-{i}".encode(), (
            f"Object {i} has wrong data"
        )
        assert deserialized.args["index"] == i, f"Object {i} has wrong index"
