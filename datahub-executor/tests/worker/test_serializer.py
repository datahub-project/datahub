import sys
import types

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.metadata.schema_classes import GenericAspectClass

import datahub_executor.worker.celery_sqs.config as config
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
