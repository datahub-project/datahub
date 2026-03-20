"""Unit tests for Fabric Data Factory data models.

Tests from_dict parsing for PipelineActivity, PipelineActivityRun,
ActivityDependency, ActivityPolicy, ExternalReference, and FabricConnection.
"""

import pytest

from datahub.ingestion.source.fabric.common.models import FabricConnection
from datahub.ingestion.source.fabric.data_factory.models import (
    ActivityDependency,
    ActivityPolicy,
    ExternalReference,
    PipelineActivity,
    PipelineActivityRun,
)


class TestActivityDependency:
    def test_from_dict(self) -> None:
        dep = ActivityDependency.from_dict(
            {"activity": "Step1", "dependencyConditions": ["Succeeded"]}
        )
        assert dep.activity == "Step1"
        assert dep.dependency_conditions == ["Succeeded"]

    def test_from_dict_multiple_conditions(self) -> None:
        dep = ActivityDependency.from_dict(
            {
                "activity": "Step1",
                "dependencyConditions": ["Succeeded", "Failed", "Skipped"],
            }
        )
        assert len(dep.dependency_conditions) == 3

    def test_from_dict_missing_key_raises(self) -> None:
        with pytest.raises(KeyError):
            ActivityDependency.from_dict({"dependencyConditions": ["Succeeded"]})


class TestActivityPolicy:
    def test_from_dict_full(self) -> None:
        policy = ActivityPolicy.from_dict(
            {
                "timeout": "0.12:00:00",
                "retry": 3,
                "retryIntervalInSeconds": 30,
                "secureInput": True,
                "secureOutput": False,
            }
        )
        assert policy.timeout == "0.12:00:00"
        assert policy.retry == 3
        assert policy.retry_interval_in_seconds == 30
        assert policy.secure_input is True
        assert policy.secure_output is False

    def test_from_dict_empty(self) -> None:
        policy = ActivityPolicy.from_dict({})
        assert policy.timeout is None
        assert policy.retry is None
        assert policy.retry_interval_in_seconds is None
        assert policy.secure_input is None
        assert policy.secure_output is None

    def test_from_dict_partial(self) -> None:
        policy = ActivityPolicy.from_dict({"timeout": "1.00:00:00"})
        assert policy.timeout == "1.00:00:00"
        assert policy.retry is None


class TestExternalReference:
    def test_from_dict(self) -> None:
        ref = ExternalReference.from_dict({"connection": "conn-123"})
        assert ref.connection == "conn-123"

    def test_from_dict_missing_key_raises(self) -> None:
        with pytest.raises(KeyError):
            ExternalReference.from_dict({})


class TestPipelineActivity:
    def test_from_dict_minimal(self) -> None:
        activity = PipelineActivity.from_dict(
            {"name": "Copy1", "type": "Copy", "typeProperties": {}}
        )
        assert activity.name == "Copy1"
        assert activity.type == "Copy"
        assert activity.type_properties == {}
        assert activity.depends_on == []
        assert activity.policy is None
        assert activity.external_references is None
        assert activity.description is None

    def test_from_dict_full(self) -> None:
        activity = PipelineActivity.from_dict(
            {
                "name": "CopyData",
                "type": "Copy",
                "description": "Copy data from source",
                "state": "Active",
                "onInactiveMarkAs": "Succeeded",
                "typeProperties": {"source": {}, "sink": {}},
                "dependsOn": [
                    {"activity": "Lookup1", "dependencyConditions": ["Succeeded"]}
                ],
                "policy": {"timeout": "0.12:00:00", "retry": 2},
                "externalReferences": {"connection": "conn-abc"},
            }
        )
        assert activity.name == "CopyData"
        assert activity.type == "Copy"
        assert activity.description == "Copy data from source"
        assert activity.state == "Active"
        assert activity.on_inactive_mark_as == "Succeeded"
        assert len(activity.depends_on) == 1
        assert activity.depends_on[0].activity == "Lookup1"
        assert activity.policy is not None
        assert activity.policy.timeout == "0.12:00:00"
        assert activity.external_references is not None
        assert activity.external_references.connection == "conn-abc"

    def test_from_dict_missing_name_raises(self) -> None:
        with pytest.raises(KeyError):
            PipelineActivity.from_dict({"type": "Copy", "typeProperties": {}})

    def test_from_dict_missing_type_raises(self) -> None:
        with pytest.raises(KeyError):
            PipelineActivity.from_dict({"name": "Copy1", "typeProperties": {}})

    def test_from_dict_missing_type_properties_raises(self) -> None:
        with pytest.raises(KeyError):
            PipelineActivity.from_dict({"name": "Copy1", "type": "Copy"})

    def test_from_dict_policy_not_dict_ignored(self) -> None:
        """Non-dict policy value is treated as None."""
        activity = PipelineActivity.from_dict(
            {
                "name": "Copy1",
                "type": "Copy",
                "typeProperties": {},
                "policy": "invalid",
            }
        )
        assert activity.policy is None

    def test_from_dict_external_references_not_dict_ignored(self) -> None:
        """Non-dict externalReferences value is treated as None."""
        activity = PipelineActivity.from_dict(
            {
                "name": "Copy1",
                "type": "Copy",
                "typeProperties": {},
                "externalReferences": "invalid",
            }
        )
        assert activity.external_references is None

    def test_from_dict_multiple_dependencies(self) -> None:
        activity = PipelineActivity.from_dict(
            {
                "name": "Step3",
                "type": "Copy",
                "typeProperties": {},
                "dependsOn": [
                    {"activity": "Step1", "dependencyConditions": ["Succeeded"]},
                    {"activity": "Step2", "dependencyConditions": ["Completed"]},
                ],
            }
        )
        assert len(activity.depends_on) == 2
        assert activity.depends_on[0].activity == "Step1"
        assert activity.depends_on[1].activity == "Step2"


class TestPipelineActivityRun:
    def test_from_dict_full(self) -> None:
        run = PipelineActivityRun.from_dict(
            {
                "activityName": "CopyData",
                "activityType": "Copy",
                "activityRunId": "ar-123",
                "pipelineRunId": "pr-456",
                "status": "Succeeded",
                "activityRunStart": "2024-01-15T10:00:00Z",
                "activityRunEnd": "2024-01-15T10:10:00Z",
                "durationInMs": 600000,
                "error": {"message": "Some error"},
                "retryAttempt": 1,
            }
        )
        assert run.activity_name == "CopyData"
        assert run.activity_type == "Copy"
        assert run.activity_run_id == "ar-123"
        assert run.pipeline_run_id == "pr-456"
        assert run.status == "Succeeded"
        assert run.activity_run_start == "2024-01-15T10:00:00Z"
        assert run.activity_run_end == "2024-01-15T10:10:00Z"
        assert run.duration_in_ms == 600000
        assert run.error_message == "Some error"
        assert run.retry_attempt == 1

    def test_from_dict_empty_defaults(self) -> None:
        run = PipelineActivityRun.from_dict({})
        assert run.activity_name == ""
        assert run.activity_type == ""
        assert run.activity_run_id == ""
        assert run.pipeline_run_id == ""
        assert run.status == ""
        assert run.activity_run_start is None
        assert run.activity_run_end is None
        assert run.duration_in_ms is None
        assert run.error_message is None
        assert run.retry_attempt is None

    def test_from_dict_empty_error_message_normalized_to_none(self) -> None:
        run = PipelineActivityRun.from_dict(
            {
                "activityName": "Copy1",
                "activityType": "Copy",
                "activityRunId": "ar-1",
                "pipelineRunId": "pr-1",
                "status": "Succeeded",
                "error": {"message": ""},
            }
        )
        assert run.error_message is None

    def test_from_dict_error_not_dict_ignored(self) -> None:
        run = PipelineActivityRun.from_dict(
            {
                "activityName": "Copy1",
                "activityType": "Copy",
                "activityRunId": "ar-1",
                "pipelineRunId": "pr-1",
                "status": "Succeeded",
                "error": "string-error",
            }
        )
        assert run.error_message is None

    def test_from_dict_no_error_key(self) -> None:
        run = PipelineActivityRun.from_dict(
            {
                "activityName": "Copy1",
                "activityType": "Copy",
                "activityRunId": "ar-1",
                "pipelineRunId": "pr-1",
                "status": "Succeeded",
            }
        )
        assert run.error_message is None


class TestFabricConnection:
    def test_from_dict_full(self) -> None:
        conn = FabricConnection.from_dict(
            {
                "id": "conn-123",
                "displayName": "My Snowflake",
                "connectionDetails": {
                    "type": "Snowflake",
                    "path": "myaccount.snowflakecomputing.com;mydb",
                },
            }
        )
        assert conn.id == "conn-123"
        assert conn.display_name == "My Snowflake"
        assert conn.connection_type == "Snowflake"
        assert conn.connection_path == "myaccount.snowflakecomputing.com;mydb"

    def test_from_dict_no_connection_details(self) -> None:
        conn = FabricConnection.from_dict(
            {"id": "conn-123", "displayName": "Empty Connection"}
        )
        assert conn.connection_type == ""
        assert conn.connection_path is None

    def test_from_dict_no_path(self) -> None:
        conn = FabricConnection.from_dict(
            {
                "id": "conn-123",
                "displayName": "Lakehouse Conn",
                "connectionDetails": {"type": "Lakehouse"},
            }
        )
        assert conn.connection_type == "Lakehouse"
        assert conn.connection_path is None

    def test_from_dict_missing_display_name_defaults_empty(self) -> None:
        conn = FabricConnection.from_dict(
            {
                "id": "conn-123",
                "connectionDetails": {"type": "SQL"},
            }
        )
        assert conn.display_name == ""
