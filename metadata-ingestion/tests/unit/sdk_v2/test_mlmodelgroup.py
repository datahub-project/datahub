from __future__ import annotations

import pathlib
import re
from datetime import datetime, timezone
from unittest import mock

import pytest

from datahub.errors import ItemNotFoundError
from datahub.metadata.urns import (
    DataPlatformUrn,
    DataProcessInstanceUrn,
    MlModelGroupUrn,
)
from datahub.sdk.mlmodelgroup import MLModelGroup
from datahub.utilities.urns.error import InvalidUrnError

_GOLDEN_DIR = pathlib.Path(__file__).parent / "mlmodelgroup_golden"


def test_mlmodelgroup_basic() -> None:
    """Test basic MLModelGroup functionality."""
    group = MLModelGroup(
        id="test_group",
        platform="mlflow",
        name="test_group",
    )

    # Test basic properties
    assert group.urn == MlModelGroupUrn(platform="mlflow", name="test_group")
    assert group.name == "test_group"
    assert group.platform == DataPlatformUrn("urn:li:dataPlatform:mlflow")

    # Test description
    group.set_description("A test model group")
    assert group.description == "A test model group"

    # Test timestamps
    now = datetime.now(timezone.utc).replace(microsecond=0)
    group.set_created(now)
    group.set_last_modified(now)
    assert group.created == now
    assert group.last_modified == now

    # Test custom properties
    group.set_custom_properties(
        {
            "purpose": "testing",
            "owner": "data-science",
        }
    )
    assert group.custom_properties == {
        "purpose": "testing",
        "owner": "data-science",
    }

    # Test training jobs
    job_urn = DataProcessInstanceUrn("job1")
    group.add_training_job(job_urn)
    assert group.training_jobs is not None
    assert str(job_urn) in group.training_jobs

    group.remove_training_job(job_urn)
    assert group.training_jobs is not None
    assert len(group.training_jobs) == 0

    # Test downstream jobs
    group.add_downstream_job(job_urn)
    assert group.downstream_jobs is not None
    assert str(job_urn) in group.downstream_jobs

    group.remove_downstream_job(job_urn)
    assert group.downstream_jobs is not None
    assert len(group.downstream_jobs) == 0


def test_mlmodelgroup_complex() -> None:
    """Test more complex MLModelGroup scenarios."""
    # Test initialization with all properties
    group = MLModelGroup(
        id="complex_group",
        platform="mlflow",
        name="complex_group",
        description="A complex test group",
        custom_properties={
            "purpose": "production",
            "owner": "ml-team",
        },
    )

    assert group.name == "complex_group"
    assert group.description == "A complex test group"
    assert group.custom_properties == {
        "purpose": "production",
        "owner": "ml-team",
    }

    # Test multiple training jobs
    job1 = DataProcessInstanceUrn("job1")
    job2 = DataProcessInstanceUrn("job2")

    group.add_training_job(job1)
    group.add_training_job(job2)
    assert group.training_jobs is not None
    assert len(group.training_jobs) == 2
    assert str(job1) in group.training_jobs
    assert str(job2) in group.training_jobs

    # Test multiple downstream jobs
    group.add_downstream_job(job1)
    group.add_downstream_job(job2)
    assert group.downstream_jobs is not None
    assert len(group.downstream_jobs) == 2
    assert str(job1) in group.downstream_jobs
    assert str(job2) in group.downstream_jobs


def test_mlmodelgroup_validation() -> None:
    """Test MLModelGroup validation and error cases."""
    # Test invalid platform
    with pytest.raises(InvalidUrnError):
        MLModelGroup(id="test", platform="")

    # Test invalid ID
    with pytest.raises(InvalidUrnError):
        MLModelGroup(id="", platform="test_platform")


# Client.entities.get tests
def test_client_get_mlmodelgroup():
    """Test retrieving an MLModelGroup using client.entities.get()."""
    # Arrange
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    group_urn = MlModelGroupUrn("mlflow", "test_group")

    # Create a group that would be returned by the client
    expected_group = MLModelGroup(
        id="test_group",
        platform="mlflow",
        name="Test Group",
        description="A test model group",
    )
    mock_entities.get.return_value = expected_group

    # Act
    result = mock_client.entities.get(group_urn)

    # Assert
    assert result == expected_group
    mock_entities.get.assert_called_once_with(group_urn)


def test_client_get_mlmodelgroup_with_properties():
    """Test retrieving an MLModelGroup with properties using client.entities.get()."""
    # Arrange
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    group_urn = MlModelGroupUrn("mlflow", "test_group")

    # Create a group with properties
    now = datetime.now(timezone.utc).replace(microsecond=0)
    expected_group = MLModelGroup(
        id="test_group",
        platform="mlflow",
        name="Test Group",
        description="A test model group with properties",
        custom_properties={
            "purpose": "testing",
            "owner": "data-science",
        },
    )
    # Set timestamps
    expected_group.set_created(now)
    expected_group.set_last_modified(now)

    mock_entities.get.return_value = expected_group

    # Act
    result = mock_client.entities.get(group_urn)

    # Assert
    assert result == expected_group
    assert result.name == "Test Group"
    assert result.description == "A test model group with properties"
    assert result.custom_properties == {
        "purpose": "testing",
        "owner": "data-science",
    }
    assert result.created == now
    assert result.last_modified == now
    mock_entities.get.assert_called_once_with(group_urn)


def test_client_get_mlmodelgroup_not_found():
    """Test behavior when retrieving a non-existent MLModelGroup."""
    # Arrange
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    group_urn = MlModelGroupUrn("mlflow", "nonexistent_group")
    error_message = f"Entity {group_urn} not found"
    mock_entities.get.side_effect = ItemNotFoundError(error_message)

    # Act & Assert
    with pytest.raises(ItemNotFoundError, match=re.escape(error_message)):
        mock_client.entities.get(group_urn)

    mock_entities.get.assert_called_once_with(group_urn)


def test_client_get_mlmodelgroup_with_string_urn():
    """Test retrieving an MLModelGroup using a string URN."""
    # Arrange
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    # Note: MlModelGroupUrn only has 2 parts (platform, name)
    urn_str = "urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,string_urn_group)"

    # Create a group that would be returned by the client
    expected_group = MLModelGroup(
        id="string_urn_group",
        platform="mlflow",
        name="String URN Group",
    )
    mock_entities.get.return_value = expected_group

    # Act
    result = mock_client.entities.get(urn_str)

    # Assert
    assert result == expected_group
    mock_entities.get.assert_called_once_with(urn_str)


def test_client_get_mlmodelgroup_with_relationships():
    """Test retrieving an MLModelGroup with relationship data."""
    # Arrange
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    group_urn = MlModelGroupUrn("mlflow", "group_with_relationships")

    # Create jobs for relationships
    job1_urn = DataProcessInstanceUrn("job1")
    job2_urn = DataProcessInstanceUrn("job2")

    # Create a group with relationships
    expected_group = MLModelGroup(
        id="group_with_relationships",
        platform="mlflow",
        name="Relationship Group",
    )
    # Add relationships
    expected_group.add_training_job(job1_urn)
    expected_group.add_downstream_job(job2_urn)

    mock_entities.get.return_value = expected_group

    # Act
    result = mock_client.entities.get(group_urn)

    # Assert
    assert result == expected_group
    assert str(job1_urn) in result.training_jobs
    assert str(job2_urn) in result.downstream_jobs
    mock_entities.get.assert_called_once_with(group_urn)
