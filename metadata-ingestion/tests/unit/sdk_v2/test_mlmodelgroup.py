from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from unittest import mock

import pytest

from datahub.metadata.urns import (
    DataProcessInstanceUrn,
    MlModelGroupUrn,
)
from datahub.sdk.mlmodelgroup import MLModelGroup
from datahub.utilities.urns.error import InvalidUrnError

_GOLDEN_DIR = pathlib.Path(__file__).parent / "mlmodelgroup_golden"


def test_mlmodelgroup() -> None:
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


def test_client_get_mlmodelgroup_():
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
