from __future__ import annotations

import pathlib
from datetime import datetime, timezone

import pytest

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
