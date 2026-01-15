"""
Unit tests for job_url_generator function
"""

import pytest

from datahub_dagster_plugin.client.dagster_generator import (
    DagsterEnvironment,
    job_url_generator,
)


@pytest.mark.parametrize(
    "is_cloud,branch,location_name,base_url,expected",
    [
        # Cloud scenarios
        (
            True,
            "prod",
            None,
            "https://example.dagster.cloud",
            "https://example.dagster.cloud/prod",
        ),
        (
            True,
            "staging",
            "analytics",
            "https://company.dagster.cloud",
            "https://company.dagster.cloud/staging/locations/analytics",
        ),
        # On-premise scenarios
        (False, "prod", None, "http://localhost:3000", "http://localhost:3000"),
        (
            False,
            "prod",
            "warehouse",
            "http://localhost:3000",
            "http://localhost:3000/locations/warehouse",
        ),
    ],
)
def test_url_generation(is_cloud, branch, location_name, base_url, expected):
    """Test URL generation for various combinations of parameters"""
    dagster_environment = DagsterEnvironment(
        repository="test_repo",
        is_cloud=is_cloud,
        is_branch_deployment=False,
        branch=branch,
        module="test_module",
        location_name=location_name,
    )

    result = job_url_generator(base_url, dagster_environment)

    assert result == expected
