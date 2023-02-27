
from datahub.cli.quickstart_versioning import QuickstartVersionMappingConfig, QuickstartExecutionPlan

example_version_mapper = QuickstartVersionMappingConfig.parse_obj({
    "quickstart_version_mappings": {
        "default": {
            "composefile_git_ref": "master",
            "docker_tag": "latest"
        },
        "v2.0.0": {
            "composefile_git_ref": "v2.0.1",
            "docker_tag": "v2.0.0"
        },
        "v1.0.0": {
            "composefile_git_ref": "v1.0.0",
            "docker_tag": "v1.0.0"
        }
    },
    "stable_versions": {
        "force": False,
        "composefile_git_ref": "v1.0.1",
        "docker_tag": "latest"
    },
    "quickstart_checks": [
        {
            "valid_until_git_ref": "master",
            "required_containers": ["A", "B"],
            "ensure_exit_success": ["A"]
        },
        {
            "valid_until_git_ref": "v1.0.1",
            "required_containers": ["datahub-gms", "datahub-mae-consumer"],
            "ensure_exit_success": ["datahub-gms"]
        }
    ]
})

def test_quickstart_version_config():
    execution_plan = example_version_mapper.get_quickstart_execution_plan("v1.0.0", False)
    expected = QuickstartExecutionPlan(
        docker_tag="v1.0.0",
        composefile_git_ref="v1.0.0",
        required_containers=["datahub-gms", "datahub-mae-consumer"],
        ensure_exit_success=["datahub-gms"]
    )
    assert execution_plan == expected

def test_quickstart_version_config_default():
    execution_plan = example_version_mapper.get_quickstart_execution_plan("v2.0.0", False)
    excepted = QuickstartExecutionPlan(
        docker_tag="v2.0.0",
        composefile_git_ref="v2.0.1",
        required_containers=["A", "B"],
        ensure_exit_success=["A"]
    )
    assert execution_plan == excepted

def test_quickstart_version_config_stable():
    execution_plan = example_version_mapper.get_quickstart_execution_plan(None, True)
    excepted = QuickstartExecutionPlan(
        docker_tag="latest",
        composefile_git_ref="v1.0.1",
        required_containers=["datahub-gms", "datahub-mae-consumer"],
        ensure_exit_success= ["datahub-gms"]
    )
    assert execution_plan == excepted


def test_quickstart_forced_stable():
    example_version_mapper.stable_versions.force = True
    execution_plan = example_version_mapper.get_quickstart_execution_plan(None)
    excepted = QuickstartExecutionPlan(
        docker_tag="latest",
        composefile_git_ref="v1.0.1",
        required_containers=["datahub-gms", "datahub-mae-consumer"],
        ensure_exit_success= ["datahub-gms"]
    )
    assert execution_plan == excepted

def test_quickstart_forced_not_a_version_tag():
    """
    If the user specifies a version that is not in the version mapping, we should still use the default version for checking.

    This is because the user may be using a version of datahub that is not in the version mapping. Which is most likely
    a recent change, otherwise it should be on an older version of datahub.
    """
    example_version_mapper.stable_versions.force = True
    execution_plan = example_version_mapper.get_quickstart_execution_plan("NOT A VERSION")
    excepted = QuickstartExecutionPlan(
        docker_tag="latest",
        composefile_git_ref="v1.0.1",
        required_containers=["datahub-gms", "datahub-mae-consumer"],
        ensure_exit_success= ["datahub-gms"]
    )
    assert execution_plan == excepted