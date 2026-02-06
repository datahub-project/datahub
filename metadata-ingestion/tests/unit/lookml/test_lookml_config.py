import pathlib
from copy import deepcopy

import pytest
from pydantic import ValidationError

from datahub.configuration.git import GitInfo
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.lookml_config import LookMLSourceConfig


# ---- GitInfo validator tests ----


def test_git_info_validator_handles_non_dict_input() -> None:
    """GitInfo validator safely handles non-dict inputs (e.g., strings) without AttributeError."""
    # When GitInfo receives a string during union deserialization, it should return it unchanged
    # rather than trying to access dict keys, which would cause an AttributeError.
    # This tests the early return logic in deploy_key_filled_from_deploy_key_file validator.
    with pytest.raises(ValidationError):
        # Passing a string directly should fail validation, but not with AttributeError
        GitInfo.model_validate("not_a_dict")


def test_git_info_validator_handles_dict_with_deploy_key_file(
    tmp_path: pathlib.Path,
) -> None:
    """GitInfo validator properly processes dict inputs with deploy_key_file."""
    # Create a temporary deploy key file
    key_file = tmp_path / "deploy_key"
    test_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC..."
    key_file.write_text(test_key)

    git_info = GitInfo.model_validate({
        "repo": "https://github.com/org/repo",
        "branch": "main",
        "deploy_key_file": str(key_file),
    })

    assert git_info.repo == "https://github.com/org/repo"
    assert git_info.branch == "main"
    assert git_info.deploy_key is not None
    assert git_info.deploy_key.get_secret_value() == test_key


@pytest.fixture
def minimal_lookml_config(tmp_path: pathlib.Path) -> dict:
    base_dir = tmp_path / "base"
    base_dir.mkdir()
    return {
        "base_folder": str(base_dir),
        "connection_to_platform_map": {"my_connection": "postgres"},
        "project_name": "test_project",
    }


def test_project_dependencies_local_path_deserializes_to_directory_path(
    tmp_path: pathlib.Path, minimal_lookml_config: dict
) -> None:
    """A string path to an existing directory is deserialized as DirectoryPath (pathlib.Path)."""
    dep_dir = tmp_path / "dep"
    dep_dir.mkdir()
    minimal_lookml_config["project_dependencies"] = {"dep_name": str(dep_dir)}

    config = LookMLSourceConfig.model_validate(minimal_lookml_config)

    dep_ref = config.project_dependencies["dep_name"]
    assert isinstance(dep_ref, pathlib.Path)
    assert dep_ref.resolve() == dep_dir.resolve()
    assert not isinstance(dep_ref, GitInfo)


def test_project_dependencies_git_info_deserializes_to_git_info(
    minimal_lookml_config: dict,
) -> None:
    """A dict with repo/branch is deserialized as GitInfo."""
    minimal_lookml_config["project_dependencies"] = {
        "dep_name": {
            "repo": "https://github.com/org/repo",
            "branch": "main",
        }
    }

    config = LookMLSourceConfig.model_validate(minimal_lookml_config)

    dep_ref = config.project_dependencies["dep_name"]
    assert isinstance(dep_ref, GitInfo)
    assert dep_ref.repo == "https://github.com/org/repo"
    assert dep_ref.branch == "main"


def test_project_dependencies_nonexistent_path_raises_validation_error(
    tmp_path: pathlib.Path, minimal_lookml_config: dict
) -> None:
    """A string path that does not exist raises ValidationError, not AttributeError."""
    nonexistent = tmp_path / "nonexistent"
    assert not nonexistent.exists()
    minimal_lookml_config["project_dependencies"] = {"dep_name": str(nonexistent)}

    with pytest.raises(ValidationError):
        LookMLSourceConfig.model_validate(minimal_lookml_config)


# ---- connection_to_platform_map (convert_string_to_connection_def) ----


def test_connection_to_platform_map_legacy_string_with_dot_upconverted(
    minimal_lookml_config: dict,
) -> None:
    """Legacy string 'platform.default_db' is upconverted to LookerConnectionDefinition."""
    minimal_lookml_config["connection_to_platform_map"] = {
        "my_conn": "snowflake.mydb",
    }
    config = LookMLSourceConfig.model_validate(minimal_lookml_config)
    assert config.connection_to_platform_map is not None
    conn = config.connection_to_platform_map["my_conn"]
    assert isinstance(conn, LookerConnectionDefinition)
    assert conn.platform == "snowflake"
    assert conn.default_db == "mydb"
    assert conn.default_schema == ""


def test_connection_to_platform_map_legacy_string_without_dot_upconverted(
    minimal_lookml_config: dict,
) -> None:
    """Legacy string without dot (platform only) is upconverted with empty default_db."""
    minimal_lookml_config["connection_to_platform_map"] = {
        "my_conn": "postgres",
    }
    config = LookMLSourceConfig.model_validate(minimal_lookml_config)
    assert config.connection_to_platform_map is not None
    conn = config.connection_to_platform_map["my_conn"]
    assert isinstance(conn, LookerConnectionDefinition)
    assert conn.platform == "postgres"
    assert conn.default_db == ""


def test_connection_to_platform_map_full_dict_preserved(
    minimal_lookml_config: dict,
) -> None:
    """Full LookerConnectionDefinition dict is preserved (not overwritten by upconversion)."""
    minimal_lookml_config["connection_to_platform_map"] = {
        "my_conn": {
            "platform": "bigquery",
            "default_db": "analytics",
            "default_schema": "public",
        },
    }
    config = LookMLSourceConfig.model_validate(minimal_lookml_config)
    assert config.connection_to_platform_map is not None
    conn = config.connection_to_platform_map["my_conn"]
    assert conn.platform == "bigquery"
    assert conn.default_db == "analytics"
    assert conn.default_schema == "public"


# ---- model validators: require connection_map or api ----


def test_missing_connection_map_and_api_raises(tmp_path: pathlib.Path) -> None:
    """Validation fails when neither connection_to_platform_map nor api is provided."""
    base_dir = tmp_path / "base"
    base_dir.mkdir()
    config = {
        "base_folder": str(base_dir),
        "project_name": "p",
        # no connection_to_platform_map, no api
    }
    with pytest.raises(ValidationError, match="connection_to_platform_map|api"):
        LookMLSourceConfig.model_validate(config)


# ---- model validators: require project_name or api ----


def test_missing_project_name_and_api_raises(
    tmp_path: pathlib.Path, minimal_lookml_config: dict
) -> None:
    """Validation fails when neither project_name nor api is provided."""
    conf = deepcopy(minimal_lookml_config)
    conf.pop("project_name", None)
    conf["project_name"] = None
    # ensure no api
    conf.pop("api", None)
    with pytest.raises(ValidationError, match="project_name|api"):
        LookMLSourceConfig.model_validate(conf)


# ---- model validators: use_api_for_view_lineage requires api ----


def test_use_api_for_view_lineage_without_api_raises(
    minimal_lookml_config: dict,
) -> None:
    """Validation fails when use_api_for_view_lineage is True but api is not provided."""
    minimal_lookml_config["use_api_for_view_lineage"] = True
    minimal_lookml_config.pop("api", None)
    with pytest.raises(ValidationError, match="api|view lineage"):
        LookMLSourceConfig.model_validate(minimal_lookml_config)


# ---- model validators: require base_folder or git_info ----


def test_missing_base_folder_and_git_info_raises(minimal_lookml_config: dict) -> None:
    """Validation fails when neither base_folder nor git_info is provided."""
    conf = deepcopy(minimal_lookml_config)
    conf["base_folder"] = None
    conf.pop("git_info", None)
    with pytest.raises(ValidationError, match="base_folder|git_info"):
        LookMLSourceConfig.model_validate(conf)


# ---- field validator: max_workers_for_parallel_processing ----


def test_max_workers_below_one_raises(minimal_lookml_config: dict) -> None:
    """max_workers_for_parallel_processing must be at least 1."""
    minimal_lookml_config["max_workers_for_parallel_processing"] = 0
    with pytest.raises(ValidationError, match="at least 1"):
        LookMLSourceConfig.model_validate(minimal_lookml_config)


def test_max_workers_above_100_capped_to_100(
    minimal_lookml_config: dict,
) -> None:
    """max_workers_for_parallel_processing above 100 is capped to 100."""
    minimal_lookml_config["max_workers_for_parallel_processing"] = 150
    config = LookMLSourceConfig.model_validate(minimal_lookml_config)
    assert config.max_workers_for_parallel_processing == 100


# ---- looker_environment literal ----


def test_looker_environment_dev_accepted(minimal_lookml_config: dict) -> None:
    """looker_environment accepts 'dev'."""
    minimal_lookml_config["looker_environment"] = "dev"
    config = LookMLSourceConfig.model_validate(minimal_lookml_config)
    assert config.looker_environment == "dev"


def test_looker_environment_invalid_raises(minimal_lookml_config: dict) -> None:
    """looker_environment only accepts 'prod' or 'dev'."""
    minimal_lookml_config["looker_environment"] = "staging"
    with pytest.raises(ValidationError):
        LookMLSourceConfig.model_validate(minimal_lookml_config)


# ---- project_dependencies: mixed local path and GitInfo ----


def test_project_dependencies_mixed_local_path_and_git_info(
    tmp_path: pathlib.Path, minimal_lookml_config: dict
) -> None:
    """Config can have both a local DirectoryPath and a GitInfo in project_dependencies."""
    dep_dir = tmp_path / "local_dep"
    dep_dir.mkdir()
    minimal_lookml_config["project_dependencies"] = {
        "local_project": str(dep_dir),
        "remote_project": {
            "repo": "https://github.com/org/other",
            "branch": "main",
        },
    }
    config = LookMLSourceConfig.model_validate(minimal_lookml_config)
    assert isinstance(config.project_dependencies["local_project"], pathlib.Path)
    assert config.project_dependencies["local_project"].resolve() == dep_dir.resolve()
    assert isinstance(config.project_dependencies["remote_project"], GitInfo)
    assert (
        config.project_dependencies["remote_project"].repo
        == "https://github.com/org/other"
    )
