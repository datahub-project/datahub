from pathlib import Path

import pytest
import yaml

from lib.targets import (
    load_target_from_env_file,
    parse_targets_from_args,
)


def test_parse_target_kv() -> None:
    targets = parse_targets_from_args(
        env_files=[],
        env_file_names=[],
        target_strings=[
            "name=staging,gms_url=http://staging:8080,frontend_url=http://staging:9002"
        ],
        gms_url=None,
        frontend_url=None,
    )
    assert len(targets) == 1
    assert targets[0].name == "staging"
    assert targets[0].gms_url == "http://staging:8080"
    assert targets[0].frontend_url == "http://staging:9002"


def test_parse_target_requires_name() -> None:
    with pytest.raises(ValueError, match="name="):
        parse_targets_from_args(
            env_files=[],
            env_file_names=[],
            target_strings=["gms_url=http://x:8080"],
            gms_url=None,
            frontend_url=None,
        )


def test_load_env_file(tmp_path: Path) -> None:
    env_path = tmp_path / "staging.datahubenv"
    env_path.write_text(
        yaml.dump(
            {
                "gms": {
                    "server": "http://staging.example:8080",
                    "token": "test-token",
                }
            }
        ),
        encoding="utf-8",
    )
    target = load_target_from_env_file(env_path)
    assert target.name == "staging"
    assert target.gms_url == "http://staging.example:8080"
    assert target.token == "test-token"
    assert target.env_file == str(env_path.resolve())


def test_env_file_hidden_name() -> None:
    from lib.targets import _default_name_from_env_path

    assert _default_name_from_env_path(Path("/tmp/.datahubenv_dev04")) == "dev04"


def test_env_file_name_override(tmp_path: Path) -> None:
    env_path = tmp_path / "a.datahubenv"
    env_path.write_text(
        yaml.dump({"gms": {"server": "http://localhost:8080", "token": "t"}}),
        encoding="utf-8",
    )
    targets = parse_targets_from_args(
        env_files=[str(env_path)],
        env_file_names=["custom"],
        target_strings=[],
        gms_url=None,
        frontend_url=None,
    )
    assert targets[0].name == "custom"


def test_gms_url_creates_local_target() -> None:
    targets = parse_targets_from_args(
        env_files=[],
        env_file_names=[],
        target_strings=[],
        gms_url="http://localhost:8080",
        frontend_url=None,
    )
    assert targets[0].name == "local"
