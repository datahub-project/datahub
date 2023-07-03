import os
from unittest import mock

import expandvars
import pytest
import yaml

from datahub.configuration.common import ConfigurationError
from datahub.configuration.config_loader import (
    list_referenced_env_variables,
    load_config_file,
)


@pytest.mark.parametrize(
    "filename,golden_config,env,referenced_env_vars",
    [
        (
            # Basic YAML load
            "tests/unit/config/basic.yml",
            {"foo": "bar", "nested": {"array": ["one", "two"], "hi": "hello"}},
            {},
            set(),
        ),
        (
            # Basic TOML load
            "tests/unit/config/basic.toml",
            {"foo": "bar", "nested": {"array": ["one", "two"], "hi": "hello"}},
            {},
            None,
        ),
        # Variable expansion load
        (
            "tests/unit/config/simple_variable_expansion.yml",
            {
                "normal": "sa",
                "path": "stuff2",
                "server": "http://localhost:8080",
                "working_dir": "stuff1",
            },
            {
                "VAR1": "stuff1",
                "VAR2": "stuff2",
            },
            set(["VAR1", "UNSET_VAR3", "VAR2"]),
        ),
        (
            "tests/unit/config/complex_variable_expansion.yml",
            {
                "foo": "bar",
                "bar": "stuff1",
                "nested": {
                    "hi": "hello",
                    "vanilla_val_1": "vanillaValue",
                    "vanilla_val_2": "vanilla$Value",
                    "vanilla_val_3": "vanilla${Value",
                    "vanilla_val_4": "vani}lla${Value",
                    "vanilla_val_5": "vanillaValue}",
                    "vanilla_val_6": "vanilla$Value}",
                    "password": "stuff2",
                    "password_1": "stuff3",
                    "password_2": "$VARNONEXISTENT",
                    "array": [
                        "one",
                        "two",
                        "stuff4",
                        "three$array",
                        [
                            "in_one",
                            "in_two",
                            "stuff5",
                            "test_urlstuff6",
                            "test_urlstuff7/action",
                        ],
                        {
                            "inner_array": [
                                "in_ar_one",
                                "stuff8",
                                "test_url$vanillavar",
                                "test_urlstuff9vanillaVarstuff10",
                                "${VAR11}",
                            ]
                        },
                    ],
                },
            },
            {
                "VAR1": "stuff1",
                "VAR2": "stuff2",
                "VAR3": "stuff3",
                "VAR4": "stuff4",
                "VAR5": "stuff5",
                "VAR6": "stuff6",
                "VAR7": "stuff7",
                "VAR8": "stuff8",
                "VAR9": "stuff9",
                "VAR10": "stuff10",
                "VAR11": "stuff11",
            },
            set(
                [
                    "VAR1",
                    "VAR2",
                    "VAR3",
                    "VAR4",
                    "VAR5",
                    "VAR6",
                    "VAR7",
                    "VAR8",
                    "VAR9",
                    "VAR10",
                    # VAR11 is escaped and hence not referenced
                    "VARNONEXISTENT",
                ]
            ),
        ),
    ],
)
def test_load_success(pytestconfig, filename, golden_config, env, referenced_env_vars):
    filepath = pytestconfig.rootpath / filename

    if referenced_env_vars is not None:
        raw_config = yaml.safe_load(filepath.read_text())
        assert list_referenced_env_variables(raw_config) == referenced_env_vars

    with mock.patch.dict(os.environ, env):
        loaded_config = load_config_file(filepath)
        assert loaded_config == golden_config

        # TODO check referenced env vars


@pytest.mark.parametrize(
    "filename,env,error_type",
    [
        (
            # Variable expansion error
            "tests/unit/config/bad_variable_expansion.yml",
            {},
            expandvars.ParameterNullOrNotSet,
        ),
        (
            # Missing file
            "tests/unit/config/this_file_does_not_exist.yml",
            {},
            ConfigurationError,
        ),
        (
            # Unknown extension
            "tests/unit/config/bad_extension.whatevenisthis",
            {},
            ConfigurationError,
        ),
        (
            # Variable expansion error
            "tests/unit/config/bad_complex_variable_expansion.yml",
            {},
            expandvars.MissingClosingBrace,
        ),
    ],
)
def test_load_error(pytestconfig, filename, env, error_type):
    filepath = pytestconfig.rootpath / filename

    with mock.patch.dict(os.environ, env):
        with pytest.raises(error_type):
            _ = load_config_file(filepath)
