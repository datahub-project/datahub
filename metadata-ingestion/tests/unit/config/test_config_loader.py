import os
from unittest import mock

import expandvars
import pytest

from datahub.configuration.common import ConfigurationError
from datahub.configuration.config_loader import load_config_file


@pytest.mark.parametrize(
    "filename,golden_config,env,error_type",
    [
        (
            # Basic YAML load
            "tests/unit/config/basic.yml",
            {"foo": "bar", "nested": {"array": ["one", "two"], "hi": "hello"}},
            {},
            None,
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
            None,
        ),
        (
            # Variable expansion error
            "tests/unit/config/bad_variable_expansion.yml",
            None,
            {},
            expandvars.ParameterNullOrNotSet,
        ),
        (
            # Missing file
            "tests/unit/config/this_file_does_not_exist.yml",
            None,
            {},
            ConfigurationError,
        ),
        (
            # Unknown extension
            "tests/unit/config/bad_extension.whatevenisthis",
            None,
            {},
            ConfigurationError,
        ),
        (
            # Variable expansion error
            "tests/unit/config/bad_complex_variable_expansion.yml",
            None,
            {},
            expandvars.MissingClosingBrace,
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
                                "stuff11",
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
            None,
        ),
    ],
)
def test_load(pytestconfig, filename, golden_config, env, error_type):
    filepath = pytestconfig.rootpath / filename

    with mock.patch.dict(os.environ, env):
        if error_type:
            with pytest.raises(error_type):
                _ = load_config_file(filepath)
        else:
            loaded_config = load_config_file(filepath)
            assert loaded_config == golden_config
