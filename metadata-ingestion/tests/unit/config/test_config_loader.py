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
