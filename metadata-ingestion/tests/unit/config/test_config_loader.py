import io
import json
import os
import pathlib
import textwrap
from unittest import mock

import deepdiff
import expandvars
import pytest
import yaml

from datahub.configuration.common import ConfigurationError
from datahub.configuration.config_loader import (
    EnvResolver,
    list_referenced_env_variables,
    load_config_file,
)


@pytest.mark.parametrize(
    "filename,golden_config,env,referenced_env_vars",
    [
        (
            # Basic YAML load
            "tests/unit/config/basic.yml",
            {
                "foo": "bar",
                "nested": {
                    "array": ["one", "two"],
                    "hi": "hello",
                    "numbers": {4: "four", 6: "six", "8": "eight"},
                },
            },
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
            {"VAR1", "UNSET_VAR3", "VAR2"},
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
            {
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
            },
        ),
    ],
)
def test_load_success(pytestconfig, filename, golden_config, env, referenced_env_vars):
    filepath = pytestconfig.rootpath / filename

    if referenced_env_vars is not None:
        raw_config = yaml.safe_load(filepath.read_text())
        assert list_referenced_env_variables(raw_config) == referenced_env_vars

    with mock.patch.dict(os.environ, env):
        loaded_config = load_config_file(filepath, resolve_env_vars=True)
        assert loaded_config == golden_config

        # TODO check referenced env vars


def test_load_strict_env_syntax() -> None:
    config = {
        "foo": "${BAR}",
        "baz": "$BAZ",
        "qux": "qux$QUX",
    }
    assert EnvResolver.list_referenced_variables(
        config,
        strict_env_syntax=True,
    ) == {"BAR"}

    assert EnvResolver(
        environ={
            "BAR": "bar",
        }
    ).resolve(config) == {
        "foo": "bar",
        "baz": "$BAZ",
        "qux": "qux$QUX",
    }


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

    with mock.patch.dict(os.environ, env), pytest.raises(error_type):
        _ = load_config_file(filepath)


class TestStdinEnvelopeWithSecrets:
    """Tests for the stdin envelope format: {"__recipe_yaml__": ..., "__secrets__": ...}."""

    def test_envelope_secrets_resolve_variables(self) -> None:
        """Secrets from stdin envelope should resolve ${VAR} in the recipe."""
        recipe_yaml = "source:\n  type: test\n  config:\n    password: ${DB_PASS}\n    host: localhost\n"
        envelope_json = json.dumps(
            {
                "__recipe_yaml__": recipe_yaml,
                "__secrets__": {"DB_PASS": "my_secret_password"},
            }
        )

        with mock.patch("sys.stdin", io.StringIO(envelope_json)):
            config = load_config_file("-", allow_stdin=True, resolve_env_vars=True)

        assert config["source"]["config"]["password"] == "my_secret_password"
        assert config["source"]["config"]["host"] == "localhost"

    def test_envelope_secrets_do_not_touch_os_environ(self) -> None:
        """Secrets from the envelope must NOT be written to os.environ."""
        recipe_yaml = "source:\n  type: test\n  config:\n    token: ${SECRET_TOKEN}\n"
        envelope_json = json.dumps(
            {
                "__recipe_yaml__": recipe_yaml,
                "__secrets__": {"SECRET_TOKEN": "tok_abc123"},
            }
        )

        # Ensure not in env before
        assert "SECRET_TOKEN" not in os.environ

        with mock.patch("sys.stdin", io.StringIO(envelope_json)):
            config = load_config_file("-", allow_stdin=True, resolve_env_vars=True)

        assert config["source"]["config"]["token"] == "tok_abc123"
        # Must NOT leak to os.environ
        assert "SECRET_TOKEN" not in os.environ

    def test_envelope_secrets_override_env_vars(self) -> None:
        """Secrets from the envelope should take priority over os.environ."""
        recipe_yaml = "source:\n  type: test\n  config:\n    key: ${MY_KEY}\n"
        envelope_json = json.dumps(
            {
                "__recipe_yaml__": recipe_yaml,
                "__secrets__": {"MY_KEY": "from_envelope"},
            }
        )

        with (
            mock.patch.dict(os.environ, {"MY_KEY": "from_env"}),
            mock.patch("sys.stdin", io.StringIO(envelope_json)),
        ):
            config = load_config_file("-", allow_stdin=True, resolve_env_vars=True)

        # Envelope secrets take priority
        assert config["source"]["config"]["key"] == "from_envelope"

    def test_envelope_falls_back_to_env_for_unresolved(self) -> None:
        """Variables not in __secrets__ should still resolve from os.environ."""
        recipe_yaml = "source:\n  type: test\n  config:\n    a: ${FROM_SECRETS}\n    b: ${FROM_ENV}\n"
        envelope_json = json.dumps(
            {
                "__recipe_yaml__": recipe_yaml,
                "__secrets__": {"FROM_SECRETS": "secret_val"},
            }
        )

        with (
            mock.patch.dict(os.environ, {"FROM_ENV": "env_val"}),
            mock.patch("sys.stdin", io.StringIO(envelope_json)),
        ):
            config = load_config_file("-", allow_stdin=True, resolve_env_vars=True)

        assert config["source"]["config"]["a"] == "secret_val"
        assert config["source"]["config"]["b"] == "env_val"

    def test_plain_yaml_stdin_still_works(self) -> None:
        """Plain YAML on stdin (no envelope) should work as before."""
        plain_yaml = "source:\n  type: test\n  config:\n    host: localhost\n"

        with mock.patch("sys.stdin", io.StringIO(plain_yaml)):
            config = load_config_file("-", allow_stdin=True, resolve_env_vars=True)

        assert config["source"]["config"]["host"] == "localhost"

    def test_plain_json_stdin_still_works(self) -> None:
        """Plain JSON on stdin (no __recipe_yaml__ key) should work as a recipe."""
        plain_json = json.dumps(
            {"source": {"type": "test", "config": {"host": "localhost"}}}
        )

        with mock.patch("sys.stdin", io.StringIO(plain_json)):
            config = load_config_file("-", allow_stdin=True, resolve_env_vars=True)

        assert config["source"]["config"]["host"] == "localhost"

    def test_envelope_with_empty_secrets(self) -> None:
        """Envelope with empty __secrets__ dict should work (no extra resolution)."""
        recipe_yaml = "source:\n  type: test\n  config:\n    host: localhost\n"
        envelope_json = json.dumps(
            {
                "__recipe_yaml__": recipe_yaml,
                "__secrets__": {},
            }
        )

        with mock.patch("sys.stdin", io.StringIO(envelope_json)):
            config = load_config_file("-", allow_stdin=True, resolve_env_vars=True)

        assert config["source"]["config"]["host"] == "localhost"

    def test_extra_env_vars_parameter_merges_with_envelope_secrets(self) -> None:
        """extra_env_vars parameter should merge with envelope __secrets__."""
        recipe_yaml = "source:\n  type: test\n  config:\n    a: ${FROM_PARAM}\n    b: ${FROM_ENVELOPE}\n"
        envelope_json = json.dumps(
            {
                "__recipe_yaml__": recipe_yaml,
                "__secrets__": {"FROM_ENVELOPE": "envelope_val"},
            }
        )

        with mock.patch("sys.stdin", io.StringIO(envelope_json)):
            config = load_config_file(
                "-",
                allow_stdin=True,
                resolve_env_vars=True,
                extra_env_vars={"FROM_PARAM": "param_val"},
            )

        assert config["source"]["config"]["a"] == "param_val"
        assert config["source"]["config"]["b"] == "envelope_val"


def test_write_file_directive(pytestconfig):
    filepath = pytestconfig.rootpath / "tests/unit/config/write_to_file_directive.yml"

    fake_ssl_key = "my-secret-key-value"

    with mock.patch.dict(os.environ, {"DATAHUB_SSL_KEY": fake_ssl_key}):
        loaded_config = load_config_file(
            filepath,
            squirrel_original_config=False,
            resolve_env_vars=True,
            process_directives=True,
        )

        # Check that the rest of the dict is unmodified.
        diff = deepdiff.DeepDiff(
            loaded_config,
            {
                "foo": "bar",
                "nested": {
                    "hi": "hello",
                    "another-key": "final-value",
                },
            },
            exclude_paths=[
                "root['nested']['ssl_cert']",
                "root['nested']['ssl_key']",
            ],
        )
        assert not diff

        # Check that the ssl_cert was written to a file.
        ssl_cert_path = loaded_config["nested"]["ssl_cert"]
        assert (
            pathlib.Path(ssl_cert_path).read_text()
            == textwrap.dedent(
                """
                -----BEGIN CERTIFICATE-----
                thisisnotarealcert
                -----END CERTIFICATE-----
                """
            ).lstrip()
        )

        # Check that the ssl_key was written to a file.
        ssl_key_path = loaded_config["nested"]["ssl_key"]
        assert pathlib.Path(ssl_key_path).read_text() == fake_ssl_key
