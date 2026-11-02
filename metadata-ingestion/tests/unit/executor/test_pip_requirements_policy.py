import pathlib
from typing import Optional
from unittest.mock import AsyncMock, patch

import anyio
import pytest

from datahub.executor.common.env_config import (
    EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV,
    EXTRA_PIP_REQUIREMENTS_ENABLED_ENV,
)
from datahub.executor.execution.pip_requirements_policy import (
    parse_allowlist,
    validate_extra_pip_requirements,
    validate_install_env_vars,
    validate_plugin_names,
)
from datahub.executor.execution.runner import (
    SubprocessRunner,
    VenvConfig,
    setup_venv,
)


@pytest.fixture(autouse=True)
def _no_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(EXTRA_PIP_REQUIREMENTS_ENABLED_ENV, raising=False)
    monkeypatch.delenv(EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV, raising=False)


def _validate(entries: list[str]) -> None:
    validate_extra_pip_requirements(entries, entries)


class TestStructuralChecks:
    @pytest.mark.parametrize(
        "entry",
        [
            "pandas",
            "Pandas==2.1.0",
            "pkg[extra]>=1,<2 ; python_version>'3'",
            "pkg @ https://example.com/x.whl",
            "acryl-datahub-cloud[forms]@/acryl-cloud",
            "/local/checkout",
            "",
        ],
    )
    def test_package_specs_pass_without_a_policy(self, entry: str) -> None:
        _validate([entry])

    @pytest.mark.parametrize(
        "entry",
        [
            "--index-url https://evil.example/simple",
            "-r /etc/passwd",
            "-e git+https://evil.example/x",
            "pandas --hash=sha256:abc",
            "pandas\n--extra-index-url https://evil.example/simple",
            "pandas\r\n-r /run/secrets/token",
        ],
    )
    def test_options_and_injected_lines_are_rejected(self, entry: str) -> None:
        with pytest.raises(ValueError, match="Rejected extra_pip_requirements"):
            _validate([entry])

    def test_error_quotes_the_raw_template_not_the_expanded_value(self) -> None:
        with pytest.raises(ValueError) as excinfo:
            validate_extra_pip_requirements(
                ["${PIP_LINE}"], ["--index-url https://user:tok-secret@x/simple"]
            )
        assert "${PIP_LINE}" in str(excinfo.value)
        assert "tok-secret" not in str(excinfo.value)


class TestPluginNames:
    def test_plugin_and_custom_source_names_pass(self) -> None:
        validate_plugin_names(["snowflake", "my_module.MySource", None, ""])

    @pytest.mark.parametrize(
        "name", ["x] @ https://evil.example/evil.whl#", "../../etc", "a b", "a;b"]
    )
    def test_names_that_escape_the_extras_bracket_are_rejected(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid plugin name"):
            validate_plugin_names([name])


class TestInstallEnvVars:
    def test_connector_env_vars_pass(self) -> None:
        validate_install_env_vars({"DBT_PROFILES_DIR": "/tmp/p", "AWS_REGION": "x"})

    @pytest.mark.parametrize(
        "name",
        ["UV_INDEX_URL", "uv_extra_index_url", "PIP_INDEX_URL", "UV_CONFIG_FILE"],
    )
    def test_index_and_tool_settings_are_rejected(self, name: str) -> None:
        with pytest.raises(ValueError, match="not accepted in a recipe"):
            validate_install_env_vars({name: "https://evil.example/simple"})


class TestDisabledFlag:
    def test_any_requirement_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EXTRA_PIP_REQUIREMENTS_ENABLED_ENV, "false")
        with pytest.raises(ValueError, match="disabled"):
            _validate(["pandas"])

    def test_empty_list_is_fine(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EXTRA_PIP_REQUIREMENTS_ENABLED_ENV, "false")
        _validate([])
        _validate([""])


class TestAllowlist:
    def test_bare_name_allows_any_version_and_pinned_entry_is_exact(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(
            EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV, "pandas, requests==2.32.5"
        )
        _validate(["Pandas[performance]>=2", "requests==2.32.5"])
        for bad in ["requests==2.0", "numpy", "pandas @ https://example.com/p.whl"]:
            with pytest.raises(ValueError, match="not on this executor's package"):
                _validate([bad])

    def test_json_form_with_direct_references_and_paths(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(
            EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV,
            '["pkg>=1,<2", "acryl-datahub-cloud[forms]@/acryl-cloud", "/datahub-integrations-service"]',
        )
        _validate(
            [
                "pkg<2,>=1",
                "acryl-datahub-cloud[other-extra]@/acryl-cloud",
                "/datahub-integrations-service",
            ]
        )
        for bad in ["pkg>=1", "acryl-datahub-cloud@/elsewhere", "/other-checkout"]:
            with pytest.raises(ValueError, match="not on this executor's package"):
                _validate([bad])

    def test_policy_applies_to_the_expanded_value(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV, "approved-plugin")
        validate_extra_pip_requirements(["${SPEC}"], ["approved-plugin==1.0"])
        with pytest.raises(ValueError, match=r"\$\{SPEC\}"):
            validate_extra_pip_requirements(["${SPEC}"], ["other-package"])

    @pytest.mark.parametrize("value", ["[not json", "[1, 2]"])
    def test_malformed_json_form_is_an_error(self, value: str) -> None:
        with pytest.raises(ValueError, match="JSON"):
            parse_allowlist(value)


async def _fake_uv(
    command: list[str],
    env: Optional[dict[str, str]] = None,
    cwd: Optional[str] = None,
) -> None:
    if "venv" in command:
        venv_path = pathlib.Path(command[-1])
        (venv_path / "bin").mkdir(parents=True, exist_ok=True)
        (venv_path / "bin" / "python").touch()


class TestSetupVenvEnforcesPolicy:
    def _run(self, config: VenvConfig, tmp_path: pathlib.Path) -> AsyncMock:
        runner = SubprocessRunner()
        execute = AsyncMock(side_effect=_fake_uv)
        with (
            patch("datahub.executor.execution.runner._find_uv", return_value="uv"),
            patch.object(runner, "execute", execute),
        ):
            anyio.run(setup_venv, config, runner, tmp_path)
        return execute

    def test_disallowed_requirement_stops_before_any_uv_command(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV, "pandas")
        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            extra_pip_requirements=["innocent-looking-package"],
        )
        with pytest.raises(ValueError, match="not on this executor's package"):
            self._run(config, tmp_path)
        assert not list(tmp_path.iterdir())

    def test_plugin_injection_stops_before_install(
        self, tmp_path: pathlib.Path
    ) -> None:
        config = VenvConfig(
            version="latest",
            main_plugin="snowflake",
            extra_pip_plugins=["x] @ https://evil.example/evil.whl#"],
        )
        with pytest.raises(ValueError, match="Invalid plugin name"):
            self._run(config, tmp_path)
        assert not list(tmp_path.iterdir())

    def test_index_override_in_extra_env_vars_stops_before_install(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV, "pandas")
        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            extra_pip_requirements=["pandas"],
            extra_env_vars={"UV_INDEX_URL": "https://evil.example/simple"},
        )
        with pytest.raises(ValueError, match="not accepted in a recipe"):
            self._run(config, tmp_path)
        assert not list(tmp_path.iterdir())

    def test_recipe_env_vars_do_not_reach_install_commands(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # HOME / XDG_CONFIG_HOME decide where uv reads its config, including the
        # package index the image was built with; a recipe must not move them.
        monkeypatch.setenv(EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV, "pandas")
        monkeypatch.setenv("HOME", "/executor-home")
        monkeypatch.setenv("XDG_CONFIG_HOME", "/executor-home/.config")
        recipe_env = {
            "HOME": str(tmp_path / "elsewhere"),
            "XDG_CONFIG_HOME": str(tmp_path / "elsewhere"),
            "MY_SOURCE_SETTING": "kept-for-the-run",
        }
        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            extra_pip_requirements=["pandas"],
            extra_env_vars=recipe_env,
        )
        runner = SubprocessRunner()
        execute = AsyncMock(side_effect=_fake_uv)
        with (
            patch("datahub.executor.execution.runner._find_uv", return_value="uv"),
            patch.object(runner, "execute", execute),
        ):
            reference = anyio.run(setup_venv, config, runner, tmp_path)

        install_envs = [
            call.kwargs["env"]
            for call in execute.call_args_list
            if "install" in call.args[0]
        ]
        assert install_envs
        for env in install_envs:
            assert env["HOME"] == "/executor-home"
            assert env["XDG_CONFIG_HOME"] == "/executor-home/.config"
            assert "MY_SOURCE_SETTING" not in env
            assert env["VIRTUAL_ENV"].startswith(str(tmp_path))
        assert reference.extra_envs() == recipe_env

    def test_caller_requirements_file_is_not_policed(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(EXTRA_PIP_REQUIREMENTS_ENABLED_ENV, "false")
        req_file = tmp_path / "requirements.txt"
        req_file.write_text("dbt-core\n")
        config = VenvConfig(
            requirements_file=req_file, extra_pip_requirements=["ignored"]
        )
        execute = self._run(config, tmp_path)
        assert any("install" in call.args[0] for call in execute.call_args_list)
