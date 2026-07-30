"""Unit tests for DockerComposeClient — uses mocked subprocess.Popen."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.docker_compose import DockerComposeClient


@pytest.fixture
def client() -> DockerComposeClient:
    return DockerComposeClient(
        project_dir="/tmp/x",
        compose_files=["docker-compose.yml"],
        profiles=["debug"],
    )


class TestRunUpgradeJob:
    def test_env_overrides_become_dash_e_flags(
        self, client: DockerComposeClient
    ) -> None:
        with patch("tests.zdu.framework.docker_compose.subprocess.Popen") as mock_popen:
            mock_popen.return_value = MagicMock()
            client.run_upgrade_job(
                env_overrides={"FOO": "bar", "BAZ": "qux"},
                service="system-update-debug",
            )
            args, kwargs = mock_popen.call_args
            cmd = args[0]
            # `-e FOO=bar` and `-e BAZ=qux` must be in the cmd, both before the service name.
            assert "-e" in cmd
            assert "FOO=bar" in cmd
            assert "BAZ=qux" in cmd
            # service name is the LAST positional before any extra_args.
            assert "system-update-debug" in cmd

    def test_extra_args_appended_after_service(
        self, client: DockerComposeClient
    ) -> None:
        with patch("tests.zdu.framework.docker_compose.subprocess.Popen") as mock_popen:
            mock_popen.return_value = MagicMock()
            client.run_upgrade_job(
                env_overrides={},
                service="system-update-debug",
                extra_args=["-u", "SystemUpdateBlocking"],
            )
            cmd = mock_popen.call_args[0][0]
            i = cmd.index("system-update-debug")
            assert cmd[i + 1 :] == ["-u", "SystemUpdateBlocking"]

    def test_compose_env_sets_subprocess_env(self, client: DockerComposeClient) -> None:
        # compose_env drives Compose's YAML variable substitution, NOT container env.
        with patch("tests.zdu.framework.docker_compose.subprocess.Popen") as mock_popen:
            mock_popen.return_value = MagicMock()
            client.run_upgrade_job(
                env_overrides={"FOO": "bar"},
                service="system-update-debug",
                compose_env={"DATAHUB_UPDATE_VERSION": "v1.6.0"},
            )
            kwargs = mock_popen.call_args.kwargs
            # subprocess gets an env= dict that contains the compose substitution var
            assert "env" in kwargs
            assert kwargs["env"]["DATAHUB_UPDATE_VERSION"] == "v1.6.0"
            # And the parent env is preserved (other PATH-like vars survive)
            assert "PATH" in kwargs["env"]

    def test_compose_env_does_not_become_dash_e_flag(
        self, client: DockerComposeClient
    ) -> None:
        # compose_env is for Compose substitution, NOT for the container.
        # It must NOT appear as a -e flag in the cmd.
        with patch("tests.zdu.framework.docker_compose.subprocess.Popen") as mock_popen:
            mock_popen.return_value = MagicMock()
            client.run_upgrade_job(
                env_overrides={},
                service="system-update-debug",
                compose_env={"DATAHUB_UPDATE_VERSION": "v1.6.0"},
            )
            cmd = mock_popen.call_args[0][0]
            assert "DATAHUB_UPDATE_VERSION=v1.6.0" not in cmd

    def test_compose_env_none_does_not_override_parent_env(
        self, client: DockerComposeClient
    ) -> None:
        # When compose_env is None (default), Popen should not receive an env=
        # kwarg at all — preserves prior behaviour where Popen inherits the
        # parent's full environment by default.
        with patch("tests.zdu.framework.docker_compose.subprocess.Popen") as mock_popen:
            mock_popen.return_value = MagicMock()
            client.run_upgrade_job(
                env_overrides={},
                service="system-update-debug",
            )
            kwargs = mock_popen.call_args.kwargs
            assert "env" not in kwargs


class TestRecreateService:
    def test_runs_compose_up_d_with_compose_env(
        self, client: DockerComposeClient
    ) -> None:
        with patch("tests.zdu.framework.docker_compose.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            with patch.object(client, "wait_healthy") as mock_wait:
                client.recreate_service(
                    service="datahub-gms-debug",
                    compose_env={"DATAHUB_GMS_VERSION": "v1.6.0"},
                    timeout_s=120,
                )
                # First subprocess.run call is `docker compose up -d <service>`
                first_call = mock_run.call_args_list[0]
                cmd = first_call[0][0]
                assert "up" in cmd
                assert "-d" in cmd
                assert "datahub-gms-debug" in cmd
                # compose_env was passed via env=
                env_kw = first_call.kwargs.get("env")
                assert env_kw is not None
                assert env_kw["DATAHUB_GMS_VERSION"] == "v1.6.0"
                # Then wait_healthy was called for the same service
                mock_wait.assert_called_once_with("datahub-gms-debug", timeout_s=120)

    def test_no_compose_env_does_not_pass_env_kwarg(
        self, client: DockerComposeClient
    ) -> None:
        with patch("tests.zdu.framework.docker_compose.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            with patch.object(client, "wait_healthy"):
                client.recreate_service(
                    service="datahub-gms-debug",
                    compose_env=None,
                    timeout_s=10,
                )
                first_call = mock_run.call_args_list[0]
                env_kw = first_call.kwargs.get("env")
                assert env_kw is None  # subprocess inherits parent env

    def test_logs_recreate_duration_before_wait_healthy(
        self, client: DockerComposeClient, caplog
    ) -> None:
        """G16 — recreate_service must log when ``docker compose up -d`` returns
        so the silent 10-30s container restart window is no longer indeterminate
        in the run log.
        """
        with patch("tests.zdu.framework.docker_compose.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            with patch.object(client, "wait_healthy"):
                with caplog.at_level(
                    "INFO", logger="tests.zdu.framework.docker_compose"
                ):
                    client.recreate_service(
                        service="datahub-gms-debug",
                        compose_env={"DATAHUB_GMS_VERSION": "v1.6.0"},
                        timeout_s=10,
                    )
        assert any(
            "docker compose up -d datahub-gms-debug returned in" in rec.message
            and "waiting for healthy" in rec.message
            for rec in caplog.records
        ), (
            f"missing recreate-duration log line in {[r.message for r in caplog.records]}"
        )

    def test_wait_healthy_logs_duration_on_success(
        self, client: DockerComposeClient, caplog
    ) -> None:
        """G16 — wait_healthy must log when the service goes healthy, otherwise
        the polling phase is silent (5-15s) and the next log line looks like
        it arrived out of nowhere.
        """
        with patch.object(client, "_run") as mock_ps:
            mock_ps.return_value = MagicMock(stdout="Up (healthy)")
            with caplog.at_level("INFO", logger="tests.zdu.framework.docker_compose"):
                client.wait_healthy("datahub-gms-debug", timeout_s=10)
        assert any(
            "Service datahub-gms-debug healthy after" in rec.message
            for rec in caplog.records
        ), f"missing wait_healthy success log in {[r.message for r in caplog.records]}"

    def test_failed_up_d_raises(self, client: DockerComposeClient) -> None:
        with patch("tests.zdu.framework.docker_compose.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout="",
                stderr="image not found",
            )
            with patch.object(client, "wait_healthy"):
                with pytest.raises(RuntimeError, match="image not found"):
                    client.recreate_service(
                        service="datahub-gms-debug",
                        compose_env={"DATAHUB_GMS_VERSION": "missing-tag"},
                        timeout_s=10,
                    )


class TestGetServiceLogs:
    def test_returns_stdout_from_docker_compose_logs(
        self, client: DockerComposeClient
    ) -> None:
        with patch.object(DockerComposeClient, "_run") as mock_run:
            mock_run.return_value = MagicMock(stdout="line 1\nline 2\n", returncode=0)
            out = client.get_service_logs("datahub-gms-debug", tail=500)
        assert "line 1" in out
        # Verify the args contain --tail and service name.
        cmd = mock_run.call_args.args[0]
        assert "logs" in cmd
        assert "--tail" in cmd
        assert "500" in cmd
        assert "datahub-gms-debug" in cmd

    def test_returns_empty_string_on_failure(self, client: DockerComposeClient) -> None:
        with patch.object(DockerComposeClient, "_run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=1, stderr="err")
            out = client.get_service_logs("nonexistent-service")
        assert out == ""


class TestGetProjectName:
    """R4 — replaces hardcoded ``datahub_`` prefix with the live project name."""

    def test_returns_name_from_compose_config_json(
        self, client: DockerComposeClient
    ) -> None:
        with patch.object(DockerComposeClient, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout='{"name": "my-fork", "services": {}}', stderr=""
            )
            assert client.get_project_name() == "my-fork"
        # And the underlying compose call uses --format json so the JSON
        # parse path is what's exercised in production.
        cmd = mock_run.call_args.args[0]
        assert "config" in cmd
        assert "--format" in cmd
        assert "json" in cmd

    def test_raises_when_compose_config_fails(
        self, client: DockerComposeClient
    ) -> None:
        with patch.object(DockerComposeClient, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1, stdout="", stderr="invalid compose file"
            )
            with pytest.raises(RuntimeError, match="docker compose config failed"):
                client.get_project_name()

    def test_raises_when_output_is_not_json(self, client: DockerComposeClient) -> None:
        with patch.object(DockerComposeClient, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout="not json", stderr=""
            )
            with pytest.raises(RuntimeError, match="non-JSON output"):
                client.get_project_name()

    def test_raises_when_name_field_missing(self, client: DockerComposeClient) -> None:
        with patch.object(DockerComposeClient, "_run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout='{"services": {}}', stderr=""
            )
            with pytest.raises(RuntimeError, match="did not return a project name"):
                client.get_project_name()


class TestDownAndWipeVolumeQualification:
    """R4/R8 — ``wipe_volumes`` are short names; the project prefix comes from
    the live ``docker compose config`` so the wipe works on non-default
    clone-directory names (e.g. ``datahub-fork`` instead of ``datahub``).
    """

    def test_prepends_live_project_prefix_to_short_volume_names(
        self, client: DockerComposeClient
    ) -> None:
        # Mock compose down + get_project_name + the per-volume rm subprocess.
        with (
            patch.object(DockerComposeClient, "_run") as mock_run,
            patch(
                "tests.zdu.framework.docker_compose.subprocess.run"
            ) as mock_subprocess_run,
        ):
            # _run is used by down + get_project_name. Both return success.
            # We don't differentiate calls here; both share the mock.
            mock_run.return_value = MagicMock(
                returncode=0, stdout='{"name": "my-fork"}', stderr=""
            )
            # subprocess.run is used for the per-volume `docker volume rm`.
            mock_subprocess_run.return_value = MagicMock(
                returncode=0, stdout="", stderr=""
            )
            client.down_and_wipe(wipe_volumes=["osdata", "mysqldata"])

        # Each volume rm should have been called with <project>_<short>.
        rm_calls = [
            c.args[0]
            for c in mock_subprocess_run.call_args_list
            if c.args and c.args[0][:3] == ["docker", "volume", "rm"]
        ]
        assert ["docker", "volume", "rm", "my-fork_osdata"] in rm_calls
        assert ["docker", "volume", "rm", "my-fork_mysqldata"] in rm_calls

    def test_underscore_in_short_name_is_NOT_treated_as_qualified(
        self, client: DockerComposeClient
    ) -> None:
        """Regression guard: the old ``"_" in short_name`` heuristic broke
        on legitimate multi-word short names. Now the project prefix is
        always prepended.
        """
        with (
            patch.object(DockerComposeClient, "_run") as mock_run,
            patch(
                "tests.zdu.framework.docker_compose.subprocess.run"
            ) as mock_subprocess_run,
        ):
            mock_run.return_value = MagicMock(
                returncode=0, stdout='{"name": "datahub"}', stderr=""
            )
            mock_subprocess_run.return_value = MagicMock(
                returncode=0, stdout="", stderr=""
            )
            client.down_and_wipe(wipe_volumes=["my_data"])

        rm_calls = [
            c.args[0]
            for c in mock_subprocess_run.call_args_list
            if c.args and c.args[0][:3] == ["docker", "volume", "rm"]
        ]
        # Must be qualified — NOT passed through as-is.
        assert ["docker", "volume", "rm", "datahub_my_data"] in rm_calls
        assert ["docker", "volume", "rm", "my_data"] not in rm_calls
