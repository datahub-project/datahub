from unittest.mock import MagicMock, patch

import pytest

from datahub.testing.docker_utils import wait_for_port


def _unready_services() -> MagicMock:
    services = MagicMock()
    services.wait_until_responsive.side_effect = RuntimeError("timeout")
    return services


@patch("datahub.testing.docker_utils.subprocess.run")
def test_wait_for_port_does_not_dump_logs_when_ready(mock_run: MagicMock) -> None:
    services = MagicMock()

    wait_for_port(
        docker_services=services,
        container_name="nifi1",
        container_port=8080,
    )

    services.wait_until_responsive.assert_called_once()
    mock_run.assert_not_called()


@patch("datahub.testing.docker_utils.subprocess.run")
def test_wait_for_port_dumps_logs_when_not_ready(mock_run: MagicMock) -> None:
    mock_run.return_value = MagicMock(returncode=0, stdout="boot log", stderr="")

    with pytest.raises(RuntimeError, match="timeout"):
        wait_for_port(
            docker_services=_unready_services(),
            container_name="nifi1",
            container_port=8080,
        )

    mock_run.assert_called_once()
    assert mock_run.call_args.args[0] == ["docker", "logs", "nifi1"]
    assert mock_run.call_args.kwargs["capture_output"] is True
    assert mock_run.call_args.kwargs["text"] is True


@patch("datahub.testing.docker_utils.subprocess.run")
def test_wait_for_port_still_raises_original_when_log_dump_fails(
    mock_run: MagicMock,
) -> None:
    mock_run.return_value = MagicMock(
        returncode=1, stdout="", stderr="No such container"
    )

    with pytest.raises(RuntimeError, match="timeout"):
        wait_for_port(
            docker_services=_unready_services(),
            container_name="nifi1",
            container_port=8080,
        )

    mock_run.assert_called_once()
