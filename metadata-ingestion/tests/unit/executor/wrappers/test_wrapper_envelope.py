"""Tests for the venv-CLI version gate that decides whether the wrapper forwards
the raw stdin envelope or pre-substitutes ${VAR} into plain YAML."""

from pathlib import Path
from typing import Optional
from unittest.mock import Mock, patch

from datahub.executor.execution.wrapper_common import (
    get_venv_datahub_version,
    supports_stdin_envelope,
)


def _metadata_output(stdout: str) -> Mock:
    return Mock(stdout=stdout, returncode=0)


class TestVenvVersionGate:
    def test_reads_installed_distribution_version(self) -> None:
        with patch(
            "datahub.executor.execution.wrapper_common.subprocess.run",
            return_value=_metadata_output("1.7.0.8\n"),
        ) as run:
            assert get_venv_datahub_version(Path("/venv/bin/python")) == (1, 7, 0, 8)
        assert run.call_args.args[0][0] == "/venv/bin/python"

    def test_epoch_and_dev_suffix_are_tolerated(self) -> None:
        with patch(
            "datahub.executor.execution.wrapper_common.subprocess.run",
            return_value=_metadata_output("1!0.0.0.dev0\n"),
        ):
            assert get_venv_datahub_version(Path("python")) == (0, 0, 0)

    def test_missing_distribution_returns_none(self) -> None:
        with patch(
            "datahub.executor.execution.wrapper_common.subprocess.run",
            return_value=_metadata_output("Traceback: PackageNotFoundError"),
        ):
            assert get_venv_datahub_version(Path("python")) is None

    def test_envelope_gate_at_threshold(self) -> None:
        cases: list[tuple[Optional[tuple[int, ...]], bool]] = [
            ((1, 5, 0, 15), True),
            ((1, 5, 0, 14), False),
            ((1, 7, 0, 8), True),
            ((0, 0, 0), False),
            (None, False),
        ]
        for version, expected in cases:
            with patch(
                "datahub.executor.execution.wrapper_common.get_venv_datahub_version",
                return_value=version,
            ):
                assert supports_stdin_envelope(Path("python")) is expected, version
