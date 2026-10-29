"""Tests for the executor worker memory limit and the pipeline's reported peak."""

from collections.abc import Iterator
from pathlib import Path
from typing import Optional
from unittest.mock import Mock, patch

import psutil
import pytest

from datahub.executor.execution.worker_memory import (
    IngestionMemoryInfo,
    MemoryLimit,
    read_memory_limit,
)


@pytest.fixture
def host_memory() -> Iterator[Mock]:
    with patch.object(psutil, "virtual_memory") as virtual_memory:
        virtual_memory.return_value.total = 256 << 30
        yield virtual_memory


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _proc_cgroup(tmp_path: Path, text: Optional[str]) -> str:
    path = tmp_path / "proc" / "self" / "cgroup"
    if text is not None:
        _write(path, text)
    return str(path)


class TestReadMemoryLimit:
    def test_cgroup_v2_reads_own_cgroup_not_the_mount_root(
        self, tmp_path: Path, host_memory: Mock
    ) -> None:
        """With a visible cgroup path the pod's limit wins over an unlimited root."""
        root = tmp_path / "cgroup"
        _write(root / "cgroup.controllers", "memory\n")
        _write(root / "memory.max", "max\n")
        _write(root / "kubepods" / "pod1" / "memory.max", "64424509440\n")
        proc = _proc_cgroup(tmp_path, "0::/kubepods/pod1\n")

        assert read_memory_limit(str(root), proc) == MemoryLimit(
            64424509440, "cgroup v2"
        )

    def test_cgroup_v2_private_namespace_uses_the_mount(
        self, tmp_path: Path, host_memory: Mock
    ) -> None:
        """In a cgroup namespace /proc/self/cgroup says '/', so the mount is the limit."""
        root = tmp_path / "cgroup"
        _write(root / "cgroup.controllers", "memory\n")
        _write(root / "memory.max", "8589934592\n")
        proc = _proc_cgroup(tmp_path, "0::/\n")

        assert read_memory_limit(str(root), proc) == MemoryLimit(
            8589934592, "cgroup v2"
        )

    def test_cgroup_v1_limit(self, tmp_path: Path, host_memory: Mock) -> None:
        root = tmp_path / "cgroup"
        _write(root / "memory" / "memory.limit_in_bytes", "9223372036854771712\n")
        _write(
            root / "memory" / "docker" / "abc" / "memory.limit_in_bytes",
            "4294967296\n",
        )
        proc = _proc_cgroup(tmp_path, "3:memory:/docker/abc\n0::/\n")

        assert read_memory_limit(str(root), proc) == MemoryLimit(
            4294967296, "cgroup v1"
        )

    def test_unlimited_cgroup_falls_back_to_host_memory(
        self, tmp_path: Path, host_memory: Mock
    ) -> None:
        """'max' on v2 and the huge v1 sentinel both mean no cgroup limit."""
        root = tmp_path / "cgroup"
        _write(root / "cgroup.controllers", "memory\n")
        _write(root / "memory.max", "max\n")
        proc = _proc_cgroup(tmp_path, "0::/\n")

        assert read_memory_limit(str(root), proc) == MemoryLimit(256 << 30, "host")

    def test_no_cgroup_mount_falls_back_to_host_memory(
        self, tmp_path: Path, host_memory: Mock
    ) -> None:
        proc = _proc_cgroup(tmp_path, None)
        assert read_memory_limit(str(tmp_path / "missing"), proc) == MemoryLimit(
            256 << 30, "host"
        )

    def test_unknown_when_nothing_is_readable(self, tmp_path: Path) -> None:
        with patch.object(psutil, "virtual_memory", side_effect=RuntimeError):
            limit = read_memory_limit(
                str(tmp_path / "missing"), _proc_cgroup(tmp_path, None)
            )
        assert limit == MemoryLimit(None, "unknown")
        assert limit.text is None

    def test_text_is_human_readable_with_its_source(self) -> None:
        assert MemoryLimit(64424509440, "cgroup v2").text == "64.42 GB (cgroup v2)"


class TestIngestionMemoryInfo:
    def test_remembers_the_latest_peak_from_the_cli_report(self) -> None:
        """The pipeline prints its CLI report every minute; the last peak wins."""
        info = IngestionMemoryInfo()
        info.record_log_line(" 'mem_info': '1.2 GB',\n")
        assert info.last_reported_peak is None

        info.record_log_line(" 'peak_memory_usage': '1.5 GB',\n")
        info.record_log_line("[2026-10-16 08:37:50,431] INFO some other line\n")
        info.record_log_line(" 'peak_memory_usage': '59.1 GB',\n")
        assert info.last_reported_peak == "59.1 GB"

    def test_ignores_a_report_without_a_value(self) -> None:
        info = IngestionMemoryInfo()
        info.record_log_line(" 'peak_memory_usage': None,\n")
        assert info.last_reported_peak is None
