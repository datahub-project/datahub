from datahub.utilities import cpu_detection
from datahub.utilities.cpu_detection import (
    WorkerDecision,
    get_available_cpu_count,
    resolve_worker_count,
)


class TestCgroupParsing:
    def test_cgroup_v2_unlimited_returns_none(self):
        assert cpu_detection._parse_cgroup_v2_cpu_max("max 100000\n") is None

    def test_cgroup_v2_quota_returns_cpu_fraction(self):
        # "<quota> <period>" — 200000/100000 == 2 CPUs.
        assert cpu_detection._parse_cgroup_v2_cpu_max("200000 100000") == 2.0

    def test_cgroup_v2_fractional(self):
        assert cpu_detection._parse_cgroup_v2_cpu_max("150000 100000") == 1.5

    def test_cgroup_v1_unlimited_quota_returns_none(self):
        # quota of -1 means no limit.
        assert cpu_detection._parse_cgroup_v1_cpu_quota("-1", "100000") is None

    def test_cgroup_v1_quota_returns_cpu_fraction(self):
        assert cpu_detection._parse_cgroup_v1_cpu_quota("250000", "100000") == 2.5

    def test_malformed_input_returns_none(self):
        assert cpu_detection._parse_cgroup_v2_cpu_max("garbage") is None
        assert cpu_detection._parse_cgroup_v1_cpu_quota("x", "y") is None


class TestGetAvailableCpuCount:
    def test_takes_minimum_of_all_signals(self, monkeypatch):
        # A 2-CPU pod on a 64-core node: cgroup quota must win over cpu_count.
        monkeypatch.setattr(cpu_detection, "_cgroup_cpu_limit", lambda: 2.0)
        monkeypatch.setattr(cpu_detection, "_affinity_cpu_count", lambda: 8)
        monkeypatch.setattr(cpu_detection, "_os_cpu_count", lambda: 64)
        assert get_available_cpu_count() == 2

    def test_fractional_cgroup_is_floored(self, monkeypatch):
        monkeypatch.setattr(cpu_detection, "_cgroup_cpu_limit", lambda: 1.5)
        monkeypatch.setattr(cpu_detection, "_affinity_cpu_count", lambda: 8)
        monkeypatch.setattr(cpu_detection, "_os_cpu_count", lambda: 64)
        assert get_available_cpu_count() == 1

    def test_non_linux_fallback_to_cpu_count(self, monkeypatch):
        # No cgroup, no affinity (e.g. macOS): fall back to os.cpu_count().
        monkeypatch.setattr(cpu_detection, "_cgroup_cpu_limit", lambda: None)
        monkeypatch.setattr(cpu_detection, "_affinity_cpu_count", lambda: None)
        monkeypatch.setattr(cpu_detection, "_os_cpu_count", lambda: 8)
        assert get_available_cpu_count() == 8

    def test_always_at_least_one(self, monkeypatch):
        monkeypatch.setattr(cpu_detection, "_cgroup_cpu_limit", lambda: None)
        monkeypatch.setattr(cpu_detection, "_affinity_cpu_count", lambda: None)
        monkeypatch.setattr(cpu_detection, "_os_cpu_count", lambda: None)
        assert get_available_cpu_count() == 1


class TestResolveWorkerCount:
    def test_auto_detect_reserves_one_core(self):
        decision = resolve_worker_count(None, detected_cpus=8)
        assert isinstance(decision, WorkerDecision)
        assert decision.workers == 7
        assert not decision.clamped
        assert not decision.fell_back_to_serial

    def test_explicit_within_capacity_is_honored(self):
        decision = resolve_worker_count(4, detected_cpus=8)
        assert decision.workers == 4
        assert not decision.clamped

    def test_explicit_over_capacity_is_clamped(self):
        # The virtualization footgun: user asks for 100 on an 8-core box.
        decision = resolve_worker_count(100, detected_cpus=8)
        assert decision.workers == 7  # 8 - 1 reserved
        assert decision.clamped

    def test_single_core_falls_back_to_serial(self):
        decision = resolve_worker_count(4, detected_cpus=1)
        assert decision.workers == 1
        assert decision.fell_back_to_serial
