import logging
from typing import Any, Dict, Iterable, List, Optional

import psutil
from prometheus_client import REGISTRY, CollectorRegistry, Metric
from prometheus_client.metrics_core import GaugeMetricFamily
from prometheus_client.registry import Collector

logger = logging.getLogger(__name__)

CGROUP_PREFIX = "/sys/fs/cgroup"

CGROUP2_CPU_LIMIT_PATH = f"{CGROUP_PREFIX}/cpu.max"
CGROUP2_MEM_LIMIT_PATH = f"{CGROUP_PREFIX}/memory.max"

CGROUP1_CPU_LIMIT_QUOTA_PATH = f"{CGROUP_PREFIX}/cpu/cpu.cfs_quota_us"
CGROUP1_CPU_LIMIT_PERIOD_PATH = f"{CGROUP_PREFIX}/cpu/cpu.cfs_period_us"
CGROUP1_MEM_LIMIT_PATH = f"{CGROUP_PREFIX}/memory/memory.limit_in_bytes"

CGROUP_MAX_READ_SIZE = 4096


class DatahubProcessCollector(Collector):
    def __init__(
        self, registry: CollectorRegistry = REGISTRY, namespace: Optional[str] = None
    ):
        self.namespace = namespace if namespace else "datahub_executor"
        registry.register(self)

    def _gauge(self, name: str, descr: str, labels: List[str]) -> GaugeMetricFamily:
        return GaugeMetricFamily(
            f"{self.namespace}_{name}",
            descr,
            labels=labels,
        )

    def _read_cgroup_str(self, path: str) -> str:
        try:
            with open(path) as f:
                val = f.read(CGROUP_MAX_READ_SIZE + 1)
                if len(val) > CGROUP_MAX_READ_SIZE:
                    logger.error("value too long when reading a cgroup file")
                    return ""
                return val.strip()
        except FileNotFoundError:
            return ""
        except Exception:
            logger.exception("Exception when reading a cgroup file {path}")
            return ""

    def _read_cgroup_int(self, path: str) -> int:
        try:
            raw = self._read_cgroup_str(path)
            if raw == "max":
                return -1
            elif raw != "":
                return int(raw)
        except Exception:
            logger.exception("Exception when reading a cgroup file {path}")
        return -1

    def _collect_mem_usage_processgroup(self) -> Iterable[Metric]:
        parent = psutil.Process()
        parent_meminfo = parent.memory_info()

        total_rss = parent_meminfo.rss
        total_vms = parent_meminfo.vms

        for child in parent.children(recursive=True):
            child_meminfo = child.memory_info()
            total_rss += child_meminfo.rss
            total_vms += child_meminfo.vms

        metric = self._gauge(
            "memory_usage_processgroup",
            "Memory usage by the entire executor process group",
            ["kind"],
        )
        metric.add_metric(["rss"], value=total_rss)
        metric.add_metric(["vms"], value=total_vms)
        return [metric]

    def _collect_mem_usage_process(self) -> Iterable[Metric]:
        meminfo = psutil.Process().memory_info()
        metric = self._gauge(
            "memory_usage_process",
            "Memory usage by the main executor process",
            labels=["kind"],
        )
        metric.add_metric(["rss"], value=meminfo.rss)
        metric.add_metric(["vms"], value=meminfo.vms)
        return [metric]

    def _collect_mem_limit_processgroup(self) -> Iterable[Metric]:
        metric = self._gauge(
            "memory_limit_processgroup",
            "Maximum memory available to the executor process",
            labels=["collector"],
        )

        values: List[Dict[str, Any]] = []
        values.append({"value": psutil.virtual_memory().total, "collector": "host"})

        # Try fetching limit from cgroup v2 api first
        val = self._read_cgroup_int(CGROUP2_MEM_LIMIT_PATH)
        if val > 0:
            values.append({"value": val, "collector": "cgroup2"})

        # If not available -- fall back to cgroup v1 api
        val = self._read_cgroup_int(CGROUP1_MEM_LIMIT_PATH)
        if val > 0:
            values.append({"value": val, "collector": "cgroup"})

        # Select the least of host's and cgroup's values. ECS sets cgroup
        # limit to 8 exabyte when the limit is not supplied by the user, which
        # hides actual host's capacity.
        minval = min(values, key=lambda x: x["value"])

        metric.add_metric([minval["collector"]], value=int(minval["value"]))
        return [metric]

    def _collect_mem_usage_host(self) -> Iterable[Metric]:
        hostinfo = psutil.virtual_memory()
        metrics = []
        m = self._gauge("memory_limit_host", "Total memory available on the host", [])
        m.add_metric([], value=hostinfo.total)
        metrics.append(m)

        m = self._gauge("memory_usage_host", "Total memory available on the host", [])
        m.add_metric([], value=hostinfo.used)
        metrics.append(m)
        return metrics

    def _collect_memory_info(self) -> Iterable[Metric]:
        metrics: List[Metric] = []
        metrics += self._collect_mem_usage_processgroup()
        metrics += self._collect_mem_usage_process()
        metrics += self._collect_mem_limit_processgroup()
        metrics += self._collect_mem_usage_host()
        return metrics

    def _get_cpu_limit_cgroup_v2(self) -> float:
        try:
            real_limit = psutil.cpu_count() * 100
            vals = self._read_cgroup_str(CGROUP2_CPU_LIMIT_PATH).split()
            if vals[0] == "max":
                return real_limit
            quota = int(vals[0])
            period = int(vals[1])
            if quota > 0 and period > 0:
                limit = (quota / period) * 100
                return real_limit if limit > real_limit else limit
            return -1
        except Exception:
            return -1

    def _get_cpu_limit_cgroup_v1(self) -> float:
        quota = self._read_cgroup_int(CGROUP1_CPU_LIMIT_QUOTA_PATH)
        period = self._read_cgroup_int(CGROUP1_CPU_LIMIT_PERIOD_PATH)
        if quota > 0 and period > 0:
            return (quota / period) * 100
        return -1

    def _collect_cpu_limit_processgroup(self) -> Iterable[Metric]:
        values: List[Dict[str, Any]] = []
        values.append({"value": (psutil.cpu_count() * 100), "collector": "host"})

        # Try cgroup v2 first (most common)
        val = self._get_cpu_limit_cgroup_v2()
        if val > 0:
            values.append({"value": val, "collector": "cgroup2"})

        # Try cgroup v1
        val = self._get_cpu_limit_cgroup_v1()
        if val > 0:
            values.append({"value": val, "collector": "cgroup"})

        minval = min(values, key=lambda x: x["value"])

        metric = self._gauge(
            "cpu_limit_processgroup",
            "CPU utilization limit in the container, in percent",
            ["collector"],
        )
        metric.add_metric([minval["collector"]], minval["value"])
        return [metric]

    def _collect_cpu_usage_processgroup(self) -> Iterable[Metric]:
        metrics = []

        parent = psutil.Process()
        parent_usage = parent.cpu_times()

        process = self._gauge(
            "cpu_usage_process",
            "CPU utilization by the process, in percent",
            ["kind"],
        )
        process.add_metric(["user"], value=parent_usage.user)
        process.add_metric(["system"], value=parent_usage.system)
        metrics.append(process)

        group_user = 0
        group_system = 0

        for child in parent.children(recursive=True):
            t = child.cpu_times()
            group_user += t.user
            group_system += t.system

        processgroup = self._gauge(
            "cpu_usage_processgroup",
            "CPU utilization by the process group, in percent",
            ["kind"],
        )
        processgroup.add_metric(["user"], value=group_user)
        processgroup.add_metric(["system"], value=group_system)
        metrics.append(processgroup)

        return metrics

    def _collect_cpu_usage_host(self) -> Iterable[Metric]:
        metrics = []
        m = self._gauge(
            "cpu_usage_host",
            "Total CPU utilization on the host, in percent",
            [],
        )
        m.add_metric([], value=psutil.cpu_percent())
        metrics.append(m)

        m = self._gauge(
            "cpu_limit_host",
            "CPU limit on the host, in percent",
            [],
        )
        m.add_metric([], psutil.cpu_count() * 100)
        metrics.append(m)

        return metrics

    def _collect_cpu_info(self) -> Iterable[Metric]:
        metrics: List[Metric] = []
        metrics += self._collect_cpu_usage_host()
        metrics += self._collect_cpu_usage_processgroup()
        metrics += self._collect_cpu_limit_processgroup()
        return metrics

    def _collect_network_info(self) -> Iterable[Metric]:
        metrics = []

        net_bytes_sent = self._gauge(
            "net_bytes_sent", "Bytes sent, per NIC", ["interface"]
        )
        metrics.append(net_bytes_sent)
        net_bytes_recv = self._gauge(
            "net_bytes_recv", "Bytes received, per NIC", ["interface"]
        )
        metrics.append(net_bytes_recv)

        net_packets_sent = self._gauge(
            "net_packets_sent", "Packets sent, per NIC", ["interface"]
        )
        metrics.append(net_packets_sent)
        net_packets_recv = self._gauge(
            "net_packets_recv", "Packets received, per NIC", ["interface"]
        )
        metrics.append(net_packets_recv)

        net_errors_in = self._gauge(
            "net_errors_in",
            "Errors when receiving packet in, per NIC",
            ["interface"],
        )
        metrics.append(net_errors_in)
        net_errors_out = self._gauge(
            "net_errors_out", "Errors when sending packet out, per NIC", ["interface"]
        )
        metrics.append(net_errors_out)

        net_dropin = self._gauge(
            "net_dropin", "Packets dropped on RX, per NIC", ["interface"]
        )
        metrics.append(net_dropin)
        net_dropout = self._gauge(
            "net_dropout", "Packets dropped on TX, per NIC", ["interface"]
        )
        metrics.append(net_dropout)

        netinfo = psutil.net_io_counters(pernic=True)
        for nic, netio in netinfo.items():
            net_bytes_sent.add_metric([nic], value=netio.bytes_sent)
            net_bytes_recv.add_metric([nic], value=netio.bytes_recv)
            net_packets_sent.add_metric([nic], value=netio.packets_sent)
            net_packets_recv.add_metric([nic], value=netio.packets_recv)
            net_errors_in.add_metric([nic], value=netio.errin)
            net_errors_out.add_metric([nic], value=netio.errout)
            net_dropin.add_metric([nic], value=netio.dropin)
            net_dropout.add_metric([nic], value=netio.dropout)

        return metrics

    def _collect_disk_info(self) -> Iterable[Metric]:
        metrics = []
        fsignore = set(["proc", "sysfs", "cgroup", "cgroup2", "devpts", "mqueue"])

        disk_bytes_total = self._gauge(
            "disk_bytes_total",
            "Total bytes, per device",
            ["dev", "mountpoint", "fstype"],
        )
        metrics.append(disk_bytes_total)

        disk_bytes_used = self._gauge(
            "disk_bytes_used",
            "Used bytes, per device",
            ["dev", "mountpoint", "fstype"],
        )
        metrics.append(disk_bytes_used)

        disk_bytes_free = self._gauge(
            "disk_bytes_free",
            "Free bytes, per device",
            ["dev", "mountpoint", "fstype"],
        )
        metrics.append(disk_bytes_free)

        for part in psutil.disk_partitions():
            if part.fstype not in fsignore:
                usage = psutil.disk_usage(part.mountpoint)
                disk_bytes_total.add_metric(
                    [part.device, part.mountpoint, part.fstype], value=usage.total
                )
                disk_bytes_used.add_metric(
                    [part.device, part.mountpoint, part.fstype], value=usage.used
                )
                disk_bytes_free.add_metric(
                    [part.device, part.mountpoint, part.fstype], value=usage.free
                )

        return metrics

    def collect(self) -> Iterable[Metric]:
        metrics: List[Metric] = []
        metrics += self._collect_memory_info()
        metrics += self._collect_network_info()
        metrics += self._collect_disk_info()
        metrics += self._collect_cpu_info()
        return metrics
