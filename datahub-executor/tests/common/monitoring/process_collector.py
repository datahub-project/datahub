from collections import namedtuple
from typing import Any, Dict, List
from unittest.mock import Mock, mock_open, patch

from prometheus_client import Metric

from datahub_executor.common.monitoring.process_collector import (
    CGROUP1_CPU_LIMIT_PERIOD_PATH,
    CGROUP1_CPU_LIMIT_QUOTA_PATH,
    CGROUP1_MEM_LIMIT_PATH,
    CGROUP2_CPU_LIMIT_PATH,
    CGROUP2_MEM_LIMIT_PATH,
    CGROUP_MAX_READ_SIZE,
    DatahubProcessCollector,
)

COLLECTOR = DatahubProcessCollector()

SAMPLE = namedtuple("SAMPLE", ["name", "labels", "value"])
METRIC = namedtuple("METRIC", ["name", "samples"])


class TestDatahubProcessCollector:
    def setup_method(self) -> None:
        pass

    def assertMetricsEqual(self, expected: List[METRIC], actual: List[Metric]) -> None:
        for e, a in zip(expected, actual, strict=False):
            assert e.name == a.name
            for se, sa in zip(e.samples, a.samples, strict=False):
                assert se.name == sa.name
                assert se.labels == sa.labels
                assert se.value == sa.value

    def mock_open_from_dict(self, filename: str, examples: Dict[str, Any]) -> Any:
        data = examples.get(filename, None)
        if data is None:
            mo = mock_open()
            mo.side_effect = FileNotFoundError
            return mo
        else:
            return mock_open(read_data=examples[filename])

    @patch("psutil.cpu_count")
    def test_collect_cpu_limit_processgroup(self, cpu_count: Mock) -> None:
        examples: List[Dict[str, Any]] = [
            # No cgroup files exist
            {
                "input": {
                    "host_limit": 8,
                    CGROUP2_CPU_LIMIT_PATH: None,
                    CGROUP1_CPU_LIMIT_QUOTA_PATH: None,
                    CGROUP1_CPU_LIMIT_PERIOD_PATH: None,
                },
                "output": [
                    METRIC(
                        "datahub_executor_cpu_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_cpu_limit_processgroup",
                                labels={"collector": "host"},
                                value=800,
                            )
                        ],
                    ),
                ],
            },
            # cgroup v2 file exists, and its value higher than host's value
            {
                "input": {
                    "host_limit": 16,
                    CGROUP2_CPU_LIMIT_PATH: "10000000 100000",
                    CGROUP1_CPU_LIMIT_QUOTA_PATH: None,
                    CGROUP1_CPU_LIMIT_PERIOD_PATH: None,
                },
                "output": [
                    METRIC(
                        "datahub_executor_cpu_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_cpu_limit_processgroup",
                                labels={"collector": "host"},
                                value=1600,
                            )
                        ],
                    ),
                ],
            },
            # cgroup v2 file exists, and its value lower than host's value
            {
                "input": {
                    "host_limit": 8,
                    CGROUP2_CPU_LIMIT_PATH: "50000 100000",
                    CGROUP1_CPU_LIMIT_QUOTA_PATH: None,
                    CGROUP1_CPU_LIMIT_PERIOD_PATH: None,
                },
                "output": [
                    METRIC(
                        "datahub_executor_cpu_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_cpu_limit_processgroup",
                                labels={"collector": "cgroup2"},
                                value=50.0,
                            )
                        ],
                    ),
                ],
            },
            # cgroup v2 file exists, and its value matches host's value
            {
                "input": {
                    "host_limit": 8,
                    CGROUP2_CPU_LIMIT_PATH: "max 100000",
                    CGROUP1_CPU_LIMIT_QUOTA_PATH: None,
                    CGROUP1_CPU_LIMIT_PERIOD_PATH: None,
                },
                "output": [
                    METRIC(
                        "datahub_executor_cpu_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_cpu_limit_processgroup",
                                labels={"collector": "host"},
                                value=800,
                            )
                        ],
                    ),
                ],
            },
            # cgroup v2 file exists with nonsensical values
            {
                "input": {
                    "host_limit": 8,
                    CGROUP2_CPU_LIMIT_PATH: "0 0",
                    CGROUP1_CPU_LIMIT_QUOTA_PATH: None,
                    CGROUP1_CPU_LIMIT_PERIOD_PATH: None,
                },
                "output": [
                    METRIC(
                        "datahub_executor_cpu_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_cpu_limit_processgroup",
                                labels={"collector": "host"},
                                value=800,
                            )
                        ],
                    ),
                ],
            },
            # cgroup v1 file exists, and its value higher than host's value
            {
                "input": {
                    "host_limit": 8,
                    CGROUP2_CPU_LIMIT_PATH: None,
                    CGROUP1_CPU_LIMIT_QUOTA_PATH: "1000000",
                    CGROUP1_CPU_LIMIT_PERIOD_PATH: "100000",
                },
                "output": [
                    METRIC(
                        "datahub_executor_cpu_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_cpu_limit_processgroup",
                                labels={"collector": "host"},
                                value=800,
                            )
                        ],
                    ),
                ],
            },
            # cgroup v1 file exists, and its value lower than host's value
            {
                "input": {
                    "host_limit": 8,
                    CGROUP2_CPU_LIMIT_PATH: None,
                    CGROUP1_CPU_LIMIT_QUOTA_PATH: "10000",
                    CGROUP1_CPU_LIMIT_PERIOD_PATH: "100000",
                },
                "output": [
                    METRIC(
                        "datahub_executor_cpu_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_cpu_limit_processgroup",
                                labels={"collector": "cgroup"},
                                value=10.0,
                            )
                        ],
                    ),
                ],
            },
        ]

        for ex in examples:
            with patch(
                "builtins.open",
                side_effect=lambda x: self.mock_open_from_dict(
                    x,
                    ex["input"],  # noqa: B023
                ).return_value,
                create=True,
            ):
                cpu_count.return_value = ex["input"]["host_limit"]
                actual = COLLECTOR._collect_cpu_limit_processgroup()
                self.assertMetricsEqual(ex["output"], list(actual))

    @patch("psutil.virtual_memory")
    def test_collect_mem_limit_processgroup(self, meminfo: Mock) -> None:
        svmem = namedtuple("svmem", ["total", "used"])
        examples: List[Dict[str, Any]] = [
            # No cgroup files exist
            {
                "input": {
                    "host_limit": 8589934592,
                    CGROUP1_MEM_LIMIT_PATH: None,
                    CGROUP2_MEM_LIMIT_PATH: None,
                },
                "output": [
                    METRIC(
                        "datahub_executor_memory_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_memory_limit_processgroup",
                                labels={"collector": "host"},
                                value=8589934592,
                            )
                        ],
                    ),
                ],
            },
            # cgroup v2 file exists, and its value higher than host's value
            {
                "input": {
                    "host_limit": 8589934592,
                    CGROUP1_MEM_LIMIT_PATH: None,
                    CGROUP2_MEM_LIMIT_PATH: "17179869184",
                },
                "output": [
                    METRIC(
                        "datahub_executor_memory_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_memory_limit_processgroup",
                                labels={"collector": "host"},
                                value=8589934592,
                            )
                        ],
                    ),
                ],
            },
            # cgroup v2 file exists, and its value lower than host's value
            {
                "input": {
                    "host_limit": 8589934592,
                    CGROUP1_MEM_LIMIT_PATH: None,
                    CGROUP2_MEM_LIMIT_PATH: "1073741824",
                },
                "output": [
                    METRIC(
                        "datahub_executor_memory_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_memory_limit_processgroup",
                                labels={"collector": "cgroup2"},
                                value=1073741824,
                            )
                        ],
                    ),
                ],
            },
            # cgroup v1 file exists, and its value higher than host's value
            {
                "input": {
                    "host_limit": 8589934592,
                    CGROUP1_MEM_LIMIT_PATH: "12884901888",
                    CGROUP2_MEM_LIMIT_PATH: None,
                },
                "output": [
                    METRIC(
                        "datahub_executor_memory_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_memory_limit_processgroup",
                                labels={"collector": "host"},
                                value=8589934592,
                            )
                        ],
                    ),
                ],
            },
            # cgroup v1 file exists, and its value lower than host's value
            {
                "input": {
                    "host_limit": 8589934592,
                    CGROUP1_MEM_LIMIT_PATH: "524288000",
                    CGROUP2_MEM_LIMIT_PATH: None,
                },
                "output": [
                    METRIC(
                        "datahub_executor_memory_limit_processgroup",
                        [
                            SAMPLE(
                                "datahub_executor_memory_limit_processgroup",
                                labels={"collector": "cgroup"},
                                value=524288000,
                            )
                        ],
                    ),
                ],
            },
        ]

        for ex in examples:
            with patch(
                "builtins.open",
                side_effect=lambda x: self.mock_open_from_dict(
                    x,
                    ex["input"],  # noqa: B023
                ).return_value,
                create=True,
            ):
                meminfo.return_value = svmem(ex["input"]["host_limit"], 1024)
                actual = COLLECTOR._collect_mem_limit_processgroup()
                self.assertMetricsEqual(ex["output"], list(actual))

    @patch("psutil.Process")
    def test_collect_cpu_usage_processgroup(self, proc: Mock) -> None:
        pcputimes = namedtuple("pcputimes", ["user", "system"])
        proc.return_value.cpu_times.return_value = pcputimes(1, 10)

        class DummyProcess:
            def __init__(self, pid: int, user: float, system: float) -> None:
                self.pid = pid
                self.cpuinfo = pcputimes(user, system)

            def cpu_times(self) -> Any:
                return self.cpuinfo

        proc.return_value.children.return_value = [
            DummyProcess(1000, 10, 11),
            DummyProcess(1001, 20, 12),
            DummyProcess(1002, 30, 13),
        ]

        actual = COLLECTOR._collect_cpu_usage_processgroup()
        expected = [
            METRIC(
                "datahub_executor_cpu_usage_process",
                [
                    SAMPLE(
                        "datahub_executor_cpu_usage_process",
                        labels={"kind": "user"},
                        value=1,
                    ),
                    SAMPLE(
                        "datahub_executor_cpu_usage_process",
                        labels={"kind": "system"},
                        value=10,
                    ),
                ],
            ),
            METRIC(
                "datahub_executor_cpu_usage_processgroup",
                [
                    SAMPLE(
                        "datahub_executor_cpu_usage_processgroup",
                        labels={"kind": "user"},
                        value=60,
                    ),
                    SAMPLE(
                        "datahub_executor_cpu_usage_processgroup",
                        labels={"kind": "system"},
                        value=36,
                    ),
                ],
            ),
        ]
        self.assertMetricsEqual(expected, list(actual))

    @patch("psutil.Process")
    def test_collect_mem_usage_processgroup(self, proc: Mock) -> None:
        pmem = namedtuple("pmem", ["rss", "vms"])
        proc.return_value.memory_info.return_value = pmem(12345, 12000000)

        class DummyProcess:
            def __init__(self, pid: int, rss: int, vms: int) -> None:
                self.pid = pid
                self.meminfo = pmem(rss, vms)

            def memory_info(self) -> Any:
                return self.meminfo

        proc.return_value.children.return_value = [
            DummyProcess(1000, 1111, 12000000),
            DummyProcess(1001, 2222, 12000000),
            DummyProcess(1002, 3333, 12000000),
        ]

        actual = COLLECTOR._collect_mem_usage_processgroup()
        expected = [
            METRIC(
                "datahub_executor_memory_usage_processgroup",
                [
                    SAMPLE(
                        "datahub_executor_memory_usage_processgroup",
                        labels={"kind": "rss"},
                        value=19011,
                    ),
                    SAMPLE(
                        "datahub_executor_memory_usage_processgroup",
                        labels={"kind": "vms"},
                        value=48000000,
                    ),
                ],
            ),
        ]
        self.assertMetricsEqual(expected, list(actual))

    @patch("psutil.cpu_percent")
    @patch("psutil.cpu_count")
    def test_collect_cpu_usage_host(self, cpu_percent: Mock, cpu_count: Mock) -> None:
        cpu_percent.return_value = 0.25
        cpu_count.return_value = 8

        actual = COLLECTOR._collect_cpu_usage_host()
        expected = [
            METRIC(
                "datahub_executor_cpu_usage_host",
                [
                    SAMPLE("datahub_executor_cpu_usage_host", labels={}, value=8),
                ],
            ),
            METRIC(
                "datahub_executor_cpu_limit_host",
                [
                    SAMPLE("datahub_executor_cpu_limit_host", labels={}, value=25),
                ],
            ),
        ]
        cpu_percent.assert_called_once()
        cpu_count.assert_called_once()
        self.assertMetricsEqual(expected, list(actual))

    @patch("psutil.virtual_memory")
    def test_collect_mem_usage_host(self, meminfo: Mock) -> None:
        svmem = namedtuple("svmem", ["total", "used"])
        meminfo.return_value = svmem(123456, 321)

        actual = COLLECTOR._collect_mem_usage_host()
        expected = [
            METRIC(
                "datahub_executor_memory_limit_host",
                [
                    SAMPLE(
                        "datahub_executor_memory_limit_host", labels={}, value=123456
                    ),
                ],
            ),
            METRIC(
                "datahub_executor_memory_usage_host",
                [
                    SAMPLE("datahub_executor_memory_usage_host", labels={}, value=321),
                ],
            ),
        ]

        meminfo.assert_called_once()
        self.assertMetricsEqual(expected, list(actual))

    @patch("psutil.net_io_counters")
    def test_collect_network_info(self, netio: Mock) -> None:
        snetio = namedtuple(
            "snetio",
            [
                "bytes_sent",
                "bytes_recv",
                "packets_sent",
                "packets_recv",
                "errin",
                "errout",
                "dropin",
                "dropout",
            ],
        )

        netio.return_value = {
            "lo": snetio(1, 2, 3, 4, 5, 6, 7, 8),
            "eth0": snetio(11, 12, 13, 14, 15, 16, 17, 18),
        }

        actual = COLLECTOR._collect_network_info()
        expected = [
            METRIC(
                "datahub_executor_net_bytes_sent",
                [
                    SAMPLE("datahub_executor_net_bytes_sent", {"interface": "lo"}, 1),
                    SAMPLE(
                        "datahub_executor_net_bytes_sent", {"interface": "eth0"}, 11
                    ),
                ],
            ),
            METRIC(
                "datahub_executor_net_bytes_recv",
                [
                    SAMPLE("datahub_executor_net_bytes_recv", {"interface": "lo"}, 2),
                    SAMPLE(
                        "datahub_executor_net_bytes_recv", {"interface": "eth0"}, 12
                    ),
                ],
            ),
            METRIC(
                "datahub_executor_net_packets_sent",
                [
                    SAMPLE("datahub_executor_net_packets_sent", {"interface": "lo"}, 3),
                    SAMPLE(
                        "datahub_executor_net_packets_sent",
                        {"interface": "eth0"},
                        13,
                    ),
                ],
            ),
            METRIC(
                "datahub_executor_net_packets_recv",
                [
                    SAMPLE("datahub_executor_net_packets_recv", {"interface": "lo"}, 4),
                    SAMPLE(
                        "datahub_executor_net_packets_recv",
                        {"interface": "eth0"},
                        14,
                    ),
                ],
            ),
            METRIC(
                "datahub_executor_net_errors_in",
                [
                    SAMPLE("datahub_executor_net_errors_in", {"interface": "lo"}, 5),
                    SAMPLE("datahub_executor_net_errors_in", {"interface": "eth0"}, 15),
                ],
            ),
            METRIC(
                "datahub_executor_net_errors_out",
                [
                    SAMPLE("datahub_executor_net_errors_out", {"interface": "lo"}, 6),
                    SAMPLE(
                        "datahub_executor_net_errors_out", {"interface": "eth0"}, 16
                    ),
                ],
            ),
            METRIC(
                "datahub_executor_net_dropin",
                [
                    SAMPLE("datahub_executor_net_dropin", {"interface": "lo"}, 7),
                    SAMPLE("datahub_executor_net_dropin", {"interface": "eth0"}, 17),
                ],
            ),
            METRIC(
                "datahub_executor_net_dropout",
                [
                    SAMPLE("datahub_executor_net_dropout", {"interface": "lo"}, 8),
                    SAMPLE("datahub_executor_net_dropout", {"interface": "eth0"}, 18),
                ],
            ),
        ]

        netio.assert_called_once_with(pernic=True)
        self.assertMetricsEqual(expected, list(actual))

    @patch("psutil.disk_partitions")
    def test_collect_disk_info(self, part: Mock) -> None:
        sdiskpart = namedtuple("sdiskpart", ["device", "mountpoint", "fstype"])
        part.return_value = [
            sdiskpart("/dev/nvme0n1p1", "/mnt/dir1", "xfs"),
            sdiskpart("/dev/nvme0n1p2", "/mnt/dir2", "xfs"),
            sdiskpart("sysfs", "/sys", "sysfs"),
        ]

        def get_usage_by_partition(mountpoint: str) -> Any:
            sdiskusage = namedtuple("sdiskusage", ["total", "used", "free"])
            example = {
                "/mnt/dir1": sdiskusage(1234567, 1111, 1233456),
                "/mnt/dir2": sdiskusage(4567890, 2222, 3334434),
            }
            return example.get(mountpoint)

        with patch("psutil.disk_usage", get_usage_by_partition):
            actual = COLLECTOR._collect_disk_info()

        expected = [
            METRIC(
                "datahub_executor_disk_bytes_total",
                [
                    SAMPLE(
                        "datahub_executor_disk_bytes_total",
                        {
                            "dev": "/dev/nvme0n1p1",
                            "mountpoint": "/mnt/dir1",
                            "fstype": "xfs",
                        },
                        value=1234567,
                    ),
                    SAMPLE(
                        "datahub_executor_disk_bytes_total",
                        {
                            "dev": "/dev/nvme0n1p2",
                            "mountpoint": "/mnt/dir2",
                            "fstype": "xfs",
                        },
                        value=4567890,
                    ),
                ],
            ),
            METRIC(
                "datahub_executor_disk_bytes_used",
                [
                    SAMPLE(
                        "datahub_executor_disk_bytes_used",
                        {
                            "dev": "/dev/nvme0n1p1",
                            "mountpoint": "/mnt/dir1",
                            "fstype": "xfs",
                        },
                        value=1111,
                    ),
                    SAMPLE(
                        "datahub_executor_disk_bytes_used",
                        {
                            "dev": "/dev/nvme0n1p2",
                            "mountpoint": "/mnt/dir2",
                            "fstype": "xfs",
                        },
                        value=2222,
                    ),
                ],
            ),
            METRIC(
                "datahub_executor_disk_bytes_free",
                [
                    SAMPLE(
                        "datahub_executor_disk_bytes_free",
                        {
                            "dev": "/dev/nvme0n1p1",
                            "mountpoint": "/mnt/dir1",
                            "fstype": "xfs",
                        },
                        value=1233456,
                    ),
                    SAMPLE(
                        "datahub_executor_disk_bytes_free",
                        {
                            "dev": "/dev/nvme0n1p2",
                            "mountpoint": "/mnt/dir2",
                            "fstype": "xfs",
                        },
                        value=3334434,
                    ),
                ],
            ),
        ]

        part.assert_called_once()
        self.assertMetricsEqual(expected, list(actual))

    def test_read_cgroup_str(self) -> None:
        with patch("builtins.open", mock_open(read_data="val1 val2")) as mocked_open:
            # Non-existent file should return empty string
            mocked_open.side_effect = FileNotFoundError
            val = COLLECTOR._read_cgroup_str("/some/non-existent-file")
            assert val == ""

            # Existing file should return its raw contents
            mocked_open.side_effect = None
            val = COLLECTOR._read_cgroup_str("/some/existing-file")
            assert val == "val1 val2"

    def test_read_cgroup_str_long(self) -> None:
        with patch(
            "builtins.open", mock_open(read_data="x" * (CGROUP_MAX_READ_SIZE + 1))
        ) as mocked_open:
            # Extremely long value should be truncated
            mocked_open.side_effect = None
            val = COLLECTOR._read_cgroup_str("/some/existing-file")
            assert val == ""

            # Simulate random exception
            mocked_open.side_effect = Exception
            val = COLLECTOR._read_cgroup_str("/some/existing-file")
            assert val == ""

    def test_read_cgroup_int(self) -> None:
        with patch("builtins.open", mock_open(read_data="123")) as mocked_open:
            # Read non-existent file
            mocked_open.side_effect = FileNotFoundError
            val = COLLECTOR._read_cgroup_int("/some/non-existing-file")
            assert val == -1

            # Read normal file
            mocked_open.side_effect = None
            val = COLLECTOR._read_cgroup_int("/some/existing-file")
            assert val == 123

        with patch("builtins.open", mock_open(read_data="string")):
            # Simulate exception
            val = COLLECTOR._read_cgroup_int("/some/existing-file")
            assert val == -1
