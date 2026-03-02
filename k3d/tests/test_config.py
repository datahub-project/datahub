"""Tests for dh.config — K3dConfig frozen dataclass."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from dh.config import K3dConfig


class TestDefaults:
    def test_default_cluster_name(self, monkeypatch):
        monkeypatch.delenv("K3D_CLUSTER_NAME", raising=False)
        cfg = K3dConfig()
        assert cfg.cluster_name == "datahub-dev"

    def test_env_cluster_name(self, monkeypatch):
        monkeypatch.setenv("K3D_CLUSTER_NAME", "my-cluster")
        cfg = K3dConfig()
        assert cfg.cluster_name == "my-cluster"

    def test_default_datahub_version(self, monkeypatch):
        monkeypatch.delenv("DATAHUB_VERSION", raising=False)
        cfg = K3dConfig()
        assert cfg.datahub_version == "head"

    def test_env_datahub_version(self, monkeypatch):
        monkeypatch.setenv("DATAHUB_VERSION", "v0.12.0")
        cfg = K3dConfig()
        assert cfg.datahub_version == "v0.12.0"

    def test_default_ports(self, monkeypatch):
        monkeypatch.delenv("K3D_HTTP_PORT", raising=False)
        monkeypatch.delenv("K3D_HTTPS_PORT", raising=False)
        cfg = K3dConfig()
        assert cfg.http_port == "80"
        assert cfg.https_port == "443"

    def test_env_ports(self, monkeypatch):
        monkeypatch.setenv("K3D_HTTP_PORT", "8080")
        monkeypatch.setenv("K3D_HTTPS_PORT", "8443")
        cfg = K3dConfig()
        assert cfg.http_port == "8080"
        assert cfg.https_port == "8443"


class TestFrozen:
    def test_cannot_assign(self):
        cfg = K3dConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.cluster_name = "nope"  # type: ignore[misc]


class TestScriptDir:
    def test_points_to_k3d_root(self):
        cfg = K3dConfig()
        # config.py is at k3d/src/dh/config.py, so parent^3 = k3d/
        assert cfg.script_dir.is_dir()
        assert (cfg.script_dir / "src" / "dh" / "config.py").is_file()


class TestDnsProperties:
    def test_mysql_host(self):
        cfg = K3dConfig()
        assert cfg.mysql_host == "prereqs-mysql.datahub-infra.svc.cluster.local"

    def test_mysql_port(self):
        assert K3dConfig().mysql_port == "3306"

    def test_kafka_host(self):
        cfg = K3dConfig()
        assert cfg.kafka_host == "prereqs-kafka.datahub-infra.svc.cluster.local"

    def test_kafka_port(self):
        assert K3dConfig().kafka_port == "9092"

    def test_elasticsearch_host(self):
        cfg = K3dConfig()
        assert cfg.elasticsearch_host == "elasticsearch-master.datahub-infra.svc.cluster.local"

    def test_elasticsearch_port(self):
        assert K3dConfig().elasticsearch_port == "9200"

    def test_custom_infra_release(self):
        cfg = K3dConfig(infra_release="my-rel", infra_namespace="my-ns")
        assert cfg.mysql_host == "my-rel-mysql.my-ns.svc.cluster.local"
        assert cfg.kafka_host == "my-rel-kafka.my-ns.svc.cluster.local"
        assert cfg.elasticsearch_host == "elasticsearch-master.my-ns.svc.cluster.local"
