"""K3dConfig frozen dataclass — replaces common.sh constants."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class K3dConfig:
    cluster_name: str = field(
        default_factory=lambda: os.environ.get("K3D_CLUSTER_NAME", "datahub-dev")
    )
    infra_namespace: str = "datahub-infra"
    infra_release: str = "prereqs"
    helm_repo_name: str = "datahub"
    helm_repo_url: str = "https://helm.datahubproject.io/"
    datahub_version: str = field(
        default_factory=lambda: os.environ.get("DATAHUB_VERSION", "head")
    )
    http_port: str = field(
        default_factory=lambda: os.environ.get("K3D_HTTP_PORT", "80")
    )
    https_port: str = field(
        default_factory=lambda: os.environ.get("K3D_HTTPS_PORT", "443")
    )

    @property
    def script_dir(self) -> Path:
        """The k3d/ root directory (parent of src/)."""
        return Path(__file__).resolve().parent.parent.parent

    @property
    def mysql_host(self) -> str:
        return f"{self.infra_release}-mysql.{self.infra_namespace}.svc.cluster.local"

    @property
    def mysql_port(self) -> str:
        return "3306"

    @property
    def kafka_host(self) -> str:
        return f"{self.infra_release}-kafka.{self.infra_namespace}.svc.cluster.local"

    @property
    def kafka_port(self) -> str:
        return "9092"

    @property
    def elasticsearch_host(self) -> str:
        return f"elasticsearch-master.{self.infra_namespace}.svc.cluster.local"

    @property
    def elasticsearch_port(self) -> str:
        return "9200"
