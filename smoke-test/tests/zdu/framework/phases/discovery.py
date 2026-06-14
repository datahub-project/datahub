from __future__ import annotations

import logging
import time
from datetime import datetime

from .base import Phase, PhaseResult
from ..context import TestContext
from ..datahub_client import DataHubClient
from ..docker_compose import DockerComposeClient

log = logging.getLogger(__name__)


class DiscoveryPhase(Phase):
    name = "discovery"

    def __init__(
        self,
        docker: DockerComposeClient,
        datahub: DataHubClient,
        gms_service: str = "datahub-gms",
        old_image_tag: str = "debug",
        new_image_tag: str = "debug",
    ) -> None:
        self._docker = docker
        self._datahub = datahub
        self._gms_service = gms_service
        self._old_image_tag = old_image_tag
        self._new_image_tag = new_image_tag

    def run(self, ctx: TestContext) -> PhaseResult:
        start = datetime.utcnow()
        t0 = time.monotonic()
        try:
            images = self._docker.get_all_service_images()
            ctx.version_snapshot = images
            log.info("Discovered %d services: %s", len(images), images)

            if self._gms_service not in images:
                return PhaseResult(
                    phase_name=self.name,
                    status="failed",
                    started_at=start,
                    duration_s=time.monotonic() - t0,
                    error=f"{self._gms_service} not found in docker compose services",
                )

            self._datahub.wait_healthy(timeout_s=60)
            log.info("GMS at %s is healthy", ctx.gms_url)

            return PhaseResult(
                phase_name=self.name,
                status="passed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                details={
                    "services": images,
                    "old_image_tag": self._old_image_tag,
                    "new_image_tag": self._new_image_tag,
                },
            )
        except Exception as exc:
            log.exception("DiscoveryPhase failed")
            return PhaseResult(
                phase_name=self.name,
                status="failed",
                started_at=start,
                duration_s=time.monotonic() - t0,
                error=str(exc),
            )
