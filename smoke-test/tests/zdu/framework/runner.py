from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .catchup_executor import CatchUpScenarioExecutor
from .config import ZDUTestConfig
from .constants import MAE_SERVICE, ZDU_SERVICES_IN_ORDER
from .context import TestContext, ValidationResult
from .datahub_client import DataHubClient
from .docker_compose import DockerComposeClient
from .es_client import ElasticsearchClient
from .failure_bundle import FailureBundleWriter
from .live_traffic_executor import LiveTrafficExecutor
from .mysql_client import MySQLClient
from .phase1_reindex_executor import Phase1ReindexExecutor
from .phases.base import PhaseResult
from .phases.build_images import BuildImagesPhase
from .phases.cleanup import CleanupPhase
from .phases.data_integrity_snapshot import DataIntegritySnapshotPhase
from .phases.discovery import DiscoveryPhase
from .phases.inject_traffic_dual import InjectTrafficDualPhase
from .phases.inject_traffic_pre import InjectTrafficPrePhase
from .phases.nuke_and_redeploy import NukeAndRedeployPhase
from .phases.preflight import PreflightPhase
from .phases.prepare_old_stack import PrepareOldStackPhase
from .phases.rolling_restart import RollingRestartPhase
from .phases.runtime_migration import RuntimeMigrationPhase
from .phases.seed import SeedPhase
from .phases.setup_old_stack import SetupOldStackPhase
from .phases.snapshot_t0 import SnapshotT0Phase
from .phases.snapshot_t1 import SnapshotT1Phase

# Tc112FaultInjectionPhase intentionally NOT imported / wired — see plan 19
# and the TC-109 (was TC-112) skip_reason. Kept as code only for a future
# fault-injection plan; superseded by SnapshotT1Phase for the data-integrity
# variant of TC-109.
from .phases.upgrade_blocking import UpgradeBlockingPhase
from .phases.upgrade_blocking_rerun import UpgradeBlockingReRunPhase
from .phases.upgrade_nonblocking import UpgradeNonBlockingPhase
from .phases.validation import ValidationPhase
from .reporter import Reporter
from .scenario_executor import ScenarioTypeRegistry
from .scenario_loader import ScenarioExecutor, ScenarioLoader, ZDUTestScenario
from .upgrade_jar_freshness import ensure_upgrade_jar_fresh

log = logging.getLogger(__name__)


@dataclass
class ZDUReport:
    config: ZDUTestConfig
    phase_results: list[PhaseResult] = field(default_factory=list)
    scenario_results: list[ValidationResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.utcnow)
    ended_at: datetime | None = None

    def phase(self, name: str) -> PhaseResult:
        for p in self.phase_results:
            if p.phase_name == name:
                return p
        raise KeyError(f"Phase {name!r} not found in report")

    def scenario_result(self, tc_number: int) -> ValidationResult:
        for r in self.scenario_results:
            if r.tc_number == tc_number:
                return r
        raise KeyError(f"TC #{tc_number} not found in report")

    @property
    def passed(self) -> bool:
        phase_ok = all(p.status != "failed" for p in self.phase_results)
        scenario_ok = all(
            r.status in ("PASS", "XFAIL", "XPASS", "SKIP")
            for r in self.scenario_results
        )
        return phase_ok and scenario_ok

    def summary(self) -> str:
        passed = sum(1 for r in self.scenario_results if r.status == "PASS")
        failed = sum(1 for r in self.scenario_results if r.status == "FAIL")
        return f"ZDU Test: {passed} PASS / {failed} FAIL — {'PASSED' if self.passed else 'FAILED'}"


class ZDUTestRunner:
    def __init__(
        self,
        config: ZDUTestConfig,
        scenarios: list[ZDUTestScenario] | None = None,
    ) -> None:
        # Make sure the host's datahub-upgrade.jar matches current source —
        # the system-update-debug compose service mounts that JAR directly,
        # so a stale build silently produces an empty mutator chain and
        # hangs the sweep until sweep_timeout_s. Set ZDU_SKIP_BOOTJAR=1 in
        # CI / pre-built environments to skip.
        ensure_upgrade_jar_fresh()

        self._config = config
        self._docker = DockerComposeClient(
            config.project_dir, config.compose_files, config.compose_profiles
        )
        self._datahub = DataHubClient(config.gms_url, config.gms_token)
        self._es = ElasticsearchClient(
            gms_url=config.gms_url,
            es_url=config.es_url,
            token=config.gms_token,
        )
        self._mysql = MySQLClient(
            host=config.mysql_host,
            port=config.mysql_port,
            user=config.mysql_user,
            password=config.mysql_password,
            database=config.mysql_database,
        )
        # Strategy registry — additional executors are registered per-suite
        # in subsequent commits as their scenario types land.
        self._registry = ScenarioTypeRegistry()
        # Suite N: per-URN aspect-migration scenarios share the non-blocking
        # phase with the sweep-invariant subset, so both dispatch through
        # ScenarioExecutor (which delegates to sweep_executor for the
        # ``scenario_type="sweep"`` branch).
        self._aspect_executor = ScenarioExecutor(self._datahub)
        self._registry.register("aspect_migration", self._aspect_executor)
        self._registry.register("sweep", self._aspect_executor)
        self._phase1_reindex_executor = Phase1ReindexExecutor()
        self._registry.register("phase1_reindex", self._phase1_reindex_executor)
        self._catchup_executor = CatchUpScenarioExecutor()
        self._registry.register("catch_up", self._catchup_executor)
        self._live_traffic_executor = LiveTrafficExecutor()
        self._registry.register("live_traffic", self._live_traffic_executor)

        self._bundle_writer = FailureBundleWriter(
            docker=self._docker,
            es=self._es,
            mysql=self._mysql,
            build_dir=Path(config.build_dir),
        )
        self._reporter = Reporter(config.report_path, bundle_writer=self._bundle_writer)

        if scenarios is None:
            loader = ScenarioLoader()
            scenarios = loader.load()  # auto-resolves: sheet URL → local CSV fallback

        if config.run_only_tc:
            scenarios = [s for s in scenarios if s.tc_number in config.run_only_tc]
        if config.skip_tc:
            scenarios = [s for s in scenarios if s.tc_number not in config.skip_tc]
        if config.suites:
            allowed = set(config.suites)
            scenarios = [s for s in scenarios if s.suite.value in allowed]

        self._scenarios = scenarios

    def run(self) -> ZDUReport:
        report = ZDUReport(config=self._config)
        ctx = TestContext(gms_url=self._config.gms_url)
        # Cache the active scenario list so cross-scenario validators
        # (e.g., Suite B TC-102 reading TC-101's expected_reindex_indices)
        # can look up siblings at validation time.
        ctx.all_scenarios = list(self._scenarios)

        # repo_root: project_dir is typically /<repo>/docker/profiles — go up two levels.
        # Falls back to cwd if the heuristic doesn't hold (e.g., custom ZDU_PROJECT_DIR).
        _project_path = Path(self._config.project_dir)
        if _project_path.name == "profiles" and _project_path.parent.name == "docker":
            _repo_root_for_build = _project_path.parent.parent
        else:
            _repo_root_for_build = Path.cwd()

        # Preflight — fail fast on missing stack / missing token / missing
        # authz-bypass env file BEFORE Phase 0 spends 5+ minutes building
        # images that downstream phases can't use anyway.
        if "preflight" not in self._config.skip_phases:
            log.info("▶ Phase: preflight")
            preflight_phase = PreflightPhase(project_dir=self._config.project_dir)
            preflight_result = preflight_phase.run(ctx, self._config)
            report.phase_results.append(preflight_result)
            if preflight_result.status == "failed" and self._config.fail_fast:
                log.error("Phase preflight FAILED — aborting (fail_fast=True)")
                report.ended_at = datetime.utcnow()
                report.scenario_results = ctx.validation_results
                self._reporter.write(report, ctx=ctx)
                return report
        else:
            log.info("Skipping phase: preflight")

        # Phase 0 (combined) — SetupOldStackPhase orchestrates build_images +
        # nuke_and_redeploy + token-refresh. Runs BEFORE downstream phases
        # construct because sub-step A mutates config.old_image_tag /
        # new_image_tag, and downstream phase constructors snapshot those.
        # Sub-step C also mutates config.gms_token in-memory — we call
        # _datahub.set_token afterwards to push the fresh credential into
        # the live session that downstream phases share.
        if "setup_old_stack" not in self._config.skip_phases:
            log.info("▶ Phase: setup_old_stack")
            setup_phase = SetupOldStackPhase(
                build_phase=BuildImagesPhase(
                    repo_root=_repo_root_for_build,
                    old_ref=self._config.build_old_ref,
                ),
                nuke_phase=NukeAndRedeployPhase(
                    docker=self._docker,
                    gms_service=self._config.gms_service,
                    health_url=self._config.gms_url,
                ),
                gms_url=self._config.gms_url,
            )
            setup_result = setup_phase.run(ctx, self._config)
            report.phase_results.append(setup_result)
            # Push refreshed token onto the live DataHubClient session so all
            # already-constructed phases (DiscoveryPhase, SeedPhase, etc.)
            # send the new bearer on subsequent calls.
            self._datahub.set_token(self._config.gms_token)
            if setup_result.status == "failed" and self._config.fail_fast:
                log.error("Phase setup_old_stack FAILED — aborting (fail_fast=True)")
                report.ended_at = datetime.utcnow()
                report.scenario_results = ctx.validation_results
                self._reporter.write(report, ctx=ctx)
                return report
        else:
            log.info("Skipping phase: setup_old_stack")

        # Stage 2 — PrepareOldStack ensures the running Compose stack matches
        # config.old_image_tag, recreating GMS if needed. Redundant when
        # SetupOldStack's nuke + redeploy already brought the stack up on OLD
        # (clean_build=True). Skip in that case to save ~25s/run; keep running
        # when clean_build is opted out (clean_build=False) since
        # PrepareOldStack is then the only thing ensuring GMS is on OLD.
        if "prepare_old_stack" in self._config.skip_phases:
            log.info("Skipping phase: prepare_old_stack")
        elif self._config.clean_build:
            log.info(
                "Skipping phase: prepare_old_stack "
                "(setup_old_stack already deployed on OLD)"
            )
        else:
            log.info("▶ Phase: prepare_old_stack")
            prepare_phase = PrepareOldStackPhase(
                docker=self._docker,
                services_to_restart=ZDU_SERVICES_IN_ORDER,
                gms_service=self._config.gms_service,
                health_url=self._config.gms_url,
                timeout_s=180,
            )
            prepare_result = prepare_phase.run(ctx, self._config)
            report.phase_results.append(prepare_result)
            if prepare_result.status == "failed" and self._config.fail_fast:
                log.error("Phase prepare_old_stack FAILED — aborting (fail_fast=True)")
                report.ended_at = datetime.utcnow()
                report.scenario_results = ctx.validation_results
                self._reporter.write(report, ctx=ctx)
                return report

        phases = [
            (
                "discovery",
                DiscoveryPhase(
                    self._docker,
                    self._datahub,
                    self._config.gms_service,
                    old_image_tag=self._config.old_image_tag,
                    new_image_tag=self._config.new_image_tag,
                ),
            ),
            (
                "seed",
                SeedPhase(
                    self._aspect_executor,
                    self._scenarios,
                    self._datahub,
                    self._docker,
                    self._mysql,
                    es=self._es,
                ),
            ),
            (
                "snapshot_t0",
                SnapshotT0Phase(
                    es=self._es,
                    mysql=self._mysql,
                    aspects_under_test=self._config.aspects_under_test,
                    upgrade_id_to_check=None,
                ),
            ),
            (
                "upgrade_blocking",
                UpgradeBlockingPhase(
                    docker=self._docker,
                    mysql=self._mysql,
                    gms_service=self._config.gms_service,
                    upgrade_service=self._config.upgrade_service,
                    timeout_s=self._config.sweep_timeout_s,
                    new_image_tag=self._config.new_image_tag,
                    build_images_root=self._config.build_images_root,
                ),
            ),
            (
                "upgrade_blocking_rerun",
                UpgradeBlockingReRunPhase(
                    docker=self._docker,
                    mysql=self._mysql,
                    gms_service=self._config.gms_service,
                    upgrade_service=self._config.upgrade_service,
                    timeout_s=self._config.sweep_timeout_s,
                    new_image_tag=self._config.new_image_tag,
                    build_images_root=self._config.build_images_root,
                ),
            ),
            (
                "snapshot_t1",
                SnapshotT1Phase(es=self._es),
            ),
            (
                "inject_traffic_pre",
                InjectTrafficPrePhase(
                    datahub=self._datahub,
                    mysql=self._mysql,
                    es=self._es,
                ),
            ),
            (
                "rolling_restart",
                RollingRestartPhase(
                    docker=self._docker,
                    mae_service=MAE_SERVICE,
                    gms_service=self._config.gms_service,
                    new_image_tag=self._config.new_image_tag,
                    build_images_root=self._config.build_images_root,
                ),
            ),
            (
                "inject_traffic_dual",
                InjectTrafficDualPhase(
                    datahub=self._datahub,
                    mysql=self._mysql,
                    es=self._es,
                ),
            ),
            (
                "upgrade_nonblocking",
                UpgradeNonBlockingPhase(
                    config=self._config,
                    docker=self._docker,
                    datahub=self._datahub,
                    mysql=self._mysql,
                ),
            ),
            (
                "runtime_migration",
                RuntimeMigrationPhase(datahub=self._datahub),
            ),
            (
                "data_integrity_snapshot",
                DataIntegritySnapshotPhase(es=self._es, mysql=self._mysql),
            ),
            # Plan 19's Tc112FaultInjectionPhase is intentionally NOT wired
            # in. See the plan doc + TC-112 skip_reason — the fault-injection
            # design can't break past production's
            # "No indices require incremental reindex" outer gate without
            # additional alias-routing pre-staging. Phase code, helpers,
            # validator, and tests stay as foundation for a future plan that
            # adds the stale-mapping pre-stage.
            (
                "validation",
                ValidationPhase(
                    registry=self._registry,
                    scenarios=self._scenarios,
                    es=self._es,
                ),
            ),
            (
                "cleanup",
                CleanupPhase(
                    docker=self._docker,
                    gms_service=self._config.gms_service,
                    health_url=self._config.gms_url,
                    rest_tag=self._config.cleanup_rest_tag,
                ),
            ),
        ]

        for phase_name, phase in phases:
            if phase_name in self._config.skip_phases:
                log.info("Skipping phase: %s", phase_name)
                continue

            log.info("▶ Phase: %s", phase_name)
            result = phase.run(ctx)
            report.phase_results.append(result)

            if result.status == "failed" and self._config.fail_fast:
                log.error("Phase %s FAILED — aborting (fail_fast=True)", phase_name)
                break

        report.ended_at = datetime.utcnow()
        report.scenario_results = ctx.validation_results
        self._reporter.write(report, ctx=ctx)
        return report
