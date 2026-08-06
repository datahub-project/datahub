from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
from datetime import datetime
from typing import Iterator

log = logging.getLogger(__name__)


class DockerComposeClient:
    def __init__(
        self,
        project_dir: str,
        compose_files: list[str],
        profiles: list[str] | None = None,
    ) -> None:
        self._project_dir = project_dir
        self._compose_files = compose_files
        self._profiles = profiles or []

    # ── internal ────────────────────────────────────────────────────────────

    def _base_cmd(self) -> list[str]:
        cmd = ["docker", "compose"]
        for profile in self._profiles:
            cmd += ["--profile", profile]
        for f in self._compose_files:
            cmd += ["-f", f]
        return cmd

    def _run(self, args: list[str]) -> subprocess.CompletedProcess:
        cmd = self._base_cmd() + args
        log.debug("docker compose: %s", " ".join(cmd))
        return subprocess.run(
            cmd,
            cwd=self._project_dir,
            capture_output=True,
            text=True,
        )

    # ── inspection ──────────────────────────────────────────────────────────

    def get_service_env(self, service: str, keys: list[str]) -> dict[str, str]:
        """Read specific env vars from a running compose service via docker inspect."""
        result = self._run(["ps", "-q", service])
        container_id = (
            result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""
        )
        if not container_id:
            log.debug("get_service_env: no container found for %s", service)
            return {}
        try:
            raw = subprocess.run(
                ["docker", "inspect", "--format", "{{json .Config.Env}}", container_id],
                capture_output=True,
                text=True,
                check=True,
            )
            env_list: list[str] = json.loads(raw.stdout.strip())
        except Exception:
            log.debug("get_service_env: inspect failed for %s", service)
            return {}
        env_map = {}
        for entry in env_list:
            if "=" in entry:
                k, _, v = entry.partition("=")
                if k in keys:
                    env_map[k] = v
        return env_map

    def get_all_service_images(self) -> dict[str, str]:
        # Return the tag-form image reference each container was started with.
        # `compose ps {{.Image}}` returns a digest (`sha256:...`) when Compose
        # records the image by ID rather than tag, which breaks downstream
        # tag-substring checks (e.g. PrepareOldStackPhase's "old_tag in current[svc]"
        # — a digest never contains the tag string). Falling back to
        # `docker inspect --format {{.Config.Image}}` per container always
        # gives the tag form.
        result = self._run(["ps", "--format", "{{.Service}}\t{{.Name}}"])
        images: dict[str, str] = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split("\t", 1)
            if len(parts) != 2:
                continue
            service, container_name = parts
            inspect = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{.Config.Image}}",
                    container_name,
                ],
                capture_output=True,
                text=True,
            )
            if inspect.returncode == 0:
                images[service] = inspect.stdout.strip()
        return images

    def get_service_logs(self, service: str, tail: int = 2000) -> str:
        """Return stdout from ``docker compose logs --tail=N <service>``.

        Returns an empty string if the compose call fails (e.g., service not
        running). Used by ``FailureBundleWriter`` to dump per-service logs.
        """
        result = self._run(["logs", "--tail", str(tail), "--no-color", service])
        if result.returncode != 0:
            log.debug(
                "get_service_logs failed for %s (rc=%s): %s",
                service,
                result.returncode,
                (result.stderr or "").strip()[:200],
            )
            return ""
        return result.stdout or ""

    def wait_healthy(self, service: str, timeout_s: int = 120) -> None:
        t_start = time.monotonic()
        deadline = t_start + timeout_s
        while time.monotonic() < deadline:
            result = self._run(["ps", "--format", "{{.Status}}", service])
            status = result.stdout.strip().lower()
            if "healthy" in status or (
                "running" in status and "unhealthy" not in status
            ):
                log.info(
                    "Service %s healthy after %.1fs",
                    service,
                    time.monotonic() - t_start,
                )
                return
            time.sleep(2)
        raise TimeoutError(f"Service {service!r} not healthy after {timeout_s}s")

    def recreate_service(
        self,
        service: str,
        compose_env: dict[str, str] | None = None,
        timeout_s: int = 120,
    ) -> None:
        """Recreate a single compose service with optional compose_env override.

        Runs ``docker compose up -d {service}`` so the existing container
        is stopped and recreated using the current YAML config (after any
        ``compose_env`` substitution). Waits for the service to report
        healthy via the existing ``wait_healthy`` poll.

        Args:
            service: compose service name (e.g. ``datahub-gms-debug``).
            compose_env: env vars for compose YAML substitution at
                read time (e.g. ``{"DATAHUB_GMS_VERSION": new_tag}``).
                ``None`` (default) inherits the parent process env.
            timeout_s: seconds to wait for ``healthy`` before raising.

        Raises:
            RuntimeError: if ``docker compose up -d`` returns non-zero.
            TimeoutError: if the service does not become healthy within
                ``timeout_s`` (propagated from ``wait_healthy``).
        """
        cmd = self._base_cmd() + ["up", "-d", service]
        log.info("Recreating service %s with compose_env=%s", service, compose_env)
        t_start = time.monotonic()

        if compose_env is not None:
            merged_env = dict(os.environ)
            merged_env.update(compose_env)
            result = subprocess.run(
                cmd,
                env=merged_env,
                cwd=self._project_dir,
                capture_output=True,
                text=True,
            )
        else:
            result = subprocess.run(
                cmd,
                cwd=self._project_dir,
                capture_output=True,
                text=True,
            )

        if result.returncode != 0:
            raise RuntimeError(
                f"docker compose up -d {service} failed (rc={result.returncode}): "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )

        # Container stop+start is ~10-30s and silent — log when it's done so
        # the gap between "Recreating service" and the next health-check log
        # is no longer indeterminate.
        log.info(
            "docker compose up -d %s returned in %.1fs, waiting for healthy",
            service,
            time.monotonic() - t_start,
        )

        self.wait_healthy(service, timeout_s=timeout_s)

    # ── nuke + redeploy (G19c) ──────────────────────────────────────────────

    def get_project_name(self) -> str:
        """Return the Compose project name.

        Reads ``docker compose config --format json``'s ``name`` field, which
        is what Compose prefixes onto volume and network names (e.g. project
        ``datahub`` produces volume ``datahub_osdata``). The default is the
        basename of the project_dir's parent — i.e. the directory the repo is
        cloned into. Users with a non-``datahub`` clone directory get a
        different prefix; hardcoding ``datahub_*`` in callers silently no-ops
        on those machines, which is the P2 issue in the branch review.
        """
        result = self._run(["config", "--format", "json"])
        if result.returncode != 0:
            raise RuntimeError(
                f"docker compose config failed (rc={result.returncode}): "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
        try:
            cfg = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"docker compose config returned non-JSON output: {exc}"
            ) from exc
        name = cfg.get("name")
        if not isinstance(name, str) or not name:
            raise RuntimeError("docker compose config did not return a project name")
        return name

    def down_and_wipe(
        self,
        timeout_s: int = 120,
        wipe_volumes: list[str] | None = None,
    ) -> None:
        """Run `docker compose down --remove-orphans` and selectively wipe volumes.

        ``wipe_volumes`` accepts the SHORT compose volume names
        (e.g. ``["osdata", "mysqldata"]``). The actual Docker volume names get
        the project-name prefix (e.g. ``datahub_osdata`` on a default clone,
        ``my-fork_osdata`` on a fork). The prefix is read live from
        ``docker compose config`` so the wipe targets whatever the user's
        compose project is actually named.

        If ``wipe_volumes`` is None (default), wipes ALL volumes via the
        ``-v`` flag (same as the old behavior). When a list is provided, only
        those named volumes are removed — useful when you want to reset some
        state (e.g. ES + MySQL) but preserve others (Kafka, etc.).
        """
        # Stop containers first; volume removal happens AFTER so containers
        # release their mounts.
        if wipe_volumes is None:
            cmd = self._base_cmd() + ["down", "-v", "--remove-orphans"]
        else:
            cmd = self._base_cmd() + ["down", "--remove-orphans"]
        log.info("[nuke] running %s", " ".join(cmd))
        t_start = time.monotonic()
        result = subprocess.run(
            cmd,
            cwd=self._project_dir,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"docker compose down failed (rc={result.returncode}): "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
        log.info("[nuke] down completed in %.1fs", time.monotonic() - t_start)

        if wipe_volumes is not None:
            project_name = self.get_project_name()
            for short_name in wipe_volumes:
                # Always prepend the project prefix. The prior code used
                # ``"_" in short_name`` to allow already-qualified names
                # through, but volume short names can contain underscores
                # legitimately (e.g. ``my_data``) and the heuristic silently
                # mis-classified them — Docker would then "no such volume"
                # no-op without prefixing. The caller contract is short names.
                full_name = f"{project_name}_{short_name}"
                log.info("[nuke] removing volume %s", full_name)
                vresult = subprocess.run(
                    ["docker", "volume", "rm", full_name],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                # rc=1 with "no such volume" is fine (already absent); only
                # warn on other failures so a missing volume doesn't block.
                if vresult.returncode != 0:
                    stderr = vresult.stderr.strip()
                    if "no such volume" in stderr.lower():
                        log.info("[nuke] volume %s already absent", full_name)
                    else:
                        log.warning(
                            "[nuke] docker volume rm %s rc=%d: %s",
                            full_name,
                            vresult.returncode,
                            stderr,
                        )

    def up_stack(
        self,
        compose_env: dict[str, str] | None = None,
        services: list[str] | None = None,
        timeout_s: int = 120,
    ) -> None:
        """Run `docker compose up -d` (optionally with explicit services).

        Like recreate_service but for whole-stack cold-boot — does NOT call
        wait_healthy because individual service-readiness is handled by the
        caller (NukeAndRedeployPhase waits for GMS specifically).
        """
        cmd = self._base_cmd() + ["up", "-d"]
        if services:
            cmd += services
        log.info(
            "[nuke] running %s (compose_env keys=%s)",
            " ".join(cmd),
            sorted((compose_env or {}).keys()),
        )
        t_start = time.monotonic()
        if compose_env is not None:
            merged_env = dict(os.environ)
            merged_env.update(compose_env)
            result = subprocess.run(
                cmd,
                cwd=self._project_dir,
                capture_output=True,
                text=True,
                env=merged_env,
                timeout=timeout_s,
            )
        else:
            result = subprocess.run(
                cmd,
                cwd=self._project_dir,
                capture_output=True,
                text=True,
                timeout=timeout_s,
            )
        if result.returncode != 0:
            raise RuntimeError(
                f"docker compose up -d failed (rc={result.returncode}): "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
        log.info("[nuke] up -d completed in %.1fs", time.monotonic() - t_start)

    # ── upgrade job ────────────────────────────────────────────────────────

    def run_upgrade_job(
        self,
        env_overrides: dict[str, str],
        service: str = "system-update-debug",
        extra_args: list[str] | None = None,
        compose_env: dict[str, str] | None = None,
        container_name: str | None = None,
    ) -> "subprocess.Popen[str]":
        """Launch the upgrade job container via ``docker compose run --rm``.

        Args:
            env_overrides: container env, passed via ``-e KEY=VALUE`` flags.
                These become ``System.getenv()`` values inside the JVM.
            service: compose service name. Default ``system-update-debug``.
            extra_args: appended after the service name (e.g.
                ``["-u", "SystemUpdateBlocking"]``).
            compose_env: env vars for Compose's YAML variable substitution.
                Distinct from ``env_overrides`` — these drive
                ``${DATAHUB_<X>_VERSION:-default}`` interpolation at compose
                read time, not container env. Used by Plan F-3 to launch the
                upgrade container with a different image tag than what's
                currently running in the stack.
            container_name: optional ``--name`` for the container so a caller
                can later ``docker kill`` it by a deterministic name (used by
                the TC-324 kill-switch test).
        """
        env_args: list[str] = []
        for key, val in env_overrides.items():
            env_args += ["-e", f"{key}={val}"]
        name_args: list[str] = []
        if container_name is not None:
            name_args = ["--name", container_name]
        cmd = self._base_cmd() + ["run", "--rm"] + name_args + env_args + [service]
        if extra_args:
            cmd += extra_args
        log.debug("Running upgrade job: %s", " ".join(cmd))

        if compose_env is not None:
            # Inherit the parent process env, then layer compose_env on top.
            # Compose reads these at YAML-read time for ``${VAR:-default}``
            # substitution, so they MUST be in the subprocess's environ —
            # ``-e`` flags don't help (those go to the container).
            merged_env = dict(os.environ)
            merged_env.update(compose_env)
            return subprocess.Popen(
                cmd,
                cwd=self._project_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=merged_env,
            )
        return subprocess.Popen(
            cmd,
            cwd=self._project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def kill_container(self, container_name: str, signal: str = "KILL") -> bool:
        """SIGKILL (or other signal) a running container by name.

        Returns ``True`` if the container was killed, ``False`` if it wasn't
        running or didn't exist. Idempotent — calling on an already-stopped
        container is a no-op.

        Used by the TC-324 kill-switch test to forcibly interrupt the
        upgrade job mid-sweep.
        """
        result = subprocess.run(
            ["docker", "kill", f"--signal={signal}", container_name],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            log.info("[docker] killed container %s with SIG%s", container_name, signal)
            return True
        # Non-zero exit usually means "no such container" or "container not running".
        log.debug(
            "[docker] kill of %s returned rc=%d (likely already stopped): %s",
            container_name,
            result.returncode,
            result.stderr.strip(),
        )
        return False

    def remove_container(self, container_name: str) -> bool:
        """Force-remove a container by name.

        Used to clean up after a killed upgrade-job container before
        restarting (otherwise the next ``docker compose run --name <name>``
        fails with a name conflict).
        """
        result = subprocess.run(
            ["docker", "rm", "-f", container_name],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return True
        log.debug(
            "[docker] rm of %s returned rc=%d: %s",
            container_name,
            result.returncode,
            result.stderr.strip(),
        )
        return False

    # ── logs ──────────────────────────────────────────────────────────────

    def tail_service_logs(
        self,
        service: str,
        since: datetime,
        stop_event: "threading.Event | None" = None,
    ) -> Iterator[str]:
        """Stream ``docker compose logs --follow`` for ``service`` since ``since``.

        ``stop_event`` is the caller's deadline signal: when set, a watchdog
        thread calls ``proc.terminate()``, which closes stdout and lets the
        generator's ``for`` loop return through the ``finally`` cleanup.
        Without this, the blocking ``readline()`` inside ``for line in
        proc.stdout`` parks the daemon thread on a quiet stream past its
        deadline — the consumer's ``thread.join(timeout=...)`` returns but
        the `docker compose logs` subprocess stays alive across phases.
        """
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        cmd = self._base_cmd() + ["logs", "--follow", "--since", since_str, service]
        log.debug("Tailing logs: %s", " ".join(cmd))
        proc = subprocess.Popen(
            cmd,
            cwd=self._project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        assert proc.stdout is not None

        if stop_event is not None:

            def _watchdog() -> None:
                stop_event.wait()
                if proc.poll() is None:
                    proc.terminate()

            threading.Thread(
                target=_watchdog, daemon=True, name=f"tail-stop-{service}"
            ).start()

        try:
            for line in proc.stdout:
                yield line
        finally:
            if proc.poll() is None:
                proc.terminate()
            proc.wait()
