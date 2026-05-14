# ZDU E2E — Plan F-3: Two-Image-Tag Architecture

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Plumb `OLD_IMAGE_TAG` and `NEW_IMAGE_TAG` through the framework so the `system-update-debug` container in Phase 4 (`UpgradeBlockingPhase`) launches with the NEW image tag while the running GMS/MAE/MCE containers stay on the OLD tag. Foundation work for the future Phase 6 `RollingRestartPhase` (separate plan) which will swap running services from OLD → NEW.

**Architecture:** The Compose YAMLs at `docker/profiles/docker-compose.gms.yml` are _already_ parameterized — each ZDU-relevant service uses `${DATAHUB_<SERVICE>_VERSION:-${DATAHUB_VERSION:-debug}}` for its image tag. F-3 just needs to drive those env vars from the Python framework. Two new `ZDUTestConfig` fields (`old_image_tag`, `new_image_tag`) + a small extension to `DockerComposeClient.run_upgrade_job()` to set compose-substitution env vars (distinct from container `-e` flags) on the `subprocess.Popen` call. `UpgradeBlockingPhase` is updated to pass `compose_env={"DATAHUB_UPDATE_VERSION": new_tag}`.

**Tech Stack:** Python 3 (existing), Docker Compose v2 (existing). No new deps.

**Out of scope (deferred to follow-up plans):**

- **Phase 6 `RollingRestartPhase`** — uses the same plumbing to swap running GMS/MAE/MCE from OLD → NEW. Its own plan (Plan 4 or similar).
- **Image presence verification** — F-3 trusts that both tags exist locally or are pullable. Image-build orchestration is operator-side.
- **Plan F-5 (test mutator for race window)** — independent of F-3, can land before or after.

**Code-review handoff:** Final task dispatches `feature-dev:code-reviewer`.

---

## File Structure

```
smoke-test/tests/zdu/
├── framework/
│   ├── config.py                                MODIFY — add old_image_tag, new_image_tag
│   ├── docker_compose.py                        MODIFY — run_upgrade_job() gets compose_env kwarg
│   ├── phases/
│   │   ├── upgrade_blocking.py                  MODIFY — pass DATAHUB_UPDATE_VERSION=NEW_IMAGE_TAG
│   │   └── discovery.py                         MODIFY — record old/new tags in PhaseResult details
│   ├── runner.py                                MODIFY — pass image tags to UpgradeBlockingPhase
│   └── test_docker_compose.py                   CREATE — unit tests for compose_env wiring
└── README.md                                    MODIFY — document the OLD/NEW image-tag workflow
```

The `docker/profiles/docker-compose.gms.yml` file is **NOT** modified — it's already parameterized correctly for our needs.

---

## Task 1: Add `old_image_tag` and `new_image_tag` to `ZDUTestConfig`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/config.py`

**Pattern:** Two string fields with defaults that match the compose YAML's existing fallback (`debug`). Env vars `ZDU_OLD_IMAGE_TAG` and `ZDU_NEW_IMAGE_TAG` map into them via `from_env`.

- [ ] **Step 1: Add fields to `ZDUTestConfig`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/config.py`, near the top of the dataclass (alongside `gms_service`, `upgrade_service`), add:

```python
    # ── Two-image-tag testing (F-3) ──────────────────────────────────────────
    # Image tags consumed by the parameterized Compose YAMLs in docker/profiles/.
    # The OLD tag is what runs in the stack at seed time; the NEW tag is what
    # Phase 4's system-update-debug container uses, and what Phase 6 (future
    # RollingRestartPhase) will swap services to. Default ``debug`` matches the
    # compose YAML fallback so a stack started with the regular dev workflow
    # works without setting these env vars.
    old_image_tag: str = "debug"
    new_image_tag: str = "debug"
```

The `default = "debug"` matches the existing `${DATAHUB_VERSION:-debug}` fallback in `docker/profiles/docker-compose.gms.yml`. Out of the box (no env vars set), F-3 is a no-op against the existing single-image dev workflow.

- [ ] **Step 2: Add env-var support in `from_env`**

In the `from_env` classmethod, find the string-fields loop and append two new entries to it:

```python
            ("ZDU_OLD_IMAGE_TAG", "old_image_tag"),
            ("ZDU_NEW_IMAGE_TAG", "new_image_tag"),
```

(Place them near the existing `("DATAHUB_GMS_TOKEN", "gms_token")` line — anywhere in the string-fields list works.)

- [ ] **Step 3: Smoke-test default + override**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
c = ZDUTestConfig.from_env()
assert c.old_image_tag == 'debug', c.old_image_tag
assert c.new_image_tag == 'debug', c.new_image_tag
print('default OK')
"
```

```bash
ZDU_OLD_IMAGE_TAG=v1.5.0 ZDU_NEW_IMAGE_TAG=v1.6.0 smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
c = ZDUTestConfig.from_env()
assert c.old_image_tag == 'v1.5.0', c.old_image_tag
assert c.new_image_tag == 'v1.6.0', c.new_image_tag
print('override OK')
"
```

If `ModuleNotFoundError`, run from `cd smoke-test`.

Both must print `default OK` / `override OK`.

- [ ] **Step 4: Run framework tests for no regression**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 116 pass.

- [ ] **Step 5: Commit**

```bash
git add smoke-test/tests/zdu/framework/config.py
git commit -m "feat(zdu): add old_image_tag + new_image_tag config fields"
```

---

## Task 2: Extend `DockerComposeClient.run_upgrade_job` with `compose_env`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/docker_compose.py`
- Create: `smoke-test/tests/zdu/framework/test_docker_compose.py` (new unit tests)

**Pattern:** Add a new keyword-only parameter `compose_env: dict[str, str] | None = None`. When provided, merge it into `os.environ` for the `subprocess.Popen` call (this drives compose's variable substitution at YAML-read time). The existing `env_overrides` parameter stays unchanged — those become `-e KEY=VAL` flags that set env inside the container.

The two are different layers and should not be conflated:

- `env_overrides` → container env (e.g., `ASPECT_MIGRATION_MUTATOR_ENABLED=true` is a Spring property the JVM reads)
- `compose_env` → compose YAML substitution env (e.g., `DATAHUB_UPDATE_VERSION=v1.6.0` decides which image tag to launch)

### 2.1 — Write the failing test

- [ ] **Step 1: Create the test file**

Create `<REPO_ROOT>/smoke-test/tests/zdu/framework/test_docker_compose.py`:

```python
"""Unit tests for DockerComposeClient — uses mocked subprocess.Popen."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.docker_compose import DockerComposeClient


@pytest.fixture
def client() -> DockerComposeClient:
    return DockerComposeClient(
        project_dir="/tmp/x",
        compose_files=["docker-compose.yml"],
        profiles=["debug"],
    )


class TestRunUpgradeJob:
    def test_env_overrides_become_dash_e_flags(self, client: DockerComposeClient) -> None:
        with patch(
            "tests.zdu.framework.docker_compose.subprocess.Popen"
        ) as mock_popen:
            mock_popen.return_value = MagicMock()
            client.run_upgrade_job(
                env_overrides={"FOO": "bar", "BAZ": "qux"},
                service="system-update-debug",
            )
            args, kwargs = mock_popen.call_args
            cmd = args[0]
            # `-e FOO=bar` and `-e BAZ=qux` must be in the cmd, both before the service name.
            assert "-e" in cmd
            assert "FOO=bar" in cmd
            assert "BAZ=qux" in cmd
            # service name is the LAST positional before any extra_args.
            assert "system-update-debug" in cmd

    def test_extra_args_appended_after_service(self, client: DockerComposeClient) -> None:
        with patch(
            "tests.zdu.framework.docker_compose.subprocess.Popen"
        ) as mock_popen:
            mock_popen.return_value = MagicMock()
            client.run_upgrade_job(
                env_overrides={},
                service="system-update-debug",
                extra_args=["-u", "SystemUpdateBlocking"],
            )
            cmd = mock_popen.call_args[0][0]
            i = cmd.index("system-update-debug")
            assert cmd[i + 1 :] == ["-u", "SystemUpdateBlocking"]

    def test_compose_env_sets_subprocess_env(self, client: DockerComposeClient) -> None:
        # compose_env drives Compose's YAML variable substitution, NOT container env.
        with patch(
            "tests.zdu.framework.docker_compose.subprocess.Popen"
        ) as mock_popen:
            mock_popen.return_value = MagicMock()
            client.run_upgrade_job(
                env_overrides={"FOO": "bar"},
                service="system-update-debug",
                compose_env={"DATAHUB_UPDATE_VERSION": "v1.6.0"},
            )
            kwargs = mock_popen.call_args.kwargs
            # subprocess gets an env= dict that contains the compose substitution var
            assert "env" in kwargs
            assert kwargs["env"]["DATAHUB_UPDATE_VERSION"] == "v1.6.0"
            # And the parent env is preserved (other PATH-like vars survive)
            assert "PATH" in kwargs["env"]

    def test_compose_env_does_not_become_dash_e_flag(
        self, client: DockerComposeClient
    ) -> None:
        # compose_env is for Compose substitution, NOT for the container.
        # It must NOT appear as a -e flag in the cmd.
        with patch(
            "tests.zdu.framework.docker_compose.subprocess.Popen"
        ) as mock_popen:
            mock_popen.return_value = MagicMock()
            client.run_upgrade_job(
                env_overrides={},
                service="system-update-debug",
                compose_env={"DATAHUB_UPDATE_VERSION": "v1.6.0"},
            )
            cmd = mock_popen.call_args[0][0]
            assert "DATAHUB_UPDATE_VERSION=v1.6.0" not in cmd

    def test_compose_env_none_does_not_override_parent_env(
        self, client: DockerComposeClient
    ) -> None:
        # When compose_env is None (default), Popen should not receive an env=
        # kwarg at all — preserves prior behaviour where Popen inherits the
        # parent's full environment by default.
        with patch(
            "tests.zdu.framework.docker_compose.subprocess.Popen"
        ) as mock_popen:
            mock_popen.return_value = MagicMock()
            client.run_upgrade_job(
                env_overrides={},
                service="system-update-debug",
            )
            kwargs = mock_popen.call_args.kwargs
            assert "env" not in kwargs
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_docker_compose.py -v
```

Expected: 5 tests fail because `compose_env` parameter doesn't exist yet (and possibly the first 2 tests pass since they exercise existing behaviour).

### 2.2 — Implement

- [ ] **Step 3: Modify `run_upgrade_job` signature + body**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/docker_compose.py`, replace the existing `run_upgrade_job` method body (around line 98) with:

```python
    def run_upgrade_job(
        self,
        env_overrides: dict[str, str],
        service: str = "system-update-debug",
        extra_args: list[str] | None = None,
        compose_env: dict[str, str] | None = None,
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
        """
        env_args: list[str] = []
        for key, val in env_overrides.items():
            env_args += ["-e", f"{key}={val}"]
        cmd = self._base_cmd() + ["run", "--rm"] + env_args + [service]
        if extra_args:
            cmd += extra_args
        log.debug("Running upgrade job: %s", " ".join(cmd))

        popen_kwargs: dict[str, object] = {
            "cwd": self._project_dir,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "text": True,
        }
        if compose_env is not None:
            # Inherit the parent process env, then layer compose_env on top.
            # Compose reads these at YAML-read time for ``${VAR:-default}``
            # substitution, so they MUST be in the subprocess's environ —
            # ``-e`` flags don't help (those go to the container).
            merged_env = dict(os.environ)
            merged_env.update(compose_env)
            popen_kwargs["env"] = merged_env

        return subprocess.Popen(cmd, **popen_kwargs)
```

Add `import os` at the top of the file if not already imported.

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_docker_compose.py -v
```

Expected: 5 tests pass.

- [ ] **Step 5: Run all framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 121 pass (116 baseline + 5 new).

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/docker_compose.py \
        smoke-test/tests/zdu/framework/test_docker_compose.py
git commit -m "feat(zdu): DockerComposeClient.run_upgrade_job(compose_env=...) for image-tag override

The existing env_overrides become container -e flags; the new compose_env
is layered onto the subprocess.Popen environ to drive Compose's YAML
variable substitution. Lets Phase 4 launch system-update-debug with a
different image tag than what's running."
```

---

## Task 3: Wire `new_image_tag` into `UpgradeBlockingPhase`

**Files:**

- Modify: `smoke-test/tests/zdu/framework/phases/upgrade_blocking.py`
- Modify: `smoke-test/tests/zdu/framework/runner.py`
- Modify: `smoke-test/tests/zdu/framework/test_upgrade_blocking.py` (extend tests)

**Pattern:** `UpgradeBlockingPhase.__init__` gets a new `new_image_tag: str = "debug"` parameter. `_launch()` passes `compose_env={"DATAHUB_UPDATE_VERSION": self._new_image_tag, "DATAHUB_VERSION": self._new_image_tag}` so the `system-update-debug` compose service picks the NEW tag.

Both `DATAHUB_UPDATE_VERSION` (per-service, takes precedence per the compose YAML) and `DATAHUB_VERSION` (global fallback) are set so we cover both possible YAML schemas.

### 3.1 — Add the parameter and pass compose_env

- [ ] **Step 1: Modify `UpgradeBlockingPhase.__init__`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/upgrade_blocking.py`, find the `__init__` method and add `new_image_tag` as a kwarg:

```python
    def __init__(
        self,
        docker: DockerComposeClient,
        mysql: MySQLClient,
        gms_service: str,
        upgrade_service: str = "system-update-debug",
        timeout_s: int = _DEFAULT_TIMEOUT_S,
        new_image_tag: str = "debug",
    ) -> None:
        self._docker = docker
        self._mysql = mysql
        self._gms_service = gms_service
        self._upgrade_service = upgrade_service
        self._timeout_s = timeout_s
        self._new_image_tag = new_image_tag
```

- [ ] **Step 2: Modify `_launch()` to pass `compose_env`**

Replace the existing `_launch()` method body:

```python
    def _launch(self, token_env: dict[str, str]) -> "subprocess.Popen[str]":
        compose_env = {
            "DATAHUB_UPDATE_VERSION": self._new_image_tag,
            "DATAHUB_VERSION": self._new_image_tag,
        }
        return self._docker.run_upgrade_job(
            env_overrides=dict(token_env),
            service=self._upgrade_service,
            extra_args=["-u", "SystemUpdateBlocking"],
            compose_env=compose_env,
        )
```

- [ ] **Step 3: Update `runner.py` to pass `new_image_tag`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/runner.py`, find the `("upgrade_blocking", UpgradeBlockingPhase(...))` entry in the `phases` list and add the new kwarg:

```python
            (
                "upgrade_blocking",
                UpgradeBlockingPhase(
                    docker=self._docker,
                    mysql=self._mysql,
                    gms_service=self._config.gms_service,
                    upgrade_service=self._config.upgrade_service,
                    timeout_s=self._config.sweep_timeout_s,
                    new_image_tag=self._config.new_image_tag,
                ),
            ),
```

### 3.2 — Add a unit test

- [ ] **Step 4: Append a test to `test_upgrade_blocking.py`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/test_upgrade_blocking.py`, append (do NOT remove existing tests):

```python
class TestUpgradeBlockingPhaseImageTag:
    def test_passes_new_image_tag_via_compose_env(
        self, docker: MagicMock, mysql: MagicMock
    ) -> None:
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["INFO  No indices require incremental reindex"], returncode=0
        )
        phase = UpgradeBlockingPhase(
            docker=docker,
            mysql=mysql,
            gms_service="datahub-gms-debug",
            upgrade_service="system-update-debug",
            timeout_s=60,
            new_image_tag="v1.6.0",
        )
        ctx = TestContext()
        result = phase.run(ctx)
        assert result.status == "passed", result.error
        # Verify compose_env was passed through with the new tag
        call_kwargs = docker.run_upgrade_job.call_args.kwargs
        assert "compose_env" in call_kwargs
        assert call_kwargs["compose_env"]["DATAHUB_UPDATE_VERSION"] == "v1.6.0"
        assert call_kwargs["compose_env"]["DATAHUB_VERSION"] == "v1.6.0"

    def test_default_new_image_tag_is_debug(
        self, docker: MagicMock, mysql: MagicMock
    ) -> None:
        # Backward-compat: phase constructor without new_image_tag uses "debug"
        # which matches the compose YAML's existing fallback.
        docker.run_upgrade_job.return_value = _popen_with_lines(
            ["INFO  done"], returncode=0
        )
        phase = UpgradeBlockingPhase(
            docker=docker,
            mysql=mysql,
            gms_service="datahub-gms-debug",
            timeout_s=60,
        )
        ctx = TestContext()
        phase.run(ctx)
        compose_env = docker.run_upgrade_job.call_args.kwargs["compose_env"]
        assert compose_env["DATAHUB_UPDATE_VERSION"] == "debug"
```

- [ ] **Step 5: Run upgrade_blocking tests**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/test_upgrade_blocking.py -v 2>&1 | tail -20
```

Expected: 12 tests pass (10 existing + 2 new).

- [ ] **Step 6: Run framework tests for no regression**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 123 pass (121 baseline + 2 new).

- [ ] **Step 7: Smoke-test that the runner constructs**

```bash
smoke-test/venv/bin/python -c "
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.runner import ZDUTestRunner
r = ZDUTestRunner(ZDUTestConfig(new_image_tag='v1.6.0'), scenarios=[])
print('runner constructed OK')
"
```

If `ModuleNotFoundError`, `cd smoke-test`. Expected: `runner constructed OK`.

- [ ] **Step 8: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/upgrade_blocking.py \
        smoke-test/tests/zdu/framework/runner.py \
        smoke-test/tests/zdu/framework/test_upgrade_blocking.py
git commit -m "feat(zdu): UpgradeBlockingPhase launches system-update-debug with new_image_tag

Phase 4 now sets DATAHUB_UPDATE_VERSION + DATAHUB_VERSION in the compose
subprocess env so the upgrade job runs the NEW image while the live
GMS/MAE/MCE stay on OLD until Phase 6 (future RollingRestartPhase) swaps them."
```

---

## Task 4: Record OLD/NEW tags in DiscoveryPhase

**Files:**

- Modify: `smoke-test/tests/zdu/framework/phases/discovery.py`
- Modify: `smoke-test/tests/zdu/framework/runner.py`

**Pattern:** Pass both image tags to `DiscoveryPhase.__init__`. Include them in the `PhaseResult.details` so they show up in the JSON report.

- [ ] **Step 1: Modify `DiscoveryPhase.__init__`**

In `<REPO_ROOT>/smoke-test/tests/zdu/framework/phases/discovery.py`, locate the `__init__` (currently takes `docker`, `datahub`, `gms_service`). Add two kwargs:

```python
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
```

- [ ] **Step 2: Add the tags to `PhaseResult.details`**

In the `run()` method, find the `return PhaseResult(...)` for the success path. Update the `details` dict from `{"services": images}` to:

```python
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
```

- [ ] **Step 3: Update `runner.py`**

In the `phases = [...]` list, find the `("discovery", DiscoveryPhase(...))` entry and add the kwargs:

```python
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
```

- [ ] **Step 4: Smoke-test**

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
from unittest.mock import MagicMock
from tests.zdu.framework.context import TestContext
from tests.zdu.framework.phases.discovery import DiscoveryPhase

docker = MagicMock()
docker.get_all_service_images.return_value = {'datahub-gms-debug': 'acryldata/datahub-gms:debug'}
datahub = MagicMock()
phase = DiscoveryPhase(docker, datahub, 'datahub-gms-debug',
                      old_image_tag='v1.5.0', new_image_tag='v1.6.0')
ctx = TestContext()
result = phase.run(ctx)
assert result.status == 'passed'
assert result.details['old_image_tag'] == 'v1.5.0'
assert result.details['new_image_tag'] == 'v1.6.0'
print('OK')
"
```

Expected: `OK`.

- [ ] **Step 5: Run framework tests**

```bash
smoke-test/venv/bin/python -m pytest smoke-test/tests/zdu/framework/ 2>&1 | tail -3
```

Expected: 123 pass.

- [ ] **Step 6: Commit**

```bash
git add smoke-test/tests/zdu/framework/phases/discovery.py \
        smoke-test/tests/zdu/framework/runner.py
git commit -m "feat(zdu): DiscoveryPhase records old + new image tags in PhaseResult details"
```

---

## Task 5: Update README with the two-image-tag workflow

**Files:**

- Modify: `smoke-test/tests/zdu/README.md`

- [ ] **Step 1: Locate the existing image-related section**

```bash
cd <REPO_ROOT>
grep -nE "image|tag|version|DATAHUB_VERSION" smoke-test/tests/zdu/README.md | head -20
```

If there's no existing section about images, add a new one. If there is, modify it to describe the OLD/NEW tag mechanism.

- [ ] **Step 2: Add a "Two-Image-Tag Testing" section**

Append (or insert after the existing "Quick Start" section) a new section:

````markdown
## Two-Image-Tag Testing (Plan F-3)

The framework supports running the upgrade job with a different image tag
than what's currently in the stack. This lets you simulate a real ZDU
upgrade flow: live GMS/MAE/MCE stay on the OLD image while
`Phase 4 (UpgradeBlockingPhase)` runs the upgrade job with the NEW image.

### Prerequisites

Both image tags must already exist locally (or be pullable from your
registry):

```bash
# Build the OLD image (whatever release you're upgrading from)
./gradlew :metadata-service:war:dockerBuild -PdockerVersion=v1.5.0 -PdockerImage=acryldata/datahub-gms

# Build the NEW image (what you're upgrading to — usually HEAD)
./gradlew :metadata-service:war:dockerBuild -PdockerVersion=v1.6.0 -PdockerImage=acryldata/datahub-gms
```
````

(Repeat for `:datahub-upgrade:dockerBuild`, etc., for each ZDU-relevant
service.)

### Running

```bash
# Bring up the stack with the OLD image
DATAHUB_VERSION=v1.5.0 scripts/dev/datahub-dev.sh start

# Run ZDU tests with explicit OLD/NEW tags
ZDU_OLD_IMAGE_TAG=v1.5.0 \
ZDU_NEW_IMAGE_TAG=v1.6.0 \
DATAHUB_GMS_TOKEN="$(grep '  token:' ~/.datahubenv | awk '{print $2}')" \
smoke-test/venv/bin/python -m tests.zdu --suite a
```

Behaviour:

- Discovery phase records both tags in the JSON report's
  `phases[0].details.{old_image_tag,new_image_tag}`.
- Phase 4 (upgrade_blocking) launches `system-update-debug` with
  `DATAHUB_UPDATE_VERSION=$ZDU_NEW_IMAGE_TAG` so the upgrade job runs
  the NEW image's logic.
- Phase 6 (rolling_restart, future) will sequentially swap GMS → MAE → MCE
  from OLD to NEW.

### Defaults

If neither env var is set, both default to `debug` — matching the
existing single-image dev workflow. F-3 is a no-op against a stack
started without these env vars.

````

- [ ] **Step 2: Commit**

```bash
git add smoke-test/tests/zdu/README.md
git commit -m "docs(zdu): document two-image-tag workflow (Plan F-3)"
````

---

## Task 6: Live integration check

**Pre-requisite:** Compose stack up.

This validates that:

1. The default-tags case (no env vars) still produces the same Suite A behaviour as before F-3.
2. When `ZDU_NEW_IMAGE_TAG` is set, the `system-update-debug` container actually launches with that tag (verifiable via `docker compose run --dry-run` or by inspecting the running container in real time).

- [ ] **Step 1: Default-tag run (no env vars set)**

```bash
cd <REPO_ROOT>/smoke-test
TOKEN=$(grep '  token:' ~/.datahubenv | awk '{print $2}')
DATAHUB_GMS_URL=http://localhost:8080 \
DATAHUB_GMS_TOKEN="$TOKEN" \
ZDU_SKIP_PHASES=upgrade,sweep_and_io \
venv/bin/python -m tests.zdu --suite a 2>&1 | tail -20
```

Pipeline runs `discovery → seed → snapshot_t0 → upgrade_blocking → validation` (we skip the legacy `upgrade` rebuild and the `sweep_and_io` phase known to time out post-Plan-2). Expected: discovery + seed + snapshot_t0 pass; upgrade_blocking runs `system-update-debug -u SystemUpdateBlocking` with `DATAHUB_UPDATE_VERSION=debug` (default) — same image as the running stack.

Verify the JSON report includes both image tags:

```bash
cd <REPO_ROOT>
smoke-test/venv/bin/python -c "
import json, glob, os
candidates = glob.glob('smoke-test/build/zdu-test-report.json') + \
             glob.glob('smoke-test/smoke-test/build/zdu-test-report.json')
path = next(p for p in candidates if os.path.exists(p))
r = json.load(open(path))
disco = next(p for p in r['phases'] if p['name'] == 'discovery')
print('old_image_tag:', disco['details'].get('old_image_tag'))
print('new_image_tag:', disco['details'].get('new_image_tag'))
"
```

Expected: both print `debug`.

- [ ] **Step 2: Custom-tag dry-run (no need to actually have a NEW image built)**

We just want to verify the Python framework correctly passes the env var to the subprocess. The actual container launch can fail if no NEW image exists — that's fine for verifying the wiring.

```bash
cd <REPO_ROOT>/smoke-test
venv/bin/python << 'PY'
from unittest.mock import patch
from tests.zdu.framework.config import ZDUTestConfig
from tests.zdu.framework.docker_compose import DockerComposeClient
from tests.zdu.framework.phases.upgrade_blocking import UpgradeBlockingPhase
from tests.zdu.framework.mysql_client import MySQLClient
from tests.zdu.framework.context import TestContext

cfg = ZDUTestConfig(new_image_tag="v1.6.0-test")
docker = DockerComposeClient(
    cfg.project_dir, cfg.compose_files, cfg.compose_profiles,
)
mysql = MySQLClient()
phase = UpgradeBlockingPhase(
    docker=docker, mysql=mysql,
    gms_service=cfg.gms_service,
    upgrade_service=cfg.upgrade_service,
    timeout_s=10,
    new_image_tag=cfg.new_image_tag,
)
captured = {}
real_popen = __import__("subprocess").Popen

def spy_popen(*args, **kwargs):
    captured["cmd"] = args[0] if args else kwargs.get("args")
    captured["env"] = kwargs.get("env")
    # Return a fake popen that exits immediately
    import io
    class FakeProc:
        def __init__(self):
            self.stdout = io.StringIO("INFO  No indices require incremental reindex\n")
            self.returncode = 0
        def wait(self, timeout=None):
            return 0
        def kill(self):
            pass
    return FakeProc()

with patch("tests.zdu.framework.docker_compose.subprocess.Popen", side_effect=spy_popen):
    phase.run(TestContext())

env = captured.get("env") or {}
print("DATAHUB_UPDATE_VERSION:", env.get("DATAHUB_UPDATE_VERSION"))
print("DATAHUB_VERSION:", env.get("DATAHUB_VERSION"))
assert env.get("DATAHUB_UPDATE_VERSION") == "v1.6.0-test"
assert env.get("DATAHUB_VERSION") == "v1.6.0-test"
print("LIVE-WIRING OK")
PY
```

Expected: prints both env values + `LIVE-WIRING OK`.

- [ ] **Step 3: If anything regressed, fix and commit**

```bash
git add -p
git commit -m "fix(zdu): live-validation regression in F-3 wiring"
```

If nothing regressed, no commit needed.

---

## Task 7: Code review

**Files:** none modified — review only.

- [ ] **Step 1: Generate the diff**

```bash
cd <REPO_ROOT>
/usr/bin/git diff 1f9363458b..HEAD -- smoke-test/tests/zdu/ > /tmp/zdu-plan-f3.diff
wc -l /tmp/zdu-plan-f3.diff
```

(`1f9363458b` is the last F-1 commit. Adjust the SHA if newer commits intervene.)

- [ ] **Step 2: Dispatch `feature-dev:code-reviewer`**

Send the reviewer this prompt:

> Review the diff at `/tmp/zdu-plan-f3.diff`. This PR plumbs OLD/NEW image-tag awareness through the framework so Phase 4 (`UpgradeBlockingPhase`) launches the upgrade job with a different image tag than what's currently in the stack. Phase 6 (future `RollingRestartPhase`) will use the same machinery to swap running services from OLD to NEW.
>
> Concretely:
>
> - `ZDUTestConfig.old_image_tag` and `new_image_tag` config fields (defaults `"debug"` to match the existing compose YAML fallback).
> - `DockerComposeClient.run_upgrade_job(compose_env=...)` new kwarg — distinct from the existing `env_overrides` (which becomes container `-e` flags). `compose_env` is layered onto the `subprocess.Popen` env so Compose's YAML variable substitution `${DATAHUB_<X>_VERSION:-default}` picks up the override.
> - `UpgradeBlockingPhase.__init__(new_image_tag=...)` — the phase passes `compose_env={"DATAHUB_UPDATE_VERSION": new_tag, "DATAHUB_VERSION": new_tag}` so the `system-update-debug` compose service launches with the NEW tag.
> - `DiscoveryPhase` records both tags in `PhaseResult.details`.
> - README updated.
>
> Check specifically:
>
> 1. **Layering correctness:** `env_overrides` becomes `-e` flags (container env); `compose_env` becomes `subprocess.Popen(env=...)` (compose's view). They are NOT conflated.
> 2. **Backward compat:** Default `compose_env=None` means `Popen` doesn't receive an `env=` kwarg (preserves prior parent-env-inheritance behaviour). Default `new_image_tag="debug"` matches existing compose YAML fallback so the no-env-var case is a no-op.
> 3. **No accidental env var leakage:** The merged env in `run_upgrade_job` starts from `dict(os.environ)` then layers `compose_env` on top. That preserves PATH and other essential vars but overrides anything in `compose_env`.
> 4. **Both `DATAHUB_UPDATE_VERSION` AND `DATAHUB_VERSION`** are set so per-service and global fallbacks both pick up the NEW tag (defensive — the compose YAML fallback chain is `DATAHUB_UPDATE_VERSION:-DATAHUB_VERSION:-debug`).
> 5. **Type hints complete.**
> 6. **Test quality:** the 5 new tests in `test_docker_compose.py` exercise the existing `env_overrides → -e` path AND the new `compose_env → subprocess env` path AND the `compose_env=None` default. The 2 tests in `test_upgrade_blocking.py` verify the phase passes the right kwargs through.
> 7. **YAGNI:** No `restart`, `cleanup`, `swap_image_tag` methods on the docker client. Just the minimal `compose_env` parameter on the existing method.
> 8. **No coupling to future Phase 6 plan:** F-3 doesn't pre-design a `RollingRestartPhase`. The plumbing is reusable but not phase-specific.
>
> Surface only HIGH-CONFIDENCE issues. Skip nits.

- [ ] **Step 3: Address review feedback**

Each finding becomes a small commit. Re-review until approved.

- [ ] **Step 4: Final commit (if any)**

```bash
git commit -m "fix(zdu): code-review feedback on F-3"
```

---

## Self-Review

**Spec coverage** (against the Notion Phase 6 doc-body change + design doc Section 5.6):

| Requirement                                                                      | Task                                                                             |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Compose files parameterized via env vars                                         | Already done (audited Task 1 setup); F-3 just consumes existing parameterization |
| `OLD_IMAGE_TAG` / `NEW_IMAGE_TAG` configurable                                   | Task 1                                                                           |
| Update service image reference + `docker compose up -d {service}` (Phase 6 work) | OUT OF SCOPE — F-3 lays plumbing only; `RollingRestartPhase` is its own plan     |
| Phase 4 runs upgrade job with NEW image                                          | Tasks 2 + 3                                                                      |
| Discovery records both tags                                                      | Task 4                                                                           |
| Docs updated                                                                     | Task 5                                                                           |

**Placeholder scan:** None.

**Type / signature consistency:**

- `ZDUTestConfig.old_image_tag: str = "debug"`, `new_image_tag: str = "debug"` (Task 1) — used in Tasks 3, 4 for runner construction.
- `DockerComposeClient.run_upgrade_job(env_overrides, service, extra_args, compose_env)` (Task 2) — invoked from `UpgradeBlockingPhase._launch` (Task 3) and the existing `phases/sweep_and_io.py` (unchanged — `compose_env` defaults to None).
- `UpgradeBlockingPhase(docker, mysql, gms_service, upgrade_service, timeout_s, new_image_tag)` (Task 3) — runner wires it (Task 3 step 3).
- `DiscoveryPhase(docker, datahub, gms_service, old_image_tag, new_image_tag)` (Task 4) — runner wires it (Task 4 step 3).

**Risks called out:**

- F-3 doesn't verify the NEW image actually exists. If the user sets `ZDU_NEW_IMAGE_TAG=v1.6.0` without first building that image, `system-update-debug` will fail to start with an image-pull error. The error surfaces in the `upgrade_blocking` PhaseResult — clear failure mode but no preflight check. Worth a small follow-up to add `docker image inspect` validation in DiscoveryPhase.
- `DATAHUB_VERSION` is set as a fallback but it ALSO drives the GMS/MAE/MCE service tags via `${DATAHUB_GMS_VERSION:-${DATAHUB_VERSION:-debug}}`. Setting `DATAHUB_VERSION=$NEW_TAG` only affects the `compose_env` of the `docker compose run` subprocess — NOT the running stack — so the running GMS stays on its original tag. This is the desired behaviour. But if a future maintainer sets `DATAHUB_VERSION` globally before `scripts/dev/datahub-dev.sh start`, the WHOLE stack will use that tag at boot, which is a separate workflow F-3 doesn't change.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-08-zdu-plan-f3-two-image-tag.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch with checkpoints.

Which approach?
