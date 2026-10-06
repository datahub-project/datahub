# These tests run against `icr.io/informix/informix-developer-database`, which is
# anonymously pullable from IBM's public container registry (no login required).
# The image is linux/amd64 only, so docker-compose.yml pins the platform; on Apple
# Silicon it runs under emulation.
#
# The test seeds a `testdb` database via `dbaccess` against setup/setup.sql, then
# runs the `informix` source and diffs the output against informix_mces_golden.json.
#
# Booting the image is the fragile part, and it fails in two distinct ways:
#   1. `docker compose up --wait` (pytest-docker's default) blocks on the image's
#      Docker HEALTHCHECK, which is start_period=60s/retries=1 and therefore flaps
#      to "unhealthy" on a loaded runner long before boot finishes. We bring the
#      container up with plain `up -d` and disable the healthcheck entirely.
#   2. The image's own informix_init.sh gives first-boot disk initialization a
#      hardcoded 60 iterations (~60s) to create the sysadmin database. Overrun it
#      and the entrypoint prints _OFFLINE_BANNER, cleans up, and exits -- so the
#      container is *gone*, not slow. setup/onconfig.mod shrinks the work to fit;
#      _informix_ready() below detects the failure immediately and surfaces the
#      server log rather than polling a dead container until the timeout.
#
# The Informix JDBC driver (com.ibm.informix:jdbc) and its org.mongodb:bson
# dependency are proprietary and are not vendored here. `accept_ibm_jdbc_license`
# makes the connector download and checksum-verify them from Maven Central on
# first run (see source/informix/driver.py), caching under ~/.datahub/jars/informix.
# That needs internet access; for air-gapped runs pre-provision the two jars and
# pass `driver_jar_paths` instead.
#
# Unlike the db2 integration test, this one needs no x86_64-only skipif for the
# Python side: jdk4py and JPype1 (the JVM bridge used to reach the JDBC driver)
# are both multi-arch. Only the Docker image is amd64-only.

import subprocess
import tempfile
from pathlib import Path

import pytest

from datahub.configuration.env_vars import is_ci
from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

pytestmark = [
    pytest.mark.integration_batch_4,
    # The image cannot bootstrap itself on DataHub's CI runners. Its entrypoint
    # drives every privileged step through `RUNAS() { sudo -u $1 ...; }`, and sudo
    # fails there at PAM's account-management stage ("Authentication service cannot
    # retrieve authentication info") -- even at uid=0, and with the rootfs not
    # mounted nosuid and sudo's setuid bit intact, so it is not a privilege or
    # mount-option problem. Every RUNAS step then no-ops, sqlhosts never gets a
    # valid INFORMIXSERVER entry, oninit reports "Bad DBSERVERNAME", and the server
    # never starts. It works locally (~27s end to end), so the test is kept and run
    # there rather than deleted; _informix_ready() prints the full evidence if
    # anyone re-enables this. Same reasoning as the hana suite's CI guard.
    #
    # Golden-file regression coverage for CI lives in the unit suite instead
    # (e.g. ViewProperties emission, FK pairing, catalog row tolerance).
    pytest.mark.skipif(
        is_ci(),
        reason="informix-developer-database cannot start on CI runners: its "
        "entrypoint's sudo calls fail PAM account management. Run locally.",
    ),
]

INFORMIX_PORT = 9088
INFORMIX_PASSWORD = "in4mix"
CONTAINER = "testinformix"

# Every way the image's startup scripts report that the server will not come up.
# The first three are the `return 255` paths in informix_init.sh; the last is
# informix_entry.sh giving up. Note that hitting one does NOT reliably stop the
# container -- the entrypoint can hang in its cleanup and sit there "running" with
# a dead server -- so matching the log is the only dependable signal.
_INIT_FAILURE_MARKERS = (
    "Informix is offline - Exit",
    "Informix stopped",
    "No sysadmin database detected",
    "No licenses available",
)
_ONLINE_LOG = "/opt/ibm/data/logs/online.log"


def _docker(*args: str) -> "subprocess.CompletedProcess[str]":
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def _sudo_probe(container: str) -> str:
    # The image's entrypoint drives every privileged step through setuid sudo, so a
    # runner that mounts the container filesystem nosuid breaks startup in a way
    # that looks nothing like a sudo problem ("Bad DBSERVERNAME"). Capture the
    # evidence here rather than making the next person infer it from the symptom.
    checks = (
        ("id", ["id"]),
        ("sudo -n true", ["sudo", "-n", "true"]),
        ("nosuid mounts", ["sh", "-c", "mount | grep nosuid || echo '(none)'"]),
        ("sudo perms", ["sh", "-c", "ls -l /usr/bin/sudo"]),
    )
    out = []
    for label, cmd in checks:
        result = _docker("exec", container, *cmd)
        detail = (result.stdout + result.stderr).strip() or "(no output)"
        out.append(f"{label}: rc={result.returncode} {detail}")
    return "\n".join(out)


def _online_log(container: str) -> str:
    # `docker cp` rather than `docker exec`, because by the time we want this the
    # container has usually already exited.
    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp) / "online.log"
        result = _docker("cp", f"{container}:{_ONLINE_LOG}", str(dest))
        if result.returncode != 0 or not dest.exists():
            return f"(unavailable: {result.stderr.strip()})"
        return dest.read_text(errors="replace")


def _informix_ready(container: str) -> bool:
    logs = _docker("logs", container)
    combined = logs.stdout + logs.stderr
    state = _docker("inspect", "-f", "{{.State.Status}}", container).stdout.strip()
    hit = next((m for m in _INIT_FAILURE_MARKERS if m in combined), None)
    if hit is not None or state in ("exited", "dead"):
        raise RuntimeError(
            f"{container} failed to initialize "
            f"(marker={hit!r}, state={state or 'unknown'}). The image gives "
            "first-boot disk init a hardcoded 60s and its logical logs must fit "
            "the root dbspace; see setup/onconfig.mod.\n"
            f"--- sudo/setuid probe ---\n{_sudo_probe(container)}\n"
            f"--- {_ONLINE_LOG} (tail) ---\n{_online_log(container)[-3000:]}\n"
            f"--- docker logs (tail) ---\n{combined[-3000:]}"
        )
    # `onstat -` prints "On-Line" once the server accepts connections. Probing the
    # server directly mirrors the db2/mysql "run a readiness command in the
    # container" pattern. The `bash -lc` wrapper is required: onstat only resolves
    # once the informix user's login profile has set INFORMIXDIR/PATH.
    probe = _docker("exec", "-u", "informix", container, "bash", "-lc", "onstat -")
    return "On-Line" in probe.stdout


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/informix"


@pytest.fixture(scope="module")
def informix_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml",
        "informix",
        setup_command=["up -d"],
    ) as docker_services:
        wait_for_port(
            docker_services,
            CONTAINER,
            INFORMIX_PORT,
            timeout=600,
            checker=lambda: _informix_ready(CONTAINER),
        )

        subprocess.run(
            [
                "docker",
                "exec",
                "-u",
                "root",
                CONTAINER,
                "bash",
                "-c",
                f"echo 'informix:{INFORMIX_PASSWORD}' | chpasswd",
            ],
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "cp",
                str(test_resources_dir / "setup" / "setup.sql"),
                f"{CONTAINER}:/tmp/setup.sql",
            ],
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "exec",
                "-u",
                "informix",
                CONTAINER,
                "bash",
                "-lc",
                "dbaccess - /tmp/setup.sql",
            ],
            check=True,
        )

        yield docker_services


@pytest.mark.integration
def test_informix_ingest(informix_runner, pytestconfig, test_resources_dir, tmp_path):
    output_path = tmp_path / "informix_mces.json"
    golden_path = test_resources_dir / "informix_mces_golden.json"

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "informix",
                "config": {
                    "host_port": f"localhost:{INFORMIX_PORT}",
                    "server": "informix",
                    "database": "testdb",
                    "username": "informix",
                    "password": INFORMIX_PASSWORD,
                    "accept_ibm_jdbc_license": True,
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
            r"root\[\d+\]\['aspect'\]\['json'\].+\[\d+\]\['auditStamp'\]\['time'\]",
            r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['created'\]\['time'\]",
        ],
    )
