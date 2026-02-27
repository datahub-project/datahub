import datetime
import functools
import itertools
import logging
import os
import pathlib
import platform
import signal
import subprocess
import sys
import tempfile
import time
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

import click
import click_spinner
import requests
from expandvars import expandvars
from requests_file import FileAdapter

from datahub._version import __version__, is_dev_mode, nice_version_name
from datahub.cli.config_utils import DATAHUB_ROOT_FOLDER
from datahub.cli.docker_check import (
    DATAHUB_COMPOSE_LEGACY_VOLUME_FILTERS,
    DATAHUB_COMPOSE_PROJECT_FILTER,
    DOCKER_COMPOSE_PROJECT_NAME,
    DockerComposeVersionError,
    QuickstartStatus,
    check_docker_quickstart,
    check_upgrade_supported,
    get_docker_client,
    run_quickstart_preflight_checks,
)
from datahub.cli.quickstart_versioning import (
    QuickstartVersionMappingConfig,
)
from datahub.configuration.env_vars import get_docker_compose_base
from datahub.ingestion.run.pipeline import Pipeline
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

_ClickPositiveInt = click.IntRange(min=1)

QUICKSTART_COMPOSE_FILE = "docker/quickstart/docker-compose.quickstart-profile.yml"

_QUICKSTART_MAX_WAIT_TIME = datetime.timedelta(minutes=10)
_QUICKSTART_UP_TIMEOUT = datetime.timedelta(seconds=100)
_QUICKSTART_STATUS_CHECK_INTERVAL = datetime.timedelta(seconds=2)

MIGRATION_REQUIRED_INSTRUCTIONS = f"""
Your existing DataHub server was installed with an \
older CLI and is incompatible with the current CLI (version {nice_version_name}).

Required steps to upgrade:
1. Backup your data (recommended): datahub docker quickstart --backup
   Guide: https://docs.datahub.com/docs/quickstart#back-up-datahub

2. Remove old installation: datahub docker nuke

3. Start fresh installation: datahub docker quickstart

4. Restore data:
    datahub docker quickstart --restore

⚠️  Without backup, all existing data will be lost.

For fresh start (if data is not needed):
1. Remove installation: 
    datahub docker nuke

2. Start fresh: 
    datahub docker quickstart
"""

REPAIR_REQUIRED_INSTRUCTIONS = f"""
Unhealthy DataHub Installation Detected

Your DataHub installation has issues that cannot be fixed with the current CLI.

Your options:

OPTION 1 - Preserve data (if needed):
1. Downgrade CLI to version 1.1: 
    pip install acryl-datahub==1.1
2. Fix the installation: 
    datahub docker quickstart
3. Create backup:
    datahub docker quickstart --backup
4. Upgrade CLI back:
    pip install acryl-datahub=={nice_version_name()}
5. Migrate: 
    datahub docker nuke && datahub docker quickstart
6. Restore data:
    datahub docker quickstart --restore

OPTION 2 - Fresh start (if data not needed):
1. Remove installation: 
    datahub docker nuke
2. Start fresh: 
    datahub docker quickstart

⚠️  The current CLI cannot repair installations created by older versions.

Additional information on backup and restore: https://docs.datahub.com/docs/quickstart#back-up-datahub
Troubleshooting guide: https://docs.datahub.com/docs/troubleshooting/quickstart
"""


class Architectures(Enum):
    x86 = "x86"
    arm64 = "arm64"
    m1 = "m1"
    m2 = "m2"


@functools.lru_cache
def _docker_subprocess_env() -> Dict[str, str]:
    # platform.machine() is equivalent to `uname -m`, as per https://stackoverflow.com/a/45124927/5004662
    DOCKER_COMPOSE_PLATFORM: str = "linux/" + platform.machine()

    env = {
        **os.environ,
        "DOCKER_BUILDKIT": "1",
    }
    if DOCKER_COMPOSE_PLATFORM:
        env["DOCKER_DEFAULT_PLATFORM"] = DOCKER_COMPOSE_PLATFORM
    return env


def show_migration_instructions():
    click.secho(MIGRATION_REQUIRED_INSTRUCTIONS, fg="red")


def show_repair_instructions():
    click.secho(REPAIR_REQUIRED_INSTRUCTIONS, fg="red")


@click.group()
def docker() -> None:
    """Helper commands for setting up and interacting with a local
    DataHub instance using Docker."""
    pass


@docker.command()
def check() -> None:
    """Check that the Docker containers are healthy"""
    status = check_docker_quickstart()

    if status.is_ok():
        click.secho("✔ No issues detected", fg="green")
        if status.running_unsupported_version:
            show_migration_instructions()
    else:
        if status.running_unsupported_version:
            show_repair_instructions()
        raise status.to_exception("The following issues were detected:")


def is_apple_silicon() -> bool:
    """Check whether we are running on an Apple Silicon machine"""
    try:
        return (
            platform.uname().machine == "arm64" and platform.uname().system == "Darwin"
        )
    except Exception:
        # Catch-all
        return False


def _set_environment_variables(
    version: Optional[str],
    mysql_port: Optional[int],
    kafka_broker_port: Optional[int],
    elastic_port: Optional[int],
) -> None:
    if version is not None:
        if not version.startswith("v") and "." in version:
            logger.warning(
                f"Version passed in '{version}' doesn't start with v, substituting with 'v{version}'"
            )
            version = f"v{version}"
        os.environ["DATAHUB_VERSION"] = version
    if mysql_port is not None:
        os.environ["DATAHUB_MAPPED_MYSQL_PORT"] = str(mysql_port)

    if kafka_broker_port is not None:
        os.environ["DATAHUB_MAPPED_KAFKA_BROKER_PORT"] = str(kafka_broker_port)

    if elastic_port is not None:
        os.environ["DATAHUB_MAPPED_ELASTIC_PORT"] = str(elastic_port)

    os.environ["METADATA_SERVICE_AUTH_ENABLED"] = "false"

    cliVersion = nice_version_name()
    if is_dev_mode():  # This should only happen during development/CI.
        cliVersion = __version__.replace(".dev0", "")
        logger.info(
            f"Development build: Using {cliVersion} instead of '{__version__}' version of CLI for UI ingestion"
        )

    os.environ["UI_INGESTION_DEFAULT_CLI_VERSION"] = cliVersion


def _get_default_quickstart_compose_file() -> Optional[str]:
    quickstart_folder = Path(DATAHUB_ROOT_FOLDER) / "quickstart"
    try:
        os.makedirs(quickstart_folder, exist_ok=True)
        return f"{quickstart_folder}/docker-compose.yml"
    except Exception as e:
        logger.debug(f"Failed to identify a default quickstart compose file due to {e}")
    return None


def _docker_compose_v2() -> List[str]:
    try:
        # Check for the docker compose v2 or newer plugin.
        compose_version = subprocess.check_output(
            ["docker", "compose", "version", "--short"], stderr=subprocess.STDOUT
        ).decode()
        assert not (
            compose_version.startswith("1.") or compose_version.startswith("v1.")
        )
        return ["docker", "compose"]
    except (OSError, subprocess.CalledProcessError, AssertionError):
        # We'll check for docker-compose as well.
        try:
            compose_version = subprocess.check_output(
                ["docker-compose", "version", "--short"], stderr=subprocess.STDOUT
            ).decode()
            if not (
                compose_version.startswith("1.") or compose_version.startswith("v1.")
            ):
                # This will happen if docker compose v2 is installed in standalone mode
                # instead of as a plugin.
                return ["docker-compose"]

            raise DockerComposeVersionError(
                f"You have docker-compose v1 ({compose_version}) installed, but we require Docker Compose v2 or later. "
                "Please upgrade to Docker Compose v2 or later. "
                "See https://docs.docker.com/compose/compose-v2/ for more information."
            )
        except (OSError, subprocess.CalledProcessError):
            # docker-compose v1 is not installed either.
            raise DockerComposeVersionError(
                "You don't have Docker Compose installed. Please install Docker Compose. See https://docs.docker.com/compose/install/.",
            ) from None


def _attempt_stop(quickstart_compose_file: List[pathlib.Path]) -> None:
    default_quickstart_compose_file = _get_default_quickstart_compose_file()
    compose_files_for_stopping = (
        quickstart_compose_file
        if quickstart_compose_file
        else (
            [pathlib.Path(default_quickstart_compose_file)]
            if default_quickstart_compose_file
            else None
        )
    )
    if compose_files_for_stopping:
        # docker-compose stop
        compose = _docker_compose_v2()
        base_command: List[str] = [
            *compose,
            "--profile",
            "quickstart",
            *itertools.chain.from_iterable(
                ("-f", f"{path}") for path in compose_files_for_stopping
            ),
            "-p",
            DOCKER_COMPOSE_PROJECT_NAME,
        ]
        try:
            logger.debug(f"Executing {base_command} stop")
            subprocess.run(
                [*base_command, "stop"],
                check=True,
                env=_docker_subprocess_env(),
            )
            click.secho("Stopped datahub successfully.", fg="green")
        except subprocess.CalledProcessError:
            click.secho(
                "Error while stopping.",
                fg="red",
            )
        return


def _backup(backup_file: str) -> int:
    resolved_backup_file = os.path.expanduser(backup_file)
    dirname = os.path.dirname(resolved_backup_file)
    logger.info(f"Creating directory {dirname} if it doesn't exist")
    os.makedirs(dirname, exist_ok=True)
    logger.info("Executing backup command")
    result = subprocess.run(
        [
            "bash",
            "-c",
            f"docker exec {DOCKER_COMPOSE_PROJECT_NAME}-mysql-1 mysqldump -u root -pdatahub datahub > {resolved_backup_file}",
        ]
    )
    logger.info(
        f"Backup written to {resolved_backup_file} with status {result.returncode}"
    )
    return result.returncode


def _restore(
    restore_primary: bool,
    restore_indices: Optional[bool],
    primary_restore_file: Optional[str],
) -> int:
    assert restore_primary or restore_indices, (
        "Either restore_primary or restore_indices must be set"
    )
    msg = "datahub> "
    if restore_primary:
        msg += f"Will restore primary database from {primary_restore_file}. "
    if restore_indices is not False:
        msg += (
            f"Will {'also ' if restore_primary else ''}re-build indexes from scratch. "
        )
    else:
        msg += "Will not re-build indexes. "
    msg += "Press y to continue."
    click.confirm(msg, abort=True)
    if restore_primary:
        assert primary_restore_file
        resolved_restore_file = os.path.expanduser(primary_restore_file)
        logger.info(f"Restoring primary db from backup at {resolved_restore_file}")
        assert os.path.exists(resolved_restore_file), (
            f"File {resolved_restore_file} does not exist"
        )
        with open(resolved_restore_file) as fp:
            result = subprocess.run(
                [
                    "bash",
                    "-c",
                    f"docker exec -i {DOCKER_COMPOSE_PROJECT_NAME}-mysql-1 bash -c 'mysql -uroot -pdatahub datahub '",
                ],
                stdin=fp,
                capture_output=True,
            )
        if result.returncode != 0:
            logger.error("Failed to run MySQL restore")
            return result.returncode
        else:
            logger.info("Successfully restored primary backup.")

    # We run restore indices by default on primary restores, and also if the --restore-indices flag is explicitly set
    if restore_indices is not False:
        with tempfile.NamedTemporaryFile() as env_fp:
            env_fp.write(
                expandvars(
                    """
            # Required Environment Variables
EBEAN_DATASOURCE_USERNAME=datahub
EBEAN_DATASOURCE_PASSWORD=datahub
EBEAN_DATASOURCE_HOST=mysql:${DATAHUB_MAPPED_MYSQL_PORT:-3306}
EBEAN_DATASOURCE_URL=jdbc:mysql://mysql:${DATAHUB_MAPPED_MYSQL_PORT:-3306}/datahub?verifyServerCertificate=false&useSSL=true&useUnicode=yes&characterEncoding=UTF-8
EBEAN_DATASOURCE_DRIVER=com.mysql.jdbc.Driver
ENTITY_REGISTRY_CONFIG_PATH=/datahub/datahub-gms/resources/entity-registry.yml
GRAPH_SERVICE_IMPL=elasticsearch
KAFKA_BOOTSTRAP_SERVER=broker:29092
KAFKA_SCHEMAREGISTRY_URL=http://datahub-gms:8080${DATAHUB_GMS_BASE_PATH}/schema-registry/api/
SCHEMA_REGISTRY_TYPE=INTERNAL

ELASTICSEARCH_HOST=search
ELASTICSEARCH_PORT=${DATAHUB_MAPPED_ELASTIC_PORT:-9200}
ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX=true
ELASTICSEARCH_PROTOCOL=http

#NEO4J_HOST=http://<your-neo-host>:7474
#NEO4J_URI=bolt://<your-neo-host>
#NEO4J_USERNAME=neo4j
#NEO4J_PASSWORD=datahub

DATAHUB_GMS_HOST=datahub-gms
DATAHUB_GMS_PORT=${DATAHUB_MAPPED_GMS_PORT:-8080}

DATAHUB_MAE_CONSUMER_HOST=datahub-gms
DATAHUB_MAE_CONSUMER_PORT=9091

# Optional Arguments

# Uncomment and set these to support SSL connection to Elasticsearch
# ELASTICSEARCH_USE_SSL=
# ELASTICSEARCH_SSL_PROTOCOL=
# ELASTICSEARCH_SSL_SECURE_RANDOM_IMPL=
# ELASTICSEARCH_SSL_TRUSTSTORE_FILE=
# ELASTICSEARCH_SSL_TRUSTSTORE_TYPE=
# ELASTICSEARCH_SSL_TRUSTSTORE_PASSWORD=
# ELASTICSEARCH_SSL_KEYSTORE_FILE=
# ELASTICSEARCH_SSL_KEYSTORE_TYPE=
# ELASTICSEARCH_SSL_KEYSTORE_PASSWORD=
            """
                ).encode("utf-8")
            )
            env_fp.flush()
            if logger.isEnabledFor(logging.DEBUG):
                with open(env_fp.name) as env_fp_reader:
                    logger.debug(f"Env file contents: {env_fp_reader.read()}")

            # continue to issue the restore indices command
            # TODO Use --version if passed
            command = (
                "docker pull acryldata/datahub-upgrade:${DATAHUB_VERSION:-head}"
                + f" && docker run --network datahub_network --env-file {env_fp.name} "
                + "acryldata/datahub-upgrade:${DATAHUB_VERSION:-head} -u RestoreIndices -a clean"
            )
            logger.info(f"Running index restore command: {command}")
            result = subprocess.run(
                args=[
                    "bash",
                    "-c",
                    "docker pull acryldata/datahub-upgrade:"
                    + "${DATAHUB_VERSION:-head}"
                    + f" && docker run --network {DOCKER_COMPOSE_PROJECT_NAME}_network --env-file {env_fp.name} "
                    + "acryldata/datahub-upgrade:${DATAHUB_VERSION:-head}"
                    + " -u RestoreIndices -a clean",
                ],
                capture_output=True,
            )
            logger.info(
                f"Index restore command finished with status {result.returncode}"
            )
        if result.returncode != 0:
            logger.info(result.stderr)
        logger.debug(result.stdout)
    return result.returncode


# TODO: Do we really need this? If someone wants to use  a different arg, they can still pass the standard docker env var DOCKER_DEFAULT_PLATFORM
# We dont really need to select a different image unlike earlier (mysql vs mariadb) since we do publish both archs for all images (or are available for external images).
def detect_quickstart_arch(arch: Optional[str]) -> Architectures:
    running_on_apple_silicon = is_apple_silicon()
    if running_on_apple_silicon:
        click.secho("Detected Apple Silicon", fg="yellow")

    quickstart_arch = (
        Architectures.x86 if not running_on_apple_silicon else Architectures.arm64
    )
    if arch:
        matched_arch = [a for a in Architectures if arch.lower() == a.value]
        if not matched_arch:
            click.secho(
                f"Failed to match arch {arch} with list of architectures supported {[a.value for a in Architectures]}"
            )
        quickstart_arch = matched_arch[0]
        click.secho(f"Using architecture {quickstart_arch}", fg="yellow")

    return quickstart_arch


@docker.command()
@click.option(
    "--version",
    type=str,
    default="default",
    help="Datahub version to be deployed. If not set, deploy using the defaults from the quickstart compose. Use 'stable' to start the latest stable version.",
)
@click.option(
    "--pull-images/--no-pull-images",
    type=bool,
    is_flag=True,
    default=True,
    help="Attempt to pull the containers from Docker Hub before starting",
)
@click.option(
    "-f",
    "--quickstart-compose-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    default=[],
    multiple=True,
    help="Use a local docker-compose file instead of pulling from GitHub",
)
@click.option(
    "--dump-logs-on-failure",
    type=bool,
    is_flag=True,
    default=False,
    help="If true, the docker-compose logs will be printed to console if something fails",
)
@click.option(
    "--mysql-port",
    type=_ClickPositiveInt,
    is_flag=False,
    default=None,
    help="If there is an existing mysql instance running on port 3306, set this to a free port to avoid port conflicts on startup",
)
@click.option(
    "--kafka-broker-port",
    type=_ClickPositiveInt,
    is_flag=False,
    default=None,
    help="If there is an existing Kafka broker running on port 9092, set this to a free port to avoid port conflicts on startup",
)
@click.option(
    "--elastic-port",
    type=_ClickPositiveInt,
    is_flag=False,
    default=None,
    help="If there is an existing Elasticsearch instance running on port 9092, set this to a free port to avoid port conflicts on startup",
)
@click.option(
    "--stop",
    type=bool,
    is_flag=True,
    default=False,
    help="Use this flag to stop the running containers",
)
@click.option(
    "--backup",
    required=False,
    is_flag=True,
    default=False,
    help="Run this to backup a running quickstart instance",
)
@click.option(
    "--backup-file",
    required=False,
    type=click.Path(exists=False, dir_okay=False, readable=True, writable=True),
    default=os.path.expanduser("~/.datahub/quickstart/backup.sql"),
    show_default=True,
    help="Run this to backup data from a running quickstart instance",
)
@click.option(
    "--restore",
    required=False,
    is_flag=True,
    default=False,
    help="Run this to restore a running quickstart instance from a previous backup (see --backup)",
)
@click.option(
    "--restore-file",
    required=False,
    type=str,
    default=os.path.expanduser("~/.datahub/quickstart/backup.sql"),
    help="Set this to provide a custom restore file",
)
@click.option(
    "--restore-indices",
    required=False,
    is_flag=True,
    default=False,
    help="Enable the restoration of indices of a running quickstart instance. Note: Using --restore will automatically restore-indices unless you use the --no-restore-indices flag.",
)
@click.option(
    "--no-restore-indices",
    required=False,
    is_flag=True,
    default=False,
    help="Disables the restoration of indices of a running quickstart instance when used in conjunction with --restore.",
)
@click.option(
    "--arch",
    required=False,
    help="Specify the architecture for the quickstart images to use. Options are x86, arm64, m1 etc.",
)
@telemetry.with_telemetry(
    capture_kwargs=[
        "version",
        "pull_images",
        "stop",
        "backup",
        "restore",
        "restore_indices",
        "arch",
    ]
)
def quickstart(
    version: Optional[str],
    pull_images: bool,
    quickstart_compose_file: List[pathlib.Path],
    dump_logs_on_failure: bool,
    mysql_port: Optional[int],
    kafka_broker_port: Optional[int],
    elastic_port: Optional[int],
    stop: bool,
    backup: bool,
    backup_file: str,
    restore: bool,
    restore_file: str,
    restore_indices: bool,
    no_restore_indices: bool,
    arch: Optional[str],
) -> None:
    """Start an instance of DataHub locally using docker-compose.

    This command will automatically download the latest docker-compose configuration
    from GitHub, pull the latest images, and bring up the DataHub system.
    There are options to override the docker-compose config file, build the containers
    locally, and dump logs to the console or to a file if something goes wrong.
    """
    if backup:
        _backup(backup_file)
        return
    if restore or restore_indices or no_restore_indices:
        if not valid_restore_options(restore, restore_indices, no_restore_indices):
            return
        # execute restore
        restore_indices_flag: Optional[bool] = None
        if restore_indices:
            restore_indices_flag = True
        if no_restore_indices:
            restore_indices_flag = False
        _restore(
            restore_primary=restore,
            primary_restore_file=restore_file,
            restore_indices=restore_indices_flag,
        )
        return

    quickstart_versioning = QuickstartVersionMappingConfig.fetch_quickstart_config()

    quickstart_execution_plan = quickstart_versioning.get_quickstart_execution_plan(
        version
    )
    logger.info(f"Using quickstart plan: {quickstart_execution_plan}")

    # Run pre-flight checks.
    with get_docker_client() as client:
        run_quickstart_preflight_checks(client)

    quickstart_compose_file = list(
        quickstart_compose_file
    )  # convert to list from tuple

    auth_resources_folder = Path(DATAHUB_ROOT_FOLDER) / "plugins/auth/resources"
    os.makedirs(auth_resources_folder, exist_ok=True)

    quickstart_compose_file_name = _get_default_quickstart_compose_file()
    if stop:
        _attempt_stop(quickstart_compose_file)
        return
    elif not quickstart_compose_file:
        logger.info(f"compose file name {quickstart_compose_file_name}")
        download_compose_files(
            quickstart_compose_file_name,
            quickstart_compose_file,
            quickstart_execution_plan.composefile_git_ref,
        )

    # check if running datahub can be upgraded to the latest version.
    if not _check_upgrade_and_show_instructions(quickstart_compose_file):
        sys.exit(1)

    # set version
    _set_environment_variables(
        version=quickstart_execution_plan.docker_tag,
        mysql_port=mysql_port,
        kafka_broker_port=kafka_broker_port,
        elastic_port=elastic_port,
    )

    compose = _docker_compose_v2()
    base_command: List[str] = [
        *compose,
        "--profile",
        "quickstart",
        *itertools.chain.from_iterable(
            ("-f", f"{path}") for path in quickstart_compose_file
        ),
        "-p",
        DOCKER_COMPOSE_PROJECT_NAME,
    ]

    click.echo(f"base_command: {base_command}")

    # Pull and possibly build the latest containers.
    try:
        if pull_images:
            click.echo("\nPulling docker images... ")
            click.secho(
                "This may take a while depending on your network bandwidth.", dim=True
            )

            # docker compose v2 seems to spam the stderr when used in a non-interactive environment.
            # As such, we'll only use the quiet flag if we're in an interactive environment.
            # If we're in quiet mode, then we'll show a spinner instead.
            quiet = not sys.stderr.isatty()
            with PerfTimer() as timer, click_spinner.spinner(disable=not quiet):
                subprocess.run(
                    [*base_command, "pull", *(("-q",) if quiet else ())],
                    check=True,
                    env=_docker_subprocess_env(),
                )

            telemetry.telemetry_instance.ping(
                "quickstart-image-pull",
                {
                    "status": "success",
                    "duration": timer.elapsed_seconds(),
                },
            )
            click.secho("Finished pulling docker images!")
    except subprocess.CalledProcessError:
        telemetry.telemetry_instance.ping(
            "quickstart-image-pull",
            {
                "status": "failure",
            },
        )
        click.secho(
            "Error while pulling images. Going to attempt to move on to docker compose up assuming the images have "
            "been built locally",
            fg="red",
        )

    # Start it up! (with retries)
    click.echo("\nStarting up DataHub...")
    start_time = datetime.datetime.now()
    status: Optional[QuickstartStatus] = None
    up_attempts = 0
    while (datetime.datetime.now() - start_time) < _QUICKSTART_MAX_WAIT_TIME:
        # We must run docker-compose up at least once.
        # Beyond that, we should run it again if something goes wrong.
        if up_attempts == 0 or (status and status.needs_up()):
            if up_attempts > 0:
                click.echo()
            up_attempts += 1

            logger.debug(f"Executing docker compose up command, attempt #{up_attempts}")
            up_process = subprocess.Popen(
                base_command + ["up", "-d", "--remove-orphans"],
                env=_docker_subprocess_env(),
            )
            try:
                up_process.wait(timeout=_QUICKSTART_UP_TIMEOUT.total_seconds())
            except subprocess.TimeoutExpired:
                logger.debug("docker compose up timed out, sending SIGTERM")
                up_process.terminate()
                try:
                    up_process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    logger.debug("docker compose up still running, sending SIGKILL")
                    up_process.kill()
                    up_process.wait()
            else:
                # If the docker process got a keyboard interrupt, raise one here.
                if up_process.returncode in {128 + signal.SIGINT, -signal.SIGINT}:
                    raise KeyboardInterrupt

        # Check docker health every few seconds.
        status = check_docker_quickstart()
        if status.is_ok():
            break

        # Wait until next iteration.
        click.echo(".", nl=False)
        time.sleep(_QUICKSTART_STATUS_CHECK_INTERVAL.total_seconds())
    else:
        assert status

        # Falls through if the while loop doesn't exit via break.
        click.echo()
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as log_file:
            ret = subprocess.run(
                base_command + ["logs"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
                env=_docker_subprocess_env(),
            )
            log_file.write(ret.stdout)

        if dump_logs_on_failure:
            click.echo("Dumping docker compose logs:")
            click.echo(pathlib.Path(log_file.name).read_text())
            click.echo()

        raise status.to_exception(
            header="Unable to run quickstart - the following issues were detected:",
            footer="If you think something went wrong, please file an issue at https://github.com/datahub-project/datahub/issues\n"
            "or send a message in our Slack https://datahub.com/slack/\n"
            f"Be sure to attach the logs from {log_file.name}",
        )

    # Handle success condition.
    click.echo()
    click.secho("✔ DataHub is now running", fg="green")
    click.secho(
        "Ingest some demo data using `datahub docker ingest-sample-data`,\n"
        "or head to http://localhost:9002 (username: datahub, password: datahub) to play around with the frontend.",
        fg="green",
    )
    click.secho(
        "Need support? Get in touch on Slack: https://datahub.com/slack/",
        fg="magenta",
    )


def get_docker_compose_base_url(version_tag: str) -> str:
    docker_compose_base = get_docker_compose_base()
    if docker_compose_base:
        return docker_compose_base

    return f"https://raw.githubusercontent.com/datahub-project/datahub/{version_tag}"


def get_github_file_url(release_version_tag: str) -> str:
    base_url = get_docker_compose_base_url(release_version_tag)
    github_file = f"{base_url}/{QUICKSTART_COMPOSE_FILE}"
    return github_file


def download_compose_files(
    quickstart_compose_file_name, quickstart_compose_file_list, compose_git_ref
):
    # download appropriate quickstart file
    github_file = get_github_file_url(compose_git_ref)
    # also allow local files
    request_session = requests.Session()
    request_session.mount("file://", FileAdapter())
    with (
        open(quickstart_compose_file_name, "wb")
        if quickstart_compose_file_name
        else tempfile.NamedTemporaryFile(suffix=".yml", delete=False)
    ) as tmp_file:
        path = pathlib.Path(tmp_file.name)
        quickstart_compose_file_list.append(path)
        logger.info(f"Fetching docker-compose file {github_file} from GitHub")
        # Download the quickstart docker-compose file from GitHub.
        quickstart_download_response = request_session.get(github_file)
        if quickstart_download_response.status_code == 404:
            raise click.ClickException(
                f"Could not find quickstart compose file for version {compose_git_ref}. "
                "Please try a different version or check the version exists at https://github.com/datahub-project/datahub/releases"
            )
        quickstart_download_response.raise_for_status()
        tmp_file.write(quickstart_download_response.content)
        logger.debug(f"Copied to {path}")


def valid_restore_options(
    restore: bool, restore_indices: bool, no_restore_indices: bool
) -> bool:
    if no_restore_indices and not restore:
        click.secho(
            "Using --no-restore-indices without a --restore isn't defined", fg="red"
        )
        return False
    if no_restore_indices and restore_indices:
        click.secho(
            "Using --restore-indices in conjunction with --no-restore-indices is undefined",
            fg="red",
        )
        return False
    if restore and restore_indices:
        click.secho(
            "Using --restore automatically implies using --restore-indices, you don't need to set both. Continuing...",
            fg="yellow",
        )
        return True
    return True


@docker.command()
@click.option(
    "--token",
    type=str,
    is_flag=False,
    default=None,
    help="The token to be used when ingesting, used when datahub is deployed with METADATA_SERVICE_AUTH_ENABLED=true",
)
@upgrade.check_upgrade
def ingest_sample_data(token: Optional[str]) -> None:
    """Ingest sample data into a running DataHub instance."""

    # Verify that docker is up.
    status = check_docker_quickstart()
    if not status.is_ok():
        raise status.to_exception(
            header="Docker is not ready:",
            footer="Try running `datahub docker quickstart` first.",
        )

    # Run ingestion.
    click.echo("Starting ingestion...")
    recipe: dict = {
        "source": {
            "type": "demo-data",
            "config": {},
        },
        "sink": {
            "type": "datahub-rest",
            "config": {"server": "http://localhost:8080"},
        },
    }

    if token is not None:
        recipe["sink"]["config"]["token"] = token

    pipeline = Pipeline.create(recipe)
    pipeline.run()
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)


@docker.command()
@telemetry.with_telemetry()
@click.option(
    "--keep-data",
    type=bool,
    is_flag=True,
    default=False,
    help="Delete data volumes",
)
def nuke(keep_data: bool) -> None:
    """Remove all Docker containers, networks, and volumes associated with DataHub."""

    with get_docker_client() as client:
        click.echo(f"Removing containers in the {DOCKER_COMPOSE_PROJECT_NAME} project")
        for container in client.containers.list(
            all=True, filters=DATAHUB_COMPOSE_PROJECT_FILTER
        ):
            container.remove(v=True, force=True)

        if keep_data:
            click.echo(
                f"Skipping deleting data volumes in the {DOCKER_COMPOSE_PROJECT_NAME} project"
            )
        else:
            click.echo(f"Removing volumes in the {DOCKER_COMPOSE_PROJECT_NAME} project")
            for filter in DATAHUB_COMPOSE_LEGACY_VOLUME_FILTERS + [
                DATAHUB_COMPOSE_PROJECT_FILTER
            ]:
                for volume in client.volumes.list(filters=filter):
                    volume.remove(force=True)

        click.echo(f"Removing networks in the {DOCKER_COMPOSE_PROJECT_NAME} project")
        for network in client.networks.list(filters=DATAHUB_COMPOSE_PROJECT_FILTER):
            network.remove()


def _check_upgrade_and_show_instructions(
    quickstart_compose_file: List[pathlib.Path],
) -> bool:
    """Check if running datahub can be upgraded to the latest version and show appropriate instructions.

    Args:
        quickstart_compose_file: List of compose file paths

    Returns:
        bool: True if upgrade is supported, False otherwise
    """
    quickstart_status = check_docker_quickstart()

    if not check_upgrade_supported(quickstart_compose_file, quickstart_status):
        if quickstart_status.is_ok():
            show_migration_instructions()
        else:
            show_repair_instructions()
        return False
    return True
