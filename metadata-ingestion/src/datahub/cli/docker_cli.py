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

from datahub.cli.config_utils import DATAHUB_ROOT_FOLDER
from datahub.cli.docker_check import (
    DATAHUB_COMPOSE_LEGACY_VOLUME_FILTERS,
    DATAHUB_COMPOSE_PROJECT_FILTER,
    DOCKER_COMPOSE_PROJECT_NAME,
    DockerComposeVersionError,
    QuickstartStatus,
    check_docker_quickstart,
    get_docker_client,
    run_quickstart_preflight_checks,
)
from datahub.cli.quickstart_versioning import QuickstartVersionMappingConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)
_ClickPositiveInt = click.IntRange(min=1)

NEO4J_AND_ELASTIC_QUICKSTART_COMPOSE_FILE = (
    "docker/quickstart/docker-compose.quickstart.yml"
)
ELASTIC_QUICKSTART_COMPOSE_FILE = (
    "docker/quickstart/docker-compose-without-neo4j.quickstart.yml"
)
NEO4J_AND_ELASTIC_M1_QUICKSTART_COMPOSE_FILE = (
    "docker/quickstart/docker-compose-m1.quickstart.yml"
)
ELASTIC_M1_QUICKSTART_COMPOSE_FILE = (
    "docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml"
)
CONSUMERS_QUICKSTART_COMPOSE_FILE = (
    "docker/quickstart/docker-compose.consumers.quickstart.yml"
)
ELASTIC_CONSUMERS_QUICKSTART_COMPOSE_FILE = (
    "docker/quickstart/docker-compose.consumers-without-neo4j.quickstart.yml"
)
KAFKA_SETUP_QUICKSTART_COMPOSE_FILE = (
    "docker/quickstart/docker-compose.kafka-setup.quickstart.yml"
)


_QUICKSTART_MAX_WAIT_TIME = datetime.timedelta(minutes=10)
_QUICKSTART_UP_TIMEOUT = datetime.timedelta(seconds=100)
_QUICKSTART_STATUS_CHECK_INTERVAL = datetime.timedelta(seconds=2)


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


@click.group()
def docker() -> None:
    """Helper commands for setting up and interacting with a local
    DataHub instance using Docker."""
    pass


@docker.command()
@upgrade.check_upgrade
@telemetry.with_telemetry()
def check() -> None:
    """Check that the Docker containers are healthy"""
    status = check_docker_quickstart()
    if status.is_ok():
        click.secho("✔ No issues detected", fg="green")
    else:
        raise status.to_exception("The following issues were detected:")


def is_m1() -> bool:
    """Check whether we are running on an M1 machine"""
    try:
        return (
            platform.uname().machine == "arm64" and platform.uname().system == "Darwin"
        )
    except Exception:
        # Catch-all
        return False


def is_arch_m1(arch: Architectures) -> bool:
    return arch in [Architectures.arm64, Architectures.m1, Architectures.m2]


def should_use_neo4j_for_graph_service(graph_service_override: Optional[str]) -> bool:
    if graph_service_override is not None:
        if graph_service_override == "elasticsearch":
            click.echo("Starting with elasticsearch due to graph-service-impl param\n")
            return False
        if graph_service_override == "neo4j":
            click.echo("Starting with neo4j due to graph-service-impl param\n")
            return True
        else:
            click.secho(
                graph_service_override
                + " is not a valid graph service option. Choose either `neo4j` or "
                "`elasticsearch`\n",
                fg="red",
            )
            raise ValueError(f"invalid graph service option: {graph_service_override}")
    with get_docker_client() as client:
        if len(client.volumes.list(filters={"name": "datahub_neo4jdata"})) > 0:
            click.echo(
                "Datahub Neo4j volume found, starting with neo4j as graph service.\n"
                "If you want to run using elastic, run `datahub docker nuke` and re-ingest your data.\n"
            )
            return True

        logger.debug(
            "No Datahub Neo4j volume found, starting with elasticsearch as graph service.\n"
            "To use neo4j as a graph backend, run \n"
            "`datahub docker quickstart --graph-service-impl neo4j`"
            "\nfrom the root of the datahub repo\n"
        )
        return False


def _set_environment_variables(
    version: Optional[str],
    mysql_version: Optional[str],
    mysql_port: Optional[int],
    zk_port: Optional[int],
    kafka_broker_port: Optional[int],
    schema_registry_port: Optional[int],
    elastic_port: Optional[int],
    kafka_setup: Optional[bool],
) -> None:
    if version is not None:
        if not version.startswith("v") and "." in version:
            logger.warning(
                f"Version passed in '{version}' doesn't start with v, substituting with 'v{version}'"
            )
            version = f"v{version}"
        os.environ["DATAHUB_VERSION"] = version
    if mysql_version is not None:
        os.environ["DATAHUB_MYSQL_VERSION"] = mysql_version
    if mysql_port is not None:
        os.environ["DATAHUB_MAPPED_MYSQL_PORT"] = str(mysql_port)

    if zk_port is not None:
        os.environ["DATAHUB_MAPPED_ZK_PORT"] = str(zk_port)

    if kafka_broker_port is not None:
        os.environ["DATAHUB_MAPPED_KAFKA_BROKER_PORT"] = str(kafka_broker_port)

    if schema_registry_port is not None:
        os.environ["DATAHUB_MAPPED_SCHEMA_REGISTRY_PORT"] = str(schema_registry_port)

    if elastic_port is not None:
        os.environ["DATAHUB_MAPPED_ELASTIC_PORT"] = str(elastic_port)
    if kafka_setup:
        os.environ["DATAHUB_PRECREATE_TOPICS"] = "true"


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
        # Check for the docker compose v2 plugin.
        compose_version = subprocess.check_output(
            ["docker", "compose", "version", "--short"], stderr=subprocess.STDOUT
        ).decode()
        assert compose_version.startswith("2.") or compose_version.startswith("v2.")
        return ["docker", "compose"]
    except (OSError, subprocess.CalledProcessError, AssertionError):
        # We'll check for docker-compose as well.
        try:
            compose_version = subprocess.check_output(
                ["docker-compose", "version", "--short"], stderr=subprocess.STDOUT
            ).decode()
            if compose_version.startswith("2.") or compose_version.startswith("v2."):
                # This will happen if docker compose v2 is installed in standalone mode
                # instead of as a plugin.
                return ["docker-compose"]

            raise DockerComposeVersionError(
                f"You have docker-compose v1 ({compose_version}) installed, but we require Docker Compose v2. "
                "Please upgrade to Docker Compose v2. "
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

KAFKA_BOOTSTRAP_SERVER=broker:29092
KAFKA_SCHEMAREGISTRY_URL=http://schema-registry:${DATAHUB_MAPPED_SCHEMA_REGISTRY_PORT:-8081}

ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=${DATAHUB_MAPPED_ELASTIC_PORT:-9200}

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


def detect_quickstart_arch(arch: Optional[str]) -> Architectures:
    running_on_m1 = is_m1()
    if running_on_m1:
        click.secho("Detected M1 machine", fg="yellow")

    quickstart_arch = Architectures.x86 if not running_on_m1 else Architectures.arm64
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
    "--build-locally",
    type=bool,
    is_flag=True,
    default=False,
    help="Attempt to build the containers locally before starting",
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
    "--graph-service-impl",
    type=str,
    is_flag=False,
    default=None,
    help="If set, forces docker-compose to use that graph service implementation",
)
@click.option(
    "--mysql-port",
    type=_ClickPositiveInt,
    is_flag=False,
    default=None,
    help="If there is an existing mysql instance running on port 3306, set this to a free port to avoid port conflicts on startup",
)
@click.option(
    "--zk-port",
    type=_ClickPositiveInt,
    is_flag=False,
    default=None,
    help="If there is an existing zookeeper instance running on port 2181, set this to a free port to avoid port conflicts on startup",
)
@click.option(
    "--kafka-broker-port",
    type=_ClickPositiveInt,
    is_flag=False,
    default=None,
    help="If there is an existing Kafka broker running on port 9092, set this to a free port to avoid port conflicts on startup",
)
@click.option(
    "--schema-registry-port",
    type=_ClickPositiveInt,
    is_flag=False,
    default=None,
    help="If there is an existing process running on port 8081, set this to a free port to avoid port conflicts with Kafka schema registry on startup",
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
    "--standalone_consumers",
    required=False,
    is_flag=True,
    default=False,
    help="Launches MAE & MCE consumers as stand alone docker containers",
)
@click.option(
    "--kafka-setup",
    required=False,
    is_flag=True,
    default=False,
    help="Launches Kafka setup job as part of the compose deployment",
)
@click.option(
    "--arch",
    required=False,
    help="Specify the architecture for the quickstart images to use. Options are x86, arm64, m1 etc.",
)
@upgrade.check_upgrade
@telemetry.with_telemetry(
    capture_kwargs=[
        "version",
        "build_locally",
        "pull_images",
        "stop",
        "backup",
        "restore",
        "restore_indices",
        "standalone_consumers",
        "kafka_setup",
        "arch",
    ]
)
def quickstart(
    version: Optional[str],
    build_locally: bool,
    pull_images: bool,
    quickstart_compose_file: List[pathlib.Path],
    dump_logs_on_failure: bool,
    graph_service_impl: Optional[str],
    mysql_port: Optional[int],
    zk_port: Optional[int],
    kafka_broker_port: Optional[int],
    schema_registry_port: Optional[int],
    elastic_port: Optional[int],
    stop: bool,
    backup: bool,
    backup_file: str,
    restore: bool,
    restore_file: str,
    restore_indices: bool,
    no_restore_indices: bool,
    standalone_consumers: bool,
    kafka_setup: bool,
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

    quickstart_arch = detect_quickstart_arch(arch)
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
            graph_service_impl,
            kafka_setup,
            quickstart_arch,
            standalone_consumers,
            quickstart_execution_plan.composefile_git_ref,
        )

    # set version
    _set_environment_variables(
        version=quickstart_execution_plan.docker_tag,
        mysql_version=quickstart_execution_plan.mysql_tag,
        mysql_port=mysql_port,
        zk_port=zk_port,
        kafka_broker_port=kafka_broker_port,
        schema_registry_port=schema_registry_port,
        elastic_port=elastic_port,
        kafka_setup=kafka_setup,
    )

    compose = _docker_compose_v2()
    base_command: List[str] = [
        *compose,
        *itertools.chain.from_iterable(
            ("-f", f"{path}") for path in quickstart_compose_file
        ),
        "-p",
        DOCKER_COMPOSE_PROJECT_NAME,
    ]

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

    if build_locally:
        logger.info("Building docker images locally...")
        subprocess.run(
            base_command + ["build", "--pull", "-q"],
            check=True,
            env=_docker_subprocess_env(),
        )
        logger.info("Finished building docker images!")

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
    if os.environ.get("DOCKER_COMPOSE_BASE"):
        return os.environ["DOCKER_COMPOSE_BASE"]

    return f"https://raw.githubusercontent.com/datahub-project/datahub/{version_tag}"


def get_github_file_url(neo4j: bool, is_m1: bool, release_version_tag: str) -> str:
    base_url = get_docker_compose_base_url(release_version_tag)
    if neo4j:
        github_file = (
            f"{base_url}/{NEO4J_AND_ELASTIC_QUICKSTART_COMPOSE_FILE}"
            if not is_m1
            else f"{base_url}/{NEO4J_AND_ELASTIC_M1_QUICKSTART_COMPOSE_FILE}"
        )
    else:
        github_file = (
            f"{base_url}/{ELASTIC_QUICKSTART_COMPOSE_FILE}"
            if not is_m1
            else f"{base_url}/{ELASTIC_M1_QUICKSTART_COMPOSE_FILE}"
        )
    return github_file


def download_compose_files(
    quickstart_compose_file_name,
    quickstart_compose_file_list,
    graph_service_impl,
    kafka_setup,
    quickstart_arch,
    standalone_consumers,
    compose_git_ref,
):
    # download appropriate quickstart file
    should_use_neo4j = should_use_neo4j_for_graph_service(graph_service_impl)
    is_m1 = is_arch_m1(quickstart_arch)
    github_file = get_github_file_url(should_use_neo4j, is_m1, compose_git_ref)
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
        quickstart_download_response.raise_for_status()
        tmp_file.write(quickstart_download_response.content)
        logger.debug(f"Copied to {path}")
    if standalone_consumers:
        base_url = get_docker_compose_base_url(compose_git_ref)
        consumer_github_file = (
            f"{base_url}/{CONSUMERS_QUICKSTART_COMPOSE_FILE}"
            if should_use_neo4j
            else f"{base_url}/{ELASTIC_CONSUMERS_QUICKSTART_COMPOSE_FILE}"
        )

        default_consumer_compose_file = (
            Path(DATAHUB_ROOT_FOLDER) / "quickstart/docker-compose.consumers.yml"
        )
        with (
            open(default_consumer_compose_file, "wb")
            if default_consumer_compose_file
            else tempfile.NamedTemporaryFile(suffix=".yml", delete=False)
        ) as tmp_file:
            path = pathlib.Path(tmp_file.name)
            quickstart_compose_file_list.append(path)
            click.echo(
                f"Fetching consumer docker-compose file {consumer_github_file} from GitHub"
            )
            # Download the quickstart docker-compose file from GitHub.
            quickstart_download_response = request_session.get(consumer_github_file)
            quickstart_download_response.raise_for_status()
            tmp_file.write(quickstart_download_response.content)
            logger.debug(f"Copied to {path}")
    if kafka_setup:
        base_url = get_docker_compose_base_url(compose_git_ref)
        kafka_setup_github_file = f"{base_url}/{KAFKA_SETUP_QUICKSTART_COMPOSE_FILE}"

        default_kafka_compose_file = (
            Path(DATAHUB_ROOT_FOLDER) / "quickstart/docker-compose.kafka-setup.yml"
        )
        with (
            open(default_kafka_compose_file, "wb")
            if default_kafka_compose_file
            else tempfile.NamedTemporaryFile(suffix=".yml", delete=False)
        ) as tmp_file:
            path = pathlib.Path(tmp_file.name)
            quickstart_compose_file_list.append(path)
            click.echo(
                f"Fetching consumer docker-compose file {kafka_setup_github_file} from GitHub"
            )
            # Download the quickstart docker-compose file from GitHub.
            quickstart_download_response = request_session.get(kafka_setup_github_file)
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
@telemetry.with_telemetry()
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
