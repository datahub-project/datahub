#!/usr/bin/env python3
"""Run production GMS locally without Docker (Python 3.10+, POSIX, JDK 25).

Each instance (gms-0, gms-1, ...) owns an H2 file, queue spool, index prefix and
HTTP port. One long-lived embedded OpenSearch node is shared across stacks.
SystemUpdate and RestoreIndices finish before GMS starts. See ../README.md for
usage and the stack.json discovery contract. The launcher blocks until stopped.
"""

from __future__ import annotations

import argparse
import base64
import errno
import fcntl
import json
import os
import re
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO, TypedDict, cast

from _boot_timings import BootTimings, dir_size_mb
from _env_layers import (
    JOB_DIRS,
    MODES,
    SCOPES,
    EnvFile,
    EnvLayerError,
    EnvValue,
    resolve,
)
from _opensearch_manager import (
    OPENSEARCH_READY_TIMEOUT_ENV,
    SharedOpenSearchManager,
    default_shared_dir,
    delete_prefixes,
    distribution_script,
    ensure_data_major,
    env_seconds,
    index_names,
    index_prefix,
    selected_major,
    validate_major,
    wait_for_ready,
)
from _system_update_checker import SystemUpdateChecker

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "scripts/dev"))
from extensions.datahub_local_extension import (  # noqa: E402
    Description,
    EnvAlias,
    LocalExtension,
    context,
    load,
    ports_for,
)

WAR = REPO_ROOT / "metadata-service/war/build/libs/war.war"
UPGRADE_JAR = REPO_ROOT / "datahub-upgrade/build/libs/datahub-upgrade.jar"
DEFAULT_DATA_DIR = REPO_ROOT / "metadata-service/gms-lite/build/stack"
OPENSEARCH_READY_TIMEOUT_SECONDS = 900
JAVA = (
    str(Path(os.environ["JAVA_HOME"]) / "bin/java")
    if os.environ.get("JAVA_HOME")
    else "java"
)
JVM_OPENS = [
    "--add-modules=java.se",
    "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.management/sun.management=ALL-UNNAMED",
    "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED",
]


class Credentials(TypedDict):
    system_client_secret: str
    token_signing_key: str
    token_salt: str


class StackInfo(TypedDict):
    bootId: str
    opensearchVersion: str
    opensearchPort: int
    opensearchShared: bool
    default: str
    instances: dict[str, str]
    indexPrefixes: dict[str, str]
    auth: bool
    frontendPort: int | None
    systemClientId: str | None
    systemClientSecret: str | None
    adminTokens: dict[str, str]
    adminTokenExpiresAt: dict[str, str]
    services: dict[str, dict[str, str]]
    serviceHealth: dict[str, dict[str, dict[str, object]]]
    servicePids: dict[str, dict[str, int]]


def log(message: str) -> None:
    print(f"[lite-stack {time.strftime('%H:%M:%S')}] {message}", flush=True)


def write_private(path: Path, content: str) -> None:
    # Publish atomically, with permissions set before credentials reach disk.
    temporary = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write(content)
    temporary.replace(path)


@contextmanager
def data_directory(
    data_dir: Path,
    fresh: bool,
    major: str | None = None,
    *,
    processes: int = 1,
    private_opensearch: bool = False,
) -> Iterator[list[str]]:
    """Keep the lock outside the directory so --fresh cannot unlink a live lock."""
    data_dir.parent.mkdir(parents=True, exist_ok=True)
    with (data_dir.parent / f".{data_dir.name}.gms-lite.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SystemExit(f"data directory is already in use: {data_dir}") from error
        major = major or selected_major()
        version_file = data_dir / "opensearch-major"
        previous = version_file.read_text().strip() if version_file.exists() else "3"
        if (data_dir / ".gms-lite").exists() and previous != major and not fresh:
            raise SystemExit(
                f"changing OpenSearch {previous} to {major} requires --fresh"
            )
        previous_instances = [
            path.name
            for path in data_dir.glob("gms-*")
            if path.is_dir() and path.name.removeprefix("gms-").isdigit()
        ]
        marker = data_dir / ".gms-lite"
        if fresh and data_dir.exists():
            if not marker.exists() and any(data_dir.iterdir()):
                raise SystemExit(
                    f"refusing to clear an unrecognized data directory: {data_dir}"
                )
            if marker.exists() and previous != major and not private_opensearch:
                # Clean the old node before erasing the major needed to find it on a retry.
                manager = SharedOpenSearchManager(
                    default_shared_dir(previous).expanduser(), major=previous
                )
                names = set(previous_instances) | {f"gms-{i}" for i in range(processes)}
                delete_prefixes(
                    manager.get_or_start(),
                    sorted(index_prefix(data_dir, name) for name in names),
                )
            shutil.rmtree(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        marker.touch()
        version_file.write_text(major + "\n")
        stack_file = data_dir / "stack.json"
        stack_file.unlink(missing_ok=True)
        try:
            yield previous_instances
        finally:
            stack_file.unlink(missing_ok=True)


def load_credentials(data_dir: Path) -> Credentials:
    path = data_dir / ".secrets.json"
    if path.exists():
        value: object = json.loads(path.read_text())
        keys = ("system_client_secret", "token_signing_key", "token_salt")
        if not isinstance(value, dict) or not all(
            isinstance(value.get(k), str) and value[k] for k in keys
        ):
            raise SystemExit(f"invalid credentials file: {path}")
        return cast(Credentials, value)
    credentials: Credentials = {
        "system_client_secret": secrets.token_urlsafe(24),
        "token_signing_key": base64.b64encode(secrets.token_bytes(32)).decode(),
        "token_salt": base64.b64encode(secrets.token_bytes(16)).decode(),
    }
    write_private(path, json.dumps(credentials))
    return credentials


def ensure_ports_available(ports: list[int]) -> None:
    for port in ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError as error:
                if error.errno != errno.EADDRINUSE:
                    raise
                raise SystemExit(
                    f"port {port} is in use; stop its owner before starting this stack"
                ) from error


def read_env_file(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    values: dict[str, str] = {}
    for number, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = line.removeprefix("export ")
        key, separator, value = line.partition("=")
        if not separator or not key.strip().isidentifier():
            raise SystemExit(f"invalid KEY=VALUE at {path}:{number}")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def http_ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError):
        return False


class Processes:
    """Own a separate process group per service, including any grandchildren."""

    def __init__(self, data_dir: Path | None = None) -> None:
        self.data_dir = data_dir
        self.children: list[subprocess.Popen[str]] = []
        self.logs: list[TextIO] = []
        self.optional: set[int] = set()
        self.identities: dict[str, str] = {}

    def start(
        self,
        command: list[str],
        env: dict[str, str],
        path: Path,
        *,
        pipe: bool = False,
        cwd: Path = REPO_ROOT,
        required: bool = True,
    ) -> subprocess.Popen[str]:
        output = path.open("w")
        self.logs.append(output)
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE if pipe else output,
            stderr=output if pipe else subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        self.children.append(process)
        identity = subprocess.run(
            ["ps", "-ww", "-p", str(process.pid), "-o", "lstart=,command="],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        if identity:
            self.identities[str(process.pid)] = identity
            if self.data_dir is not None:
                write_private(
                    self.data_dir / "processes.json", json.dumps(self.identities)
                )
        if not required:
            self.optional.add(process.pid)
        return process

    def check(self) -> None:
        for child in list(self.children):
            if child.poll() is not None:
                if child.pid in self.optional:
                    log(
                        f"optional service PID {child.pid} exited with {child.returncode}; see logs"
                    )
                    # Retire failed groups now, before the leader PID can be reused on a long run.
                    self.stop_group(child)
                    self.children.remove(child)
                    self.optional.discard(child.pid)
                    continue
                raise SystemExit(
                    f"service PID {child.pid} exited with {child.returncode}; see logs"
                )

    @staticmethod
    def stop_group(child: subprocess.Popen[str]) -> None:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=5)
        # A wrapper may have exited while a grandchild ignored SIGTERM.
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def stop(self) -> None:
        # Stop GMS before OpenSearch so the queue spool and final index writes can flush.
        for child in reversed(self.children):
            self.stop_group(child)
        for output in self.logs:
            output.close()
        for child in self.children:
            if child.stdout is not None:
                child.stdout.close()


def start_opensearch(
    args: argparse.Namespace, processes: Processes, log_dir: Path
) -> int:
    timeout = env_seconds(
        OPENSEARCH_READY_TIMEOUT_ENV, OPENSEARCH_READY_TIMEOUT_SECONDS
    )
    extension = load(getattr(args, "extension", None))
    description = (
        extension.describe(json.loads(getattr(args, "extension_options", "{}")))
        if extension
        else None
    )
    process_env = dict(os.environ)
    if description and "opensearch-knn" in description.search_plugins:
        process_env["OPENSEARCH_KNN_ENABLED"] = "true"
    if not args.private_opensearch:
        manager = SharedOpenSearchManager(
            args.shared_opensearch_dir, major=args.opensearch, env=process_env
        )
        bound = manager.get_or_start()
        log(f"Shared OpenSearch ready on port {bound}; survives stack shutdown")
        log(
            "To stop it: python3 "
            + shlex.join(
                [
                    str(Path(__file__).with_name("_opensearch_manager.py")),
                    "stop",
                    "--opensearch",
                    args.opensearch,
                    "--dir",
                    str(manager.shared_dir),
                ]
            )
        )
        return bound
    private_dir = args.data_dir / f"opensearch-{args.opensearch}"
    private_dir.mkdir(parents=True, exist_ok=True)
    ensure_data_major(private_dir, args.opensearch)
    process = processes.start(
        [
            str(distribution_script(REPO_ROOT, args.opensearch)),
            "--port",
            str(args.opensearch_port or 0),
            "--data",
            str(private_dir),
        ],
        process_env,
        log_dir / "opensearch.log",
        pipe=True,
    )
    bound = wait_for_ready(process, processes.logs[-1], timeout)
    if bound is None:
        raise SystemExit(
            f"embedded OpenSearch did not report READY; see {log_dir / 'opensearch.log'}"
        )
    log(f"Private OpenSearch ready on port {bound}")
    return bound


def spring_env_name(path: str) -> str:
    """Map a YAML property path to Spring Boot's environment variable spelling."""
    indexed = re.sub(r"\[(\d+)\]", r"_\1_", path)
    return indexed.replace(".", "_").replace("-", "").upper().replace("__", "_")


# Paths mirror application.yaml; camel-case segments become one word in env names.
SEARCH_CONNECTION_PROPERTIES: dict[str, tuple[tuple[str, ...], str]] = {
    "elasticsearch.pathPrefix": (("ELASTICSEARCH_PATH_PREFIX",), ""),
    "elasticsearch.useSSL": (("ELASTICSEARCH_USE_SSL",), "false"),
    "postgres.pgCron.iam.awsRegion": (("AWS_REGION",), ""),
    **{
        f"elasticsearch.clusters.{cluster}.{field}": (
            (
                placeholder
                if cluster == "primary"
                else "ELASTICSEARCH_CLUSTERS_SECONDARY_" + suffix,
            ),
            value,
        )
        for cluster in ("primary", "secondary")
        for field, placeholder, suffix, value in (
            ("uri", "ELASTICSEARCH_URI", "URI", ""),
            ("username", "ELASTICSEARCH_USERNAME", "USERNAME", ""),
            ("password", "ELASTICSEARCH_PASSWORD", "PASSWORD", ""),
            ("region", "AWS_REGION", "REGION", ""),
            (
                "opensearchUseAwsIamAuth",
                "OPENSEARCH_USE_AWS_IAM_AUTH",
                "OPENSEARCH_USE_AWS_IAM_AUTH",
                "false",
            ),
        )
    },
    **{
        f"elasticsearch.clusters.{cluster}.sslContext.{field}": (
            (
                (
                    "ELASTICSEARCH_"
                    if cluster == "primary"
                    else "ELASTICSEARCH_CLUSTERS_SECONDARY_"
                )
                + "SSL_"
                + suffix,
            ),
            "",
        )
        for cluster in ("primary", "secondary")
        for field, suffix in (
            ("protocol", "PROTOCOL"),
            ("secureRandomImplementation", "SECURE_RANDOM_IMPL"),
            ("trustStoreFile", "TRUSTSTORE_FILE"),
            ("trustStoreType", "TRUSTSTORE_TYPE"),
            ("trustStorePassword", "TRUSTSTORE_PASSWORD"),
            ("keyStoreFile", "KEYSTORE_FILE"),
            ("keyStoreType", "KEYSTORE_TYPE"),
            ("keyStorePassword", "KEYSTORE_PASSWORD"),
            ("keyPassword", "KEY_PASSWORD"),
        )
    },
    **{
        f"elasticsearch.clusters.{cluster}.proxy.{field}": (
            (f"ELASTICSEARCH_CLUSTERS_{cluster.upper()}_PROXY_{field.upper()}",)
            + (
                (f"ELASTICSEARCH_PROXY_{field.upper()}",)
                if cluster == "primary"
                else ()
            ),
            value,
        )
        for cluster in ("primary", "secondary")
        for field, value in (
            ("host", ""),
            ("port", ""),
            ("scheme", "http"),
            ("username", ""),
            ("password", ""),
        )
    },
    **{
        f"elasticsearch.clusters.{cluster}.{field}": (
            (f"ELASTICSEARCH_CLUSTERS_{cluster.upper()}_{suffix}",),
            value,
        )
        for cluster in ("primary", "secondary")
        for field, suffix, value in (
            (
                "proxy.useSystemProxyProperties",
                "PROXY_USE_SYSTEM_PROXY_PROPERTIES",
                "false",
            ),
            ("index.prefix", "INDEX_PREFIX", ""),
        )
    },
    **{
        f"elasticsearch.componentCluster.{field}": (
            ("ELASTICSEARCH_COMPONENT_CLUSTER_" + suffix,),
            "primary",
        )
        for field, suffix in (
            ("searchV2", "SEARCH_V2"),
            ("searchV3", "SEARCH_V3"),
            ("semantic", "SEMANTIC"),
            ("graph", "GRAPH"),
            ("timeseries", "TIMESERIES"),
            ("systemMetadata", "SYSTEM_METADATA"),
            ("usage", "USAGE"),
        )
    },
    **{
        f"elasticsearch.clusterMigration.{family}.{field}": (
            (f"ELASTICSEARCH_CLUSTER_MIGRATION_{suffix}_{key}",),
            "",
        )
        for family, suffix in (
            ("graph", "GRAPH"),
            ("timeseries", "TIMESERIES"),
            ("systemMetadata", "SYSTEM_METADATA"),
            ("usage", "USAGE"),
        )
        for field, key in (
            ("targetCluster", "TARGET_CLUSTER"),
            ("copy.remote.host", "COPY_REMOTE_HOST"),
            ("copy.remote.username", "COPY_REMOTE_USERNAME"),
            ("copy.remote.password", "COPY_REMOTE_PASSWORD"),
        )
    },
}
# Placeholders that other, non-connection settings also read (AWS_REGION feeds the Bedrock embedding
# region and S3 clients). Blanking them would break those settings, so the connection properties
# that use them are pinned through their Spring property names only, which win over placeholders.
SHARED_PLACEHOLDERS = frozenset({"AWS_REGION"})
SEARCH_CONNECTION_ENV: dict[str, str] = {
    name: value
    for path, (placeholders, value) in SEARCH_CONNECTION_PROPERTIES.items()
    for name in (
        *(name for name in placeholders if name not in SHARED_PLACEHOLDERS),
        spring_env_name(path),
    )
}


SECRET_KEY_MARKERS = ("SECRET", "TOKEN", "PASSWORD", "SALT", "SIGNING_KEY", "API_KEY")
PLACEHOLDER_SECRETS: Credentials = {
    "system_client_secret": "<per-stack>",
    "token_signing_key": "<per-stack>",
    "token_salt": "<per-stack>",
}


def env_mode(requested: str | None) -> str:
    return (
        requested if requested is not None else "ci" if "CI" in os.environ else "local"
    )


def stack_context(
    instance: str,
    gms_port: int,
    os_port: int,
    data_dir: Path,
    credentials: Credentials,
    auth: bool,
    major: str,
    frontend_port: int | None,
) -> dict[str, str]:
    """The ${GMS_LITE_*} placeholders for one isolated instance."""
    directory = data_dir / instance
    return {
        "GMS_LITE_REPO_ROOT": str(REPO_ROOT),
        "GMS_LITE_INSTANCE": instance,
        "GMS_LITE_INSTANCE_DIR": str(directory),
        "GMS_LITE_INSTANCE_DIR_URI": directory.resolve().as_uri(),
        "GMS_LITE_INDEX_PREFIX": index_prefix(data_dir, instance),
        "GMS_LITE_GMS_PORT": str(gms_port),
        "GMS_LITE_OPENSEARCH_PORT": str(os_port),
        "GMS_LITE_OPENSEARCH_VERSION": major,
        "GMS_LITE_SCHEMA_FIELD_HASH_ID_ENABLED": str(major == "3").lower(),
        "GMS_LITE_FRONTEND_PORT": str(frontend_port or 9002),
        "GMS_LITE_AUTH": str(auth).lower(),
        "GMS_LITE_SYSTEM_CLIENT_SECRET": credentials["system_client_secret"],
        "GMS_LITE_TOKEN_SIGNING_KEY": credentials["token_signing_key"],
        "GMS_LITE_TOKEN_SALT": credentials["token_salt"],
    }


# These file values cannot be replaced by inherited or ad-hoc settings. Search aliases
# are neutralized separately because Spring accepts both YAML placeholders and relaxed names.
PROTECTED_KEYS = frozenset(
    {
        "EBEAN_DATASOURCE_URL",
        "EBEAN_DATASOURCE_DRIVER",
        "EBEAN_DATASOURCE_USERNAME",
        "EBEAN_DATASOURCE_PASSWORD",
        "EBEAN_AUTOCREATE",
        "DATAHUB_SQL_SETUP_ENABLED",
        "DATAHUB_PGQUEUE_INMEMORY_SPOOL_FILE",
        "DATAHUB_OBJECT_STORAGE_URI",
        "ELASTICSEARCH_HOST",
        "ELASTICSEARCH_PORT",
        "INDEX_PREFIX",
        "ELASTICSEARCH_SHIM_ENGINE_TYPE",
        "ELASTICSEARCH_SHIM_AUTO_DETECT",
        "ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED",
        "DATAHUB_SYSTEM_CLIENT_ID",
        "DATAHUB_SYSTEM_CLIENT_SECRET",
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY",
        "DATAHUB_TOKEN_SERVICE_SALT",
        "METADATA_SERVICE_AUTH_ENABLED",
        "SERVER_PORT",
        "SERVER_ADDRESS",
        "DATAHUB_GMS_URL",
        "DATAHUB_GMS_HOST",
        "DATAHUB_GMS_PORT",
        "DATAHUB_BASE_URL",
    }
)


def build_envs(
    placeholders: Mapping[str, str],
    *,
    job: str | None,
    mode: str,
    adhoc: list[EnvFile],
    service_roles: tuple[str, ...] = (),
    extra_layers: tuple[Path, ...] = (),
    protected_keys: tuple[str, ...] = (),
    aliases: tuple[EnvAlias, ...] = (),
) -> dict[str, dict[str, EnvValue]]:
    envs = {}
    for service in ("gms", "system-update", "restore-indices", *service_roles):
        values = resolve(
            service,
            repo_root=REPO_ROOT,
            job=job,
            mode=mode,
            adhoc=adhoc,
            context=placeholders,
            shell=os.environ,
            extra_layers=extra_layers,
        )
        for alias in aliases:
            if alias.service == service:
                value = values.get(alias.source)
                if value is None and alias.source in os.environ:
                    value = EnvValue(os.environ[alias.source], "shell")
                if value is not None:
                    values[alias.name] = EnvValue(
                        value.value, f"{value.source} (via {alias.source})"
                    )
        # Resolve pins from the shipped base files, never user/job/extension layers.
        baseline = resolve(
            service,
            repo_root=REPO_ROOT,
            job=None,
            mode=mode,
            adhoc=(),
            context=placeholders,
            shell={},
        )
        values.update(
            {
                key: EnvValue(value.value, "protected")
                for key, value in baseline.items()
                if key in PROTECTED_KEYS
            }
        )
        if protected_keys:
            extra_baseline = resolve(
                service,
                repo_root=REPO_ROOT,
                job=None,
                mode=mode,
                adhoc=(),
                context=placeholders,
                shell={},
                extra_layers=extra_layers,
            )
            values.update(
                {
                    key: EnvValue(value.value, "protected")
                    for key, value in extra_baseline.items()
                    if key in protected_keys
                }
            )
        if service in ("gms", "system-update", "restore-indices"):
            values.update(
                {
                    key: EnvValue(value, "protected")
                    for key, value in SEARCH_CONNECTION_ENV.items()
                }
            )
        if service in ("system-update", "restore-indices"):
            values["SPRING_PROFILES_ACTIVE"] = EnvValue(
                with_upgrade_profile(process_env(values)), "launcher"
            )
        envs[service] = values
    return envs


def process_env(values: Mapping[str, EnvValue]) -> dict[str, str]:
    return {**os.environ, **{key: v.value for key, v in values.items()}}


def env_report(
    envs: Mapping[str, Mapping[str, Mapping[str, EnvValue]]],
) -> dict[str, object]:
    return {
        instance: {
            service: {
                key: {
                    "value": "<redacted>"
                    if v.value and any(marker in key for marker in SECRET_KEY_MARKERS)
                    else v.value,
                    "source": v.source,
                }
                for key, v in sorted(values.items())
            }
            for service, values in services.items()
        }
        for instance, services in envs.items()
    }


def instance_env(
    base: dict[str, str],
    instance: str,
    gms_port: int,
    os_port: int,
    data_dir: Path,
    credentials: Credentials,
    auth: bool,
    major: str,
    frontend_port: int | None,
) -> dict[str, str]:
    """GMS environment; auxiliary processes resolve their own service files."""
    placeholders = stack_context(
        instance, gms_port, os_port, data_dir, credentials, auth, major, frontend_port
    )
    values = build_envs(placeholders, job=None, mode=env_mode(None), adhoc=[])["gms"]
    values.update(
        {
            key: EnvValue(value, "override")
            for key, value in base.items()
            if key not in PROTECTED_KEYS and key not in SEARCH_CONNECTION_ENV
        }
    )
    (data_dir / instance / "object-storage").mkdir(parents=True, exist_ok=True)
    return process_env(values)


def resolved_environments(
    args: argparse.Namespace,
    bound: int,
    credentials: Credentials,
    extension: LocalExtension | None,
    description: Description | None,
) -> dict[str, dict[str, dict[str, EnvValue]]]:
    options = json.loads(args.extension_options)
    declared_ports = {f"gms-{i}": args.port_base + i for i in range(args.processes)}
    if description:
        declared_ports.update(
            ports_for(description, args.extension_slot, args.processes)
        )
    result = {}
    for i in range(args.processes):
        name = f"gms-{i}"
        ctx = context(REPO_ROOT, args.data_dir, name, declared_ports, options)
        placeholders = stack_context(
            name,
            args.port_base + i,
            bound,
            args.data_dir,
            credentials,
            args.auth,
            args.opensearch,
            args.frontend_port,
        )
        if extension:
            extra = extension.placeholders(ctx)
            if set(extra).intersection(placeholders):
                raise EnvLayerError("extension placeholders overlap core placeholders")
            placeholders.update(extra)
        # Auxiliary port roles do not describe processes; services() is authoritative.
        services = extension.services(ctx) if extension else ()
        roles = tuple(spec.role for spec in services)
        scopes = SCOPES | frozenset(roles)
        result[name] = build_envs(
            placeholders,
            job=args.job,
            mode=env_mode(args.mode),
            adhoc=[EnvFile.parse(spec, scopes) for spec in args.env_files],
            service_roles=roles,
            extra_layers=description.layer_dirs if description else (),
            protected_keys=description.protected_keys if description else (),
            aliases=description.env_aliases if description else (),
        )
        for role in roles:
            result[name][role]["DATAHUB_GMS_TOKEN"] = EnvValue(
                "<after-ready>" if args.auth else "", "launcher"
            )
        if (args.data_dir / name / "gms.mv.db").exists():
            result[name]["system-update"]["EBEAN_AUTOCREATE"] = EnvValue(
                "false", "protected"
            )
    return result


def with_upgrade_profile(env: dict[str, str]) -> str:
    profiles = [
        p.strip() for p in env.get("SPRING_PROFILES_ACTIVE", "").split(",") if p.strip()
    ]
    if "upgrade" not in profiles:
        profiles.append("upgrade")
    return ",".join(profiles)


def run_upgrade(
    processes: Processes,
    env: dict[str, str],
    log_dir: Path,
    instance: str,
    heap: str,
    upgrade: str,
    *,
    create: bool = False,
) -> None:
    upgrade_env = {
        **env,
        "SPRING_PROFILES_ACTIVE": with_upgrade_profile(env),
        "EBEAN_AUTOCREATE": str(create).lower(),
    }
    command = [
        JAVA,
        f"-Xmx{heap}",
        *shlex.split(env.get("GMS_LITE_UPGRADE_JAVA_OPTS", "")),
        *JVM_OPENS,
        "-jar",
        str(UPGRADE_JAR),
        "-u",
        upgrade,
    ]
    if upgrade == "RestoreIndices":
        command += [
            "-a",
            "batchSize=1000",
            "-a",
            "numThreads=2",
            "-a",
            "batchDelayMs=0",
        ]
    path = log_dir / f"{instance}-{upgrade.lower()}.log"
    log(f"{instance}: {upgrade} running (log: {path})")
    process = processes.start(command, upgrade_env, path)
    code = process.wait()
    if code:
        raise SystemExit(f"{upgrade} failed for {instance} (exit {code}); see {path}")
    processes.children.remove(process)


def ensure_usage_event_index(os_port: int, prefix: str) -> None:
    template = (
        REPO_ROOT
        / "datahub-upgrade/src/main/resources/index/usage-event/opensearch_template.json"
    )
    mappings = json.loads(
        template.read_text().replace("DUE_SHARDS", "1").replace("DUE_REPLICAS", "0")
    )["template"]["mappings"]
    request = urllib.request.Request(
        f"http://localhost:{os_port}/{prefix}_datahub_usage_event",
        data=json.dumps(
            {
                "mappings": mappings,
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(request, timeout=30):
            pass
    except urllib.error.HTTPError as error:
        if (
            error.code != 400
            or b"resource_already_exists_exception" not in error.read()
        ):
            raise


def mint_admin_token(url: str, env: dict[str, str]) -> str:
    # The system authenticator expects a clear-text id:secret after Basic.
    authorization = (
        f"Basic {env['DATAHUB_SYSTEM_CLIENT_ID']}:{env['DATAHUB_SYSTEM_CLIENT_SECRET']}"
    )
    request = urllib.request.Request(
        f"{url}/api/graphql",
        data=json.dumps(
            {
                "query": "mutation($input: CreateAccessTokenInput!) { createAccessToken(input: $input) { accessToken } }",
                "variables": {
                    "input": {
                        "type": "PERSONAL",
                        "actorUrn": "urn:li:corpuser:datahub",
                        "duration": "ONE_DAY",
                        "name": "gms-lite local development",
                    }
                },
            }
        ).encode(),
        headers={"Content-Type": "application/json", "Authorization": authorization},
    )
    deadline = time.monotonic() + 120
    last = ""
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = json.load(response)
            if not payload.get("errors"):
                return str(payload["data"]["createAccessToken"]["accessToken"])
            last = json.dumps(payload["errors"])
        except (urllib.error.URLError, OSError) as error:
            last = str(error)
        time.sleep(2)
    raise SystemExit(f"could not mint a local datahub access token on {url}: {last}")


def token_expires_at(token: str) -> str:
    # Read the server-issued expiry so discovery does not overstate the token's lifetime.
    payload = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    return (
        datetime.fromtimestamp(claims["exp"], tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def wait_admin_access(url: str, token: str, processes: Processes) -> None:
    # Bootstrap policies reach search through the spool, then the authorizer refreshes its cache.
    request = urllib.request.Request(
        f"{url}/api/graphql",
        data=json.dumps(
            {"query": "{ me { platformPrivileges { managePolicies } } }"}
        ).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        processes.check()
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                payload = json.load(response)
            if (
                not payload.get("errors")
                and payload["data"]["me"]["platformPrivileges"]["managePolicies"]
            ):
                return
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(2)
    raise SystemExit(f"bootstrap policies did not become available on {url}; see logs")


def run_stack(args: argparse.Namespace, processes: Processes) -> None:
    health_timeout = env_seconds("GMS_LITE_GMS_HEALTH_TIMEOUT", 600)
    env_seconds(OPENSEARCH_READY_TIMEOUT_ENV, OPENSEARCH_READY_TIMEOUT_SECONDS)
    extension = load(args.extension)
    options = json.loads(args.extension_options)
    description = extension.describe(options) if extension else None
    data_dir: Path = args.data_dir
    log_dir = data_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    timings = BootTimings(data_dir / "timings.json")
    if not args.skip_build:
        subprocess.run(
            [
                str(REPO_ROOT / "gradlew"),
                ":metadata-service:war:bootJar",
                ":datahub-upgrade:bootJar",
                ":metadata-service:opensearch-embedded:installDist",
                f"-PopensearchVersion={args.opensearch}",
                *(
                    [build.task for build in description.builds if build.required]
                    if description
                    else []
                ),
                *(description.build_arguments if description else ()),
                "-x",
                "test",
            ],
            cwd=REPO_ROOT,
            check=True,
        )
    for path in (WAR, UPGRADE_JAR, distribution_script(REPO_ROOT, args.opensearch)):
        if not path.exists():
            raise SystemExit(f"missing {path}; run without --skip-build")
    credentials = load_credentials(data_dir)
    with timings.phase("opensearch"):
        bound = start_opensearch(args, processes, log_dir)
    names = [f"gms-{i}" for i in range(args.processes)]
    declared_ports = {f"gms-{i}": args.port_base + i for i in range(args.processes)}
    if description:
        declared_ports.update(
            ports_for(description, args.extension_slot, args.processes)
        )
    resolved = resolved_environments(args, bound, credentials, extension, description)
    write_private(
        data_dir / "resolved-env.json",
        json.dumps(env_report(resolved), indent=2) + "\n",
    )
    contexts = {
        name: context(REPO_ROOT, data_dir, name, declared_ports, options)
        for name in names
    }
    args.extension_contexts = contexts
    environments = {
        name: process_env(values["gms"]) for name, values in resolved.items()
    }
    for name in names:
        (data_dir / name / "object-storage").mkdir(parents=True, exist_ok=True)
    if args.fresh and not args.private_opensearch:
        prefixes = {env["INDEX_PREFIX"] for env in environments.values()}
        prefixes.update(
            index_prefix(data_dir, name) for name in args.previous_instances
        )
        delete_prefixes(bound, sorted(prefixes))
    existing_indices = index_names(bound)
    checker = SystemUpdateChecker()
    for name, env in environments.items():
        write_private(
            data_dir / name / "env.sh",
            "\n".join(
                f"export {key}={shlex.quote(value)}"
                for key, value in sorted(env.items())
                if key not in os.environ or os.environ[key] != value
            )
            + "\n",
        )
        with timings.phase("usage_event_index"):
            ensure_usage_event_index(bound, env["INDEX_PREFIX"])
        if checker.is_complete(data_dir, name, env["INDEX_PREFIX"], existing_indices):
            log(f"{name}: warm start, bootstrap already completed")
        else:
            timings.warm = False
            with timings.phase("system_update"):
                run_upgrade(
                    processes,
                    process_env(resolved[name]["system-update"]),
                    log_dir,
                    name,
                    args.heap,
                    "SystemUpdate",
                    create=not (data_dir / name / "gms.mv.db").exists(),
                )
            with timings.phase("restore_indices"):
                run_upgrade(
                    processes,
                    process_env(resolved[name]["restore-indices"]),
                    log_dir,
                    name,
                    args.heap,
                    "RestoreIndices",
                )
            checker.mark_complete(
                data_dir, name, env["INDEX_PREFIX"], index_names(bound)
            )
    service_pids: dict[str, dict[str, int]] = {}
    with timings.phase("gms_boot"):
        for name, env in environments.items():
            child = processes.start(
                [JAVA, f"-Xmx{args.heap}", *JVM_OPENS, "-jar", str(WAR)],
                env,
                log_dir / f"{name}-gms.log",
            )
            service_pids.setdefault("gms", {})[name] = child.pid
        urls = {
            name: f"http://localhost:{args.port_base + i}"
            for i, name in enumerate(names)
        }
        for name, url in urls.items():
            deadline = time.monotonic() + health_timeout
            while not http_ok(f"{url}/health"):
                processes.check()
                if time.monotonic() >= deadline:
                    raise SystemExit(
                        f"{name} did not become healthy; see {log_dir / (name + '-gms.log')}"
                    )
                time.sleep(1)
            log(f"{name} healthy at {url}")
    with timings.phase("admin_token"):
        tokens = (
            {
                name: mint_admin_token(url, environments[name])
                for name, url in urls.items()
            }
            if args.auth
            else {}
        )
        for name, token in tokens.items():
            wait_admin_access(urls[name], token, processes)
    services: dict[str, dict[str, str]] = {}
    health: dict[str, dict[str, dict[str, object]]] = {}
    for name in names:
        ctx = context(
            REPO_ROOT,
            data_dir,
            name,
            declared_ports,
            options,
            tokens.get(name, ""),
        )
        contexts[name] = ctx
        if extension is None:
            continue
        for spec in extension.services(ctx):
            if (
                not spec.ports
                or spec.role not in spec.ports
                or any(role not in ctx.ports for role in spec.ports)
            ):
                raise SystemExit(f"service {spec.role} uses an undeclared port role")
            resolved[name][spec.role]["DATAHUB_GMS_TOKEN"] = EnvValue(
                tokens.get(name, ""), "launcher"
            )
            path = log_dir / f"{name}-{spec.role}.log"
            with timings.phase(f"service:{spec.role}"):
                child = None
                try:
                    child = processes.start(
                        list(spec.argv),
                        {
                            **process_env(resolved[name][spec.role]),
                            "DATAHUB_GMS_TOKEN": tokens.get(name, ""),
                        },
                        path,
                        cwd=spec.cwd,
                        required=spec.required,
                    )
                    deadline = time.monotonic() + spec.timeout
                    while not http_ok(spec.readiness_url):
                        processes.check()
                        if child.poll() is not None or time.monotonic() >= deadline:
                            raise RuntimeError(
                                f"{name}/{spec.role} did not become healthy; see {path}"
                            )
                        time.sleep(1)
                    service_pids.setdefault(spec.role, {})[name] = child.pid
                    services.setdefault(spec.role, {})[name] = (
                        f"http://localhost:{ctx.ports[spec.role]}"
                    )
                    health.setdefault(spec.role, {})[name] = {
                        "url": spec.readiness_url,
                        "required": spec.required,
                    }
                    log(f"{name}/{spec.role} healthy")
                except (OSError, RuntimeError) as error:
                    if spec.required:
                        raise SystemExit(str(error)) from error
                    if child is not None:
                        processes.stop_group(child)
                        if child in processes.children:
                            processes.children.remove(child)
                        processes.optional.discard(child.pid)
                    log(f"optional service: {error}")
        extension.after_ready(ctx)
    stack: StackInfo = {
        "services": services,
        "serviceHealth": health,
        "servicePids": service_pids,
        "bootId": secrets.token_hex(16),
        "opensearchVersion": args.opensearch,
        "opensearchPort": bound,
        "opensearchShared": not args.private_opensearch,
        "default": names[0],
        "instances": urls,
        "indexPrefixes": {
            name: env["INDEX_PREFIX"] for name, env in environments.items()
        },
        "auth": args.auth,
        "frontendPort": args.frontend_port,
        "systemClientId": "__datahub_system" if args.auth else None,
        "systemClientSecret": credentials["system_client_secret"]
        if args.auth
        else None,
        "adminTokens": tokens,
        "adminTokenExpiresAt": {
            name: token_expires_at(token) for name, token in tokens.items()
        },
    }
    result = timings.save()
    write_private(
        data_dir / "resolved-env.json",
        json.dumps(env_report(resolved), indent=2) + "\n",
    )
    write_private(data_dir / "stack.json", json.dumps(stack, indent=2) + "\n")
    split = " ".join(f"{name}={value:.1f}s" for name, value in timings.phases.items())
    log(
        f"boot timings: {split} total={result['total_s']:.1f}s warm={timings.warm} data_dir={dir_size_mb(data_dir):.0f} MB"
    )
    log(f"stack ready: {data_dir / 'stack.json'}")
    while True:
        processes.check()
        time.sleep(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--processes", type=int, default=1, help="isolated GMS processes (default: 1)"
    )
    parser.add_argument(
        "--port-base",
        type=int,
        default=8090,
        help="first GMS HTTP port (default: 8090)",
    )
    parser.add_argument(
        "--opensearch-port",
        type=int,
        default=None,
        help="start a private OpenSearch on this port (0: ephemeral)",
    )
    parser.add_argument(
        "--private-opensearch",
        action="store_true",
        help="own a node that stops with this stack",
    )
    parser.add_argument(
        "--shared-opensearch-dir",
        type=Path,
        default=None,
        help="shared node state (or GMS_LITE_OPENSEARCH_DIR)",
    )
    parser.add_argument(
        "--frontend-port",
        type=int,
        default=None,
        help="frontend port written to stack.json for Vite lite mode",
    )
    parser.add_argument("--job", choices=tuple(JOB_DIRS))
    parser.add_argument("--mode", choices=MODES)
    parser.add_argument(
        "--print-env",
        action="store_true",
        help="show redacted resolved env and boot nothing",
    )
    parser.add_argument("--opensearch", choices=("2", "3"))
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument(
        "--env-file",
        dest="env_files",
        action="append",
        default=[],
        help="ad-hoc [scope:]PATH after env layers; repeatable, later wins",
    )
    parser.add_argument(
        "--heap", default="1g", help="heap cap per GMS/upgrade JVM (default: 1g)"
    )
    parser.add_argument(
        "--no-auth",
        action="store_false",
        dest="auth",
        help="disable token authentication on local GMS",
    )
    parser.add_argument(
        "--fresh", action="store_true", help="clear this stack's data before starting"
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="reuse existing jars and OpenSearch installDist",
    )
    parser.add_argument(
        "--extension", help="optional module:attribute service provider"
    )
    parser.add_argument("--extension-options", default="{}")
    parser.add_argument("--extension-slot", type=int, default=0)
    args = parser.parse_args()
    extension = load(args.extension)
    description = (
        extension.describe(json.loads(args.extension_options)) if extension else None
    )
    args.extension_contexts = {}
    if args.processes < 1 or not 1 <= args.port_base <= 65536 - args.processes:
        parser.error(
            "--processes must be positive and all GMS ports must be in 1..65535"
        )
    ports = list(range(args.port_base, args.port_base + args.processes))
    if args.frontend_port is not None:
        if not 1 <= args.frontend_port <= 65535 or args.frontend_port in ports:
            parser.error("--frontend-port must be a distinct port in 1..65535")
    if args.opensearch_port is not None and (
        not 0 <= args.opensearch_port <= 65535 or args.opensearch_port in ports
    ):
        parser.error("--opensearch-port must be 0 or a distinct port in 1..65535")
    if args.opensearch_port:
        ports.append(args.opensearch_port)
    args.private_opensearch = (
        args.private_opensearch or args.opensearch_port is not None
    )
    if description:
        additional = list(
            ports_for(description, args.extension_slot, args.processes).values()
        )
        if set(additional).intersection(
            ports + ([args.frontend_port] if args.frontend_port else [])
        ):
            parser.error("extension ports overlap core ports")
        ports.extend(additional)
    args.data_dir = args.data_dir.expanduser().resolve()
    version_file = args.data_dir / "opensearch-major"
    recorded = (
        version_file.read_text().strip()
        if version_file.exists()
        else ("3" if (args.data_dir / ".gms-lite").exists() else None)
    )
    args.opensearch = validate_major(args.opensearch or recorded or selected_major())
    args.shared_opensearch_dir = args.shared_opensearch_dir or default_shared_dir(
        args.opensearch
    )
    try:
        preview = resolved_environments(
            args, args.opensearch_port or 0, PLACEHOLDER_SECRETS, extension, description
        )
    except EnvLayerError as error:
        parser.error(str(error))
    if args.print_env:
        print(json.dumps(env_report(preview), indent=2))
        return
    ensure_ports_available(ports)
    processes = Processes(args.data_dir)

    def interrupted(signum: int, frame: object) -> None:
        # Ignore repeated group signals while finally flushes the spool and stops children.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    with data_directory(
        args.data_dir,
        args.fresh,
        args.opensearch,
        processes=args.processes,
        private_opensearch=args.private_opensearch,
    ) as previous_instances:
        args.previous_instances = previous_instances
        try:
            run_stack(args, processes)
        finally:
            processes.stop()
            if extension:
                for ctx in reversed(list(args.extension_contexts.values())):
                    extension.after_stop(ctx)


if __name__ == "__main__":
    main()
