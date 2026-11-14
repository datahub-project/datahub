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
import hashlib
import json
import os
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO, TypedDict, cast

from _opensearch_manager import (
    SharedOpenSearchManager,
    default_shared_dir,
    delete_prefixes,
    index_names,
    wait_for_ready,
)
from _system_update_checker import SystemUpdateChecker

REPO_ROOT = Path(__file__).resolve().parents[3]
WAR = REPO_ROOT / "metadata-service/war/build/libs/war.war"
UPGRADE_JAR = REPO_ROOT / "datahub-upgrade/build/libs/datahub-upgrade.jar"
OPENSEARCH = (
    REPO_ROOT
    / "metadata-service/opensearch-embedded/build/install/opensearch-embedded/bin/opensearch-embedded"
)
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
    opensearchPort: int
    opensearchShared: bool
    default: str
    instances: dict[str, str]
    indexPrefixes: dict[str, str]
    auth: bool
    adminTokens: dict[str, str]


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
def data_directory(data_dir: Path, fresh: bool) -> Iterator[list[str]]:
    """Keep the lock outside the directory so --fresh cannot unlink a live lock."""
    data_dir.parent.mkdir(parents=True, exist_ok=True)
    with (data_dir.parent / f".{data_dir.name}.gms-lite.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SystemExit(f"data directory is already in use: {data_dir}") from error
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
            shutil.rmtree(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        marker.touch()
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
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def http_ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError):
        return False


class Processes:
    """Own a separate process group per service, including any grandchildren."""

    def __init__(self) -> None:
        self.children: list[subprocess.Popen[str]] = []
        self.logs: list[TextIO] = []

    def start(
        self, command: list[str], env: dict[str, str], path: Path, *, pipe: bool = False
    ) -> subprocess.Popen[str]:
        output = path.open("w")
        self.logs.append(output)
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            env=env,
            stdout=subprocess.PIPE if pipe else output,
            stderr=output if pipe else subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        self.children.append(process)
        return process

    def check(self) -> None:
        for child in self.children:
            if child.poll() is not None:
                raise SystemExit(
                    f"service PID {child.pid} exited with {child.returncode}; see logs"
                )

    def stop(self) -> None:
        # Stop GMS before OpenSearch so the queue spool and final index writes can flush.
        for child in reversed(self.children):
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                continue
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
        for output in self.logs:
            output.close()
        for child in self.children:
            if child.stdout is not None:
                child.stdout.close()


def index_prefix(data_dir: Path, instance: str) -> str:
    # A path hash avoids collisions between equally named directories in different worktrees.
    stack_id = hashlib.sha256(str(data_dir.resolve()).encode()).hexdigest()[:12]
    return f"gmslite{stack_id}{instance.replace('-', '')}"


def start_opensearch(
    args: argparse.Namespace, processes: Processes, log_dir: Path
) -> int:
    if not args.private_opensearch:
        manager = SharedOpenSearchManager(args.shared_opensearch_dir)
        bound = manager.get_or_start()
        log(f"Shared OpenSearch ready on port {bound}; survives stack shutdown")
        log(
            "To stop it: python3 "
            + shlex.join(
                [
                    str(Path(__file__).with_name("_opensearch_manager.py")),
                    "stop",
                    "--dir",
                    str(manager.shared_dir),
                ]
            )
        )
        return bound
    process = processes.start(
        [
            str(OPENSEARCH),
            "--port",
            str(args.opensearch_port or 0),
            "--data",
            str(args.data_dir / "opensearch"),
        ],
        dict(os.environ),
        log_dir / "opensearch.log",
        pipe=True,
    )
    bound = wait_for_ready(
        process, processes.logs[-1], OPENSEARCH_READY_TIMEOUT_SECONDS
    )
    if bound is None:
        raise SystemExit(
            f"embedded OpenSearch did not report READY; see {log_dir / 'opensearch.log'}"
        )
    log(f"Private OpenSearch ready on port {bound}")
    return bound


def instance_env(
    base: dict[str, str],
    instance: str,
    gms_port: int,
    os_port: int,
    data_dir: Path,
    credentials: Credentials,
    auth: bool,
) -> dict[str, str]:
    directory = data_dir / instance
    directory.mkdir(parents=True, exist_ok=True)
    storage = directory / "object-storage"
    storage.mkdir(exist_ok=True)
    return {
        **os.environ,
        **base,
        "EBEAN_DATASOURCE_URL": f"jdbc:h2:file:{directory / 'gms'};MODE=MySQL",
        "EBEAN_DATASOURCE_DRIVER": "org.h2.Driver",
        "EBEAN_DATASOURCE_USERNAME": "sa",
        "EBEAN_DATASOURCE_PASSWORD": "",
        # Ebean autocreate drops existing tables. Only a new database may enable it.
        "EBEAN_AUTOCREATE": "false",
        "DATAHUB_SQL_SETUP_ENABLED": "false",
        "DATAHUB_MESSAGING_TRANSPORT": "pgqueue",
        "DATAHUB_PGQUEUE_ENABLED": "true",
        "DATAHUB_PGQUEUE_STORE": "inmemory",
        "DATAHUB_PGQUEUE_INMEMORY_SPOOL_FILE": str(directory / "queue-spool.json"),
        "SCHEMA_REGISTRY_TYPE": "INTERNAL",
        "KAFKA_SCHEMAREGISTRY_URL": f"http://localhost:{gms_port}/schema-registry/api",
        "MAE_CONSUMER_ENABLED": "true",
        "MCE_CONSUMER_ENABLED": "true",
        "PE_CONSUMER_ENABLED": "true",
        "DATAHUB_OBJECT_STORAGE_URI": storage.as_uri(),
        "ELASTICSEARCH_HOST": "localhost",
        "ELASTICSEARCH_PORT": str(os_port),
        "ELASTICSEARCH_SHIM_ENGINE_TYPE": "OPENSEARCH_3",
        "ELASTICSEARCH_SHIM_AUTO_DETECT": "false",
        "ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED": "true",
        "INDEX_PREFIX": index_prefix(data_dir, instance),
        "GRAPH_SERVICE_IMPL": "elasticsearch",
        "ELASTICSEARCH_NUM_SHARDS_PER_INDEX": "1",
        "ELASTICSEARCH_NUM_REPLICAS_PER_INDEX": "0",
        # The embedded node has no ISM. Use the shipped mappings without lifecycle settings.
        "SKIP_CREATE_USAGE_EVENT_INDICES_STEP": "true",
        "BOOTSTRAP_SYSTEM_UPDATE_WAIT_FOR_SYSTEM_UPDATE": "false",
        "METADATA_SERVICE_AUTH_ENABLED": str(auth).lower(),
        "POLICY_CACHE_REFRESH_INTERVAL_SECONDS": "10",
        "DATAHUB_SYSTEM_CLIENT_ID": "__datahub_system",
        "DATAHUB_SYSTEM_CLIENT_SECRET": credentials["system_client_secret"],
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": credentials["token_signing_key"],
        "DATAHUB_TOKEN_SERVICE_SALT": credentials["token_salt"],
        "ENTITY_REGISTRY_CONFIG_PATH": str(
            REPO_ROOT / "metadata-models/src/main/resources/entity-registry.yml"
        ),
        "DATAHUB_GMS_HOST": "localhost",
        "DATAHUB_GMS_PORT": str(gms_port),
        "SERVER_PORT": str(gms_port),
        "SERVER_ADDRESS": "127.0.0.1",
    }


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
    command = [JAVA, f"-Xmx{heap}", *JVM_OPENS, "-jar", str(UPGRADE_JAR), "-u", upgrade]
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
    data_dir: Path = args.data_dir
    log_dir = data_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    if not args.skip_build:
        subprocess.run(
            [
                str(REPO_ROOT / "gradlew"),
                ":metadata-service:war:bootJar",
                ":datahub-upgrade:bootJar",
                ":metadata-service:opensearch-embedded:installDist",
                "-x",
                "test",
            ],
            cwd=REPO_ROOT,
            check=True,
        )
    for path in (WAR, UPGRADE_JAR, OPENSEARCH):
        if not path.exists():
            raise SystemExit(f"missing {path}; run without --skip-build")
    base = read_env_file(args.env_file)
    credentials = load_credentials(data_dir)
    bound = start_opensearch(args, processes, log_dir)
    names = [f"gms-{i}" for i in range(args.processes)]
    environments = {
        name: instance_env(
            base, name, args.port_base + i, bound, data_dir, credentials, args.auth
        )
        for i, name in enumerate(names)
    }
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
        search_data_exists = any(
            index.startswith(env["INDEX_PREFIX"] + "_")
            and index != env["INDEX_PREFIX"] + "_datahub_usage_event"
            for index in existing_indices
        )
        ensure_usage_event_index(bound, env["INDEX_PREFIX"])
        if checker.is_complete(data_dir, name) and search_data_exists:
            log(f"{name}: warm start, bootstrap already completed")
        else:
            run_upgrade(
                processes,
                env,
                log_dir,
                name,
                args.heap,
                "SystemUpdate",
                create=not (data_dir / name / "gms.mv.db").exists(),
            )
            run_upgrade(processes, env, log_dir, name, args.heap, "RestoreIndices")
            checker.mark_complete(data_dir, name)
    for name, env in environments.items():
        processes.start(
            [JAVA, f"-Xmx{args.heap}", *JVM_OPENS, "-jar", str(WAR)],
            env,
            log_dir / f"{name}-gms.log",
        )
    urls = {
        name: f"http://localhost:{args.port_base + i}" for i, name in enumerate(names)
    }
    for name, url in urls.items():
        deadline = time.monotonic() + 600
        while not http_ok(f"{url}/health"):
            processes.check()
            if time.monotonic() >= deadline:
                raise SystemExit(
                    f"{name} did not become healthy; see {log_dir / (name + '-gms.log')}"
                )
            time.sleep(1)
        log(f"{name} healthy at {url}")
    tokens = (
        {name: mint_admin_token(url, environments[name]) for name, url in urls.items()}
        if args.auth
        else {}
    )
    for name, token in tokens.items():
        wait_admin_access(urls[name], token, processes)
    stack: StackInfo = {
        "bootId": secrets.token_hex(16),
        "opensearchPort": bound,
        "opensearchShared": not args.private_opensearch,
        "default": names[0],
        "instances": urls,
        "indexPrefixes": {
            name: env["INDEX_PREFIX"] for name, env in environments.items()
        },
        "auth": args.auth,
        "adminTokens": tokens,
    }
    write_private(data_dir / "stack.json", json.dumps(stack, indent=2) + "\n")
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
        default=default_shared_dir(),
        help="shared node state (or GMS_LITE_OPENSEARCH_DIR)",
    )
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument(
        "--env-file",
        type=Path,
        help="optional KEY=VALUE file; connection settings stay isolated",
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
    args = parser.parse_args()
    if args.processes < 1 or not 1 <= args.port_base <= 65536 - args.processes:
        parser.error(
            "--processes must be positive and all GMS ports must be in 1..65535"
        )
    ports = list(range(args.port_base, args.port_base + args.processes))
    if args.opensearch_port is not None and (
        not 0 <= args.opensearch_port <= 65535 or args.opensearch_port in ports
    ):
        parser.error("--opensearch-port must be 0 or a distinct port in 1..65535")
    if args.opensearch_port:
        ports.append(args.opensearch_port)
    args.private_opensearch = (
        args.private_opensearch or args.opensearch_port is not None
    )
    ensure_ports_available(ports)
    args.data_dir = args.data_dir.expanduser().resolve()
    processes = Processes()

    def interrupted(signum: int, frame: object) -> None:
        # Ignore repeated group signals while finally flushes the spool and stops children.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    with data_directory(args.data_dir, args.fresh) as previous_instances:
        args.previous_instances = previous_instances
        try:
            run_stack(args, processes)
        finally:
            processes.stop()


if __name__ == "__main__":
    main()
