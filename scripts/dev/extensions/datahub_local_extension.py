"""Typed, process-independent inputs for optional local stack services (stdlib only)."""

from __future__ import annotations

import argparse
import importlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Protocol, cast

Option = str | int | bool | None
Options = dict[str, Option]


@dataclass(frozen=True)
class Build:
    task: str
    artifact: Path
    required: bool = True


@dataclass(frozen=True)
class EnvAlias:
    service: str
    name: str
    source: str


@dataclass(frozen=True)
class Description:
    # Every role reserves one port per instance before allocation, including auxiliary ports.
    port_bases: Mapping[str, int] = field(default_factory=dict)
    builds: tuple[Build, ...] = ()
    build_arguments: tuple[str, ...] = ()
    search_plugins: tuple[str, ...] = ()
    layer_dirs: tuple[Path, ...] = ()
    artifact_globs: tuple[str, ...] = ()
    protected_keys: tuple[str, ...] = ()
    env_aliases: tuple[EnvAlias, ...] = ()


@dataclass(frozen=True)
class Context:
    repo_root: Path
    data_dir: Path
    instance: str
    ports: Mapping[str, int]
    options: Mapping[str, Option]
    token: str = ""


@dataclass(frozen=True)
class Service:
    role: str
    argv: tuple[str, ...]
    cwd: Path
    ports: tuple[str, ...]
    readiness_url: str
    timeout: float = 120
    required: bool = True


class LocalExtension(Protocol):
    id: str
    schema_version: int

    def add_arguments(self, parser: argparse.ArgumentParser) -> None: ...
    def describe(self, options: Mapping[str, Option]) -> Description: ...
    def placeholders(self, ctx: Context) -> Mapping[str, str]: ...
    def services(self, ctx: Context) -> tuple[Service, ...]: ...
    def after_ready(self, ctx: Context) -> None: ...
    def after_stop(self, ctx: Context) -> None: ...
    def after_nuke(self, ctx: Context) -> None: ...


def load(reference: str | None) -> LocalExtension | None:
    if not reference:
        return None
    module, separator, attribute = reference.partition(":")
    if not separator or not module or not attribute:
        raise ValueError("extension must be module:attribute")
    extension = cast(
        LocalExtension, getattr(importlib.import_module(module), attribute)
    )
    if not extension.id or extension.schema_version < 1:
        raise ValueError("extension requires an id and a positive schema_version")
    return extension


def configured_options(
    extension: LocalExtension, args: argparse.Namespace, previous: Mapping[str, Option]
) -> Options:
    parser = argparse.ArgumentParser(add_help=False)
    extension.add_arguments(parser)
    defaults = vars(parser.parse_args([]))
    options: Options = {}
    for key, default in defaults.items():
        previous_value = previous.get(key, default)
        # Older providers persisted store_true negative flags. Preserve that choice when
        # migrating to BooleanOptionalAction's positive destination names.
        negative = previous.get(f"no_{key}")
        if key not in previous and isinstance(negative, bool):
            previous_value = not negative
        explicit = getattr(args, key, None)
        options[key] = explicit if explicit is not None else previous_value
    return options


def ports_for(description: Description, slot: int, processes: int) -> dict[str, int]:
    ports = {
        f"{role}-{i}": base + slot * 1000 + i
        for role, base in description.port_bases.items()
        for i in range(processes)
    }
    if len(set(ports.values())) != len(ports) or any(
        not 1 <= port <= 65535 for port in ports.values()
    ):
        raise ValueError("extension port roles overlap or exceed 1..65535")
    return ports


def context(
    repo_root: Path,
    data_dir: Path,
    instance: str,
    ports: Mapping[str, int],
    options: Mapping[str, Option],
    token: str = "",
) -> Context:
    ordinal = instance.removeprefix("gms-")
    instance_ports = {
        role.removesuffix(f"-{ordinal}"): port
        for role, port in ports.items()
        if role.endswith(f"-{ordinal}")
    }
    return Context(repo_root, data_dir, instance, instance_ports, options, token)
