"""Layered env files for the Docker-free stack. Stdlib only; env/README.md describes the layout.

For one service, values resolve from lowest to highest precedence:

1. ``os.environ``, which only passes through keys no file sets.
2. For each layer in ``env/common``, ``env/docker-free`` and the job's folder (``--job``):
   ``all.env``, then the service's files, then the same names under ``local/`` (no CI) or ``ci/``.
3. Ad-hoc ``--env-file [scope:]PATH`` files, in command-line order.
4. The shell, only for keys whose final declaration is ``KEY=${KEY:-default}`` (as in compose).

``${GMS_LITE_*}`` placeholders expand from a context the launcher computes (ports, paths, secrets).
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

# The files each service reads in a layer, after all.env. system-update shares the GMS files and
# adds its own; restore-indices must not, or it would pick up EBEAN_AUTOCREATE (drop + recreate).
SERVICE_FILES: dict[str, tuple[str, ...]] = {
    "gms": ("gms",),
    "system-update": ("gms", "system-update"),
    "restore-indices": ("gms",),
}
SCOPES = frozenset(name for names in SERVICE_FILES.values() for name in names)
JOB_DIRS: dict[str, str] = {
    "smoke-test": "smoke-test/env",
    "playwright": "e2e-test/ui/playwright/env",
}
MODES = ("local", "ci")

_OPT_IN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*):-(.*)\}", re.DOTALL)
_TOKEN = re.compile(r"\$\$|\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$")


class EnvLayerError(ValueError):
    pass


@dataclass(frozen=True)
class EnvValue:
    value: str
    source: str


@dataclass(frozen=True)
class EnvFile:
    """An ad-hoc ``--env-file``; ``scope`` limits it to the services that read that file name."""

    path: Path
    scope: str | None = None

    @classmethod
    def parse(cls, spec: str, scopes: frozenset[str] = SCOPES) -> EnvFile:
        scope, sep, path = spec.partition(":")
        if sep and scope in scopes:
            return cls(Path(path), scope)
        return cls(Path(spec))


def read_env_file(path: Path) -> dict[str, str]:
    """KEY=VALUE lines; `export ` prefix and comments tolerated. A missing file reads as empty."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def layer_files(
    repo_root: Path,
    service: str,
    job: str | None,
    mode: str,
    extra_layers: Sequence[Path] = (),
) -> list[Path]:
    """The convention files for ``service``, lowest precedence first."""
    layers = [repo_root / "env/common", repo_root / "env/docker-free"]
    layers.extend(extra_layers)
    if job is not None:
        layers.append(repo_root / JOB_DIRS[job])
    names = ("all", *SERVICE_FILES.get(service, (service,)))
    return [
        folder / f"{name}.env"
        for layer in layers
        for folder in (layer, layer / mode)
        for name in names
    ]


def resolve(
    service: str,
    *,
    repo_root: Path,
    job: str | None,
    mode: str,
    adhoc: Sequence[EnvFile],
    context: Mapping[str, str],
    shell: Mapping[str, str],
    extra_layers: Sequence[Path] = (),
) -> dict[str, EnvValue]:
    """Every key the files set for ``service``, expanded, with the file (or shell) it came from."""
    declared: dict[str, tuple[str, str]] = {}
    for path in layer_files(repo_root, service, job, mode, extra_layers):
        for key, raw in read_env_file(path).items():
            declared[key] = (raw, _display(path, repo_root))
    for env_file in adhoc:
        if env_file.scope is not None and env_file.scope not in SERVICE_FILES.get(
            service, (service,)
        ):
            continue
        if not env_file.path.is_file():
            raise EnvLayerError(f"--env-file {env_file.path} does not exist")
        for key, raw in read_env_file(env_file.path).items():
            declared[key] = (raw, _display(env_file.path, repo_root))
    return {
        key: _expand(key, raw, source, context, shell)
        for key, (raw, source) in declared.items()
    }


def _expand(
    key: str,
    raw: str,
    source: str,
    context: Mapping[str, str],
    shell: Mapping[str, str],
) -> EnvValue:
    opt_in = _OPT_IN.fullmatch(raw)
    if opt_in is not None:
        if opt_in[1] != key:
            raise EnvLayerError(
                f"{source}: {key}=${{{opt_in[1]}:-...}}: a shell opt-in may only name {key} itself"
            )
        if key in shell:
            return EnvValue(shell[key], f"shell (opted in by {source})")
        raw = opt_in[2]

    def substitute(match: re.Match[str]) -> str:
        if match[0] == "$$":
            return "$"
        name = match[1]
        if name is None:
            raise EnvLayerError(
                f"{source}: {key}: unsupported '$' (use $$ for a literal $)"
            )
        if name not in context:
            raise EnvLayerError(f"{source}: {key}: unknown placeholder ${{{name}}}")
        return context[name]

    return EnvValue(_TOKEN.sub(substitute, raw), source)


def _display(path: Path, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        return str(path)
