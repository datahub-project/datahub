"""Checks on user-supplied package specifications before the executor installs them.

``extra_pip_requirements`` and the plugin names from an ingestion recipe are written
by whoever can edit the ingestion source, and installing a package runs its build
scripts, so these strings must not be able to do more than name a distribution.
The structural checks here are always on. The enable flag and the allowlist are
set by the executor's operator through environment variables.
"""

import dataclasses
import json
import re
from collections.abc import Iterable, Mapping
from typing import Optional

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

from datahub.executor.common.env_config import (
    EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV,
    EXTRA_PIP_REQUIREMENTS_ENABLED_ENV,
    get_extra_pip_requirements_allowlist,
    get_extra_pip_requirements_enabled,
)

# Environment variables that steer uv and pip: package indexes, find-links,
# constraint and config files, TLS trust. The install step only sees the
# executor's own environment, never a recipe's extra_env_vars, so these are
# rejected in a recipe outright rather than silently ignored: they belong in the
# executor's environment, set by its operator.
_INSTALL_ENV_PREFIXES = ("UV_", "PIP_")

# A PEP 508 extras identifier. Plugin names are spliced into ``acryl-datahub[<plugins>]``
# and into venv directory names, so ``]``, ``@``, ``/``, ``;`` or whitespace in one
# would change what gets installed, or where.
_PLUGIN_NAME_RE = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$")


def validate_plugin_names(names: Iterable[Optional[str]]) -> None:
    """Reject plugin names that cannot be spliced safely into a requirement line."""
    invalid = [
        name
        for name in names
        if name is not None and name != "" and not _PLUGIN_NAME_RE.match(name)
    ]
    if invalid:
        raise ValueError(
            f"Invalid plugin name(s) {invalid}: a plugin name may only contain "
            "letters, digits, '.', '-' and '_'."
        )


def validate_install_env_vars(extra_env_vars: Mapping[str, object]) -> None:
    """Reject recipe env vars that are install settings.

    They would have no effect, since the install step does not see a recipe's
    env vars, so failing early points the author at the executor's environment.
    """
    steering = [
        name
        for name in extra_env_vars
        if name.strip().upper().startswith(_INSTALL_ENV_PREFIXES)
    ]
    if steering:
        raise ValueError(
            f"extra_env_vars {steering} are package install settings (uv and pip "
            "settings such as package indexes) and are not accepted in a recipe. "
            "Set them in the executor's environment instead."
        )


def _structural_problem(entry: str) -> Optional[str]:
    """Why ``entry`` cannot be written to a requirements file as a single package.

    Entries are written one per line, so a line break would smuggle in a second
    line, and a line starting with ``-`` is parsed as an option such as
    ``--index-url`` (redirecting every install) or ``-r`` (reading a file on the
    executor). Options after the package, such as ``--hash``, are rejected too.
    """
    if any(ord(ch) < 32 or ch == "\x7f" for ch in entry):
        return "contains a line break or control character"
    if any(token.startswith("-") for token in entry.split()):
        return (
            "looks like a pip option rather than a package; options such as "
            "--index-url, --extra-index-url or -r are not accepted here "
            "(configure package indexes with UV_INDEX_URL / UV_EXTRA_INDEX_URL "
            "on the executor instead)"
        )
    return None


def _parse_requirement(text: str) -> Optional[Requirement]:
    try:
        return Requirement(text)
    except InvalidRequirement:
        return None


@dataclasses.dataclass(frozen=True)
class _AllowlistEntry:
    raw: str
    requirement: Optional[Requirement]

    def matches(self, candidate: str) -> bool:
        if candidate.strip() == self.raw:
            return True
        requirement = _parse_requirement(candidate)
        if requirement is None or self.requirement is None:
            return False
        allowed = self.requirement
        if canonicalize_name(requirement.name) != canonicalize_name(allowed.name):
            return False
        if allowed.url is not None:
            return requirement.url == allowed.url
        if requirement.url is not None:
            # A direct reference (``name @ url`` or ``name@/path``) bypasses the
            # package index, so it needs an allowlist entry with that same URL.
            return False
        if not allowed.specifier:
            # A bare package name allows any version and extras of that package.
            return True
        return requirement.specifier == allowed.specifier


def parse_allowlist(value: str) -> list[_AllowlistEntry]:
    """Parse the allowlist env var: a JSON array, or comma-separated entries.

    Use the JSON form when an entry itself contains a comma (``pkg>=1,<2``).
    """
    stripped = value.strip()
    if stripped.startswith("["):
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"{EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV} is not a valid JSON array: {e}"
            ) from e
        if not isinstance(parsed, list) or not all(
            isinstance(item, str) for item in parsed
        ):
            raise ValueError(
                f"{EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV} must be a JSON array of strings"
            )
        items = parsed
    else:
        items = stripped.split(",")
    entries = [item.strip() for item in items if item.strip()]
    return [_AllowlistEntry(raw=e, requirement=_parse_requirement(e)) for e in entries]


def validate_extra_pip_requirements(
    raw_requirements: list[str], expanded_requirements: list[str]
) -> None:
    """Check expanded requirements against the structural rules and operator policy.

    ``raw_requirements`` are the recipe's strings and ``expanded_requirements``
    the same strings after env-var expansion. Policy is applied to the expanded
    value, which is what gets installed; error messages quote the raw value so
    that a token expanded from the executor's environment is never echoed back.
    """
    pairs = [
        (raw, expanded)
        for raw, expanded in zip(raw_requirements, expanded_requirements, strict=True)
        if expanded.strip()
    ]
    problems = []
    for raw, expanded in pairs:
        problem = _structural_problem(expanded)
        if problem:
            problems.append(f"{raw!r} {problem}")
    if problems:
        raise ValueError(
            "Rejected extra_pip_requirements entries: " + "; ".join(problems)
        )
    if not pairs:
        return

    if not get_extra_pip_requirements_enabled():
        raise ValueError(
            "extra_pip_requirements is disabled on this executor "
            f"({EXTRA_PIP_REQUIREMENTS_ENABLED_ENV}=false). Remove it from the "
            "recipe, or have the packages added to the executor image."
        )

    allowlist_value = get_extra_pip_requirements_allowlist()
    if allowlist_value is None:
        return
    allowlist = parse_allowlist(allowlist_value)
    rejected = [
        raw
        for raw, expanded in pairs
        if not any(entry.matches(expanded) for entry in allowlist)
    ]
    if rejected:
        raise ValueError(
            f"extra_pip_requirements entries {rejected} are not on this executor's "
            f"package allowlist ({EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV}). Ask an "
            "administrator to allow them."
        )
