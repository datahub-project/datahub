#!/usr/bin/env python3
"""
Flag locked C-extension packages missing wheels for supported Python versions.

Parses uv.lock wheel filenames. Handles abi3, py3-none-any, and standard cpXY tags.
A package version is only checked against the Python minors it is actually
installed on, per its environment marker in constraints.txt (the lock forks some
packages per Python version, e.g. pandas 2.1.x for <3.13 and 2.3.x for 3.13).
Exits non-zero if gaps are found (unless the package is in KNOWN_EXCEPTIONS).
"""

import re
import sys
from pathlib import Path
from typing import Collection, Dict, List, Optional, Set, Tuple

import toml
from packaging.markers import Marker

METADATA_INGESTION_DIR = Path(__file__).resolve().parent.parent

# uv.lock is resolved for 3.11-3.13 (see [tool.uv] environments in
# pyproject.toml), so only cp311-cp313 coverage is required; older-CPython wheels
# the lock also lists are ignored. Published python_requires stays >=3.10; extras
# that pull unstructured (numba/llvmlite/blis/spacy, ...) fail closed via that
# stack's own requires-python rather than this lock check.
REQUIRED_PYTHON_MINORS = {11, 12, 13}

KNOWN_EXCEPTIONS = {
    "kerberos",  # Unmaintained, source-only. Needs replacement with krb5/gssapi.
    "python-ldap",  # Source-only, requires OpenLDAP headers.
    "scipy",  # 1.17+ dropped 3.10; extras that pull scipy need 3.11+ wheels.
    "sqlalchemy",  # 1.4.x (required by db2 plugin, <2) has no 3.12 wheels; builds from source.
}


def parse_uv_lock():
    data = toml.loads((METADATA_INGESTION_DIR / "uv.lock").read_text())
    packages = []
    for pkg in data.get("package", []):
        wheels = []
        for wheel in pkg.get("wheels", []):
            url = wheel.get("url", "")
            m = re.search(r"/([^/]+\.whl)$", url)
            if m:
                wheels.append(m.group(1))
        packages.append(
            {"name": pkg["name"], "version": pkg.get("version", ""), "wheels": wheels}
        )
    return packages


# A package is required for a Python minor if its marker admits that minor on
# any of these platforms.
_PLATFORM_ENVIRONMENTS: List[Dict[str, str]] = [
    {"os_name": "posix", "sys_platform": "linux", "platform_system": "Linux"},
    {"os_name": "posix", "sys_platform": "darwin", "platform_system": "Darwin"},
    {"os_name": "nt", "sys_platform": "win32", "platform_system": "Windows"},
]
_PLATFORM_MACHINES: List[str] = ["x86_64", "arm64", "aarch64", "AMD64"]


def parse_constraint_markers() -> Dict[Tuple[str, str], Marker]:
    """Map (name, version) to the environment marker in constraints.txt."""
    markers: Dict[Tuple[str, str], Marker] = {}
    for line in (METADATA_INGESTION_DIR / "constraints.txt").read_text().splitlines():
        m = re.match(r"^([A-Za-z0-9._-]+)==([^\s;]+)\s*;\s*(.+)$", line)
        if m:
            markers[(m.group(1).lower(), m.group(2))] = Marker(m.group(3))
    return markers


def installed_minors(marker: Optional[Marker], minors: Collection[int]) -> Set[int]:
    """Return the subset of minors on which the marker selects the package."""
    if marker is None:
        return set(minors)
    return {
        minor
        for minor in minors
        if any(
            marker.evaluate(
                {
                    **platform_env,
                    "platform_machine": machine,
                    "python_version": f"3.{minor}",
                    "python_full_version": f"3.{minor}.0",
                    "implementation_name": "cpython",
                    "platform_python_implementation": "CPython",
                }
            )
            for platform_env in _PLATFORM_ENVIRONMENTS
            for machine in _PLATFORM_MACHINES
        )
    }


def get_supported_minors(wheels, max_minor):
    """Return (is_pure_python, set of supported 3.X minor versions)."""
    minors: set[int] = set()
    for whl in wheels:
        if "py3-none-any" in whl or "py2.py3-none-any" in whl:
            return True, set()
        # py3-none-<platform>: Python-version-agnostic but platform-specific (e.g. ruff, jdk4py)
        if re.search(r"py3-none-(?!any)", whl):
            return True, set()
        # abi3: cp311-abi3 means 3.11+
        abi3 = re.search(r"cp3(\d+)-abi3", whl)
        if abi3:
            minors.update(range(int(abi3.group(1)), max_minor + 1))
            continue
        for m in re.finditer(r"cp3(\d+)", whl):
            minors.add(int(m.group(1)))
    return False, minors


def main():
    max_minor = max(REQUIRED_PYTHON_MINORS)
    issues = []
    markers = parse_constraint_markers()

    for pkg in parse_uv_lock():
        if pkg["name"] in KNOWN_EXCEPTIONS or not pkg["wheels"]:
            continue
        is_pure, minors = get_supported_minors(pkg["wheels"], max_minor)
        if is_pure:
            continue
        required = installed_minors(
            markers.get((pkg["name"].lower(), pkg["version"])), REQUIRED_PYTHON_MINORS
        )
        missing = required - minors
        if missing:
            missing_s = ", ".join(f"3.{v}" for v in sorted(missing))
            has_s = ", ".join(f"3.{v}" for v in sorted(minors))
            issues.append(
                f"  {pkg['name']}=={pkg['version']}: no wheels for {missing_s} (has: {has_s})"
            )

    if issues:
        print(f"Wheel gaps (checked 3.{min(REQUIRED_PYTHON_MINORS)}-3.{max_minor}):\n")
        print("\n".join(sorted(issues)))
        print(
            "\nFix: widen version constraint, or add to KNOWN_EXCEPTIONS with reason."
        )
        return 1

    print(
        f"Wheel coverage OK for Python 3.{min(REQUIRED_PYTHON_MINORS)}-3.{max_minor}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
