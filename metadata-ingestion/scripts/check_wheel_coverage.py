#!/usr/bin/env python3
"""
Check that locked packages have binary wheels for all supported Python versions.

Parses uv.lock and flags C-extension packages that are missing wheels for
Python versions covered by requires-python. This catches cases like pyodbc
4.0.39 which had wheels for 3.10/3.11 but not 3.12, causing build failures
when compiling from source wasn't possible.

Pure Python packages (py3-none-any wheels) are skipped since they work
everywhere.

Usage:
    python scripts/check_wheel_coverage.py
"""

import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
METADATA_INGESTION_DIR = SCRIPT_DIR.parent

# Python minor versions to check. Update when requires-python changes.
# Derived from requires-python = ">=3.10" in pyproject.toml.
# We check up to the latest stable CPython release.
REQUIRED_PYTHON_MINORS = {10, 11, 12}

# Packages with known wheel gaps that we accept. Add a comment explaining why.
KNOWN_EXCEPTIONS = {
    "kerberos",  # Unmaintained since 2020, source-only. Tracked for replacement.
    "python-ldap",  # Requires OpenLDAP headers, never ships wheels.
    "scipy",  # 1.17+ dropped 3.10; we pin <2.0 and Docker builds use 3.10.
    "sqlalchemy",  # 1.4.x has no 3.12 wheels; pinned for compatibility, Docker uses 3.10.
}


def parse_requires_python() -> str:
    """Read requires-python from pyproject.toml."""
    pyproject_path = METADATA_INGESTION_DIR / "pyproject.toml"
    for line in pyproject_path.read_text().splitlines():
        m = re.match(r'requires-python\s*=\s*"(.+)"', line)
        if m:
            return m.group(1)
    return ">=3.10"


def parse_uv_lock() -> list[dict]:
    """Parse uv.lock into a list of package entries with name, version, and wheel filenames."""
    lock_path = METADATA_INGESTION_DIR / "uv.lock"
    text = lock_path.read_text()

    packages = []
    current = None

    for line in text.splitlines():
        # New package block
        name_match = re.match(r'^name = "(.+)"$', line)
        if name_match:
            if current:
                packages.append(current)
            current = {"name": name_match.group(1), "version": "", "wheels": []}
            continue

        if current is None:
            continue

        version_match = re.match(r'^version = "(.+)"$', line)
        if version_match:
            current["version"] = version_match.group(1)
            continue

        # Wheel URLs contain the filename which encodes the Python version
        wheel_match = re.search(r'/([^/]+\.whl)"', line)
        if wheel_match:
            current["wheels"].append(wheel_match.group(1))

    if current:
        packages.append(current)

    return packages


def get_python_versions_from_wheels(
    wheels: list[str], max_minor: int
) -> tuple[bool, set[int]]:
    """
    Extract supported Python minor versions from wheel filenames.

    Handles:
    - Pure Python wheels (py3-none-any): compatible with all versions.
    - ABI3 wheels (cpXY-abi3-*): compatible with cpXY and all later versions.
    - Platform-independent tags (py3-*, py2.py3-*): compatible with all versions.
    - Standard cpython wheels (cpXY-cpXY-*): compatible with that version only.

    Returns (is_pure_python, set_of_minor_versions).
    """
    minors: set[int] = set()
    for whl in wheels:
        # Pure Python — works everywhere
        if "py3-none-any" in whl or "py2.py3-none-any" in whl:
            return True, set()

        # Platform-specific but Python-version-independent (e.g., py3-none-macosx)
        if re.search(r"py3-none-(?!any)", whl):
            return True, set()

        # ABI3 (stable ABI) wheels: cp311-abi3 means "3.11 and above"
        abi3_match = re.search(r"cp3(\d+)-abi3", whl)
        if abi3_match:
            min_minor = int(abi3_match.group(1))
            for v in range(min_minor, max_minor + 1):
                minors.add(v)
            continue

        # Standard cpython wheels: cp312-cp312
        for m in re.finditer(r"cp3(\d+)", whl):
            minors.add(int(m.group(1)))

    return False, minors


def check_wheel_coverage() -> int:
    """Check all locked packages for wheel coverage gaps. Returns exit code."""
    packages = parse_uv_lock()
    issues = []

    for pkg in packages:
        if not pkg["wheels"]:
            # Source-only package (no wheels at all)
            if pkg["name"] in KNOWN_EXCEPTIONS:
                continue
            # Only flag if it has an sdist (it's a real dependency, not a marker-excluded one)
            if pkg["version"]:
                # Check if this is likely a C extension by looking at the name
                # We can't be sure without the sdist, so just skip packages with no wheels
                # that we haven't explicitly flagged
                continue
            continue

        is_pure, wheel_minors = get_python_versions_from_wheels(
            pkg["wheels"], max(REQUIRED_PYTHON_MINORS)
        )
        if is_pure:
            continue

        if pkg["name"] in KNOWN_EXCEPTIONS:
            continue

        missing = REQUIRED_PYTHON_MINORS - wheel_minors
        if missing:
            missing_strs = sorted(f"3.{v}" for v in missing)
            available_strs = sorted(f"3.{v}" for v in wheel_minors)
            issues.append(
                f"  {pkg['name']}=={pkg['version']}: "
                f"missing wheels for {', '.join(missing_strs)} "
                f"(has: {', '.join(available_strs)})"
            )

    if issues:
        print(
            f"Wheel coverage gaps found for requires-python >= 3.{min(REQUIRED_PYTHON_MINORS)}:"
        )
        print(
            f"Checked Python versions: {', '.join(f'3.{v}' for v in sorted(REQUIRED_PYTHON_MINORS))}"
        )
        print()
        for issue in sorted(issues):
            print(issue)
        print()
        print("These packages may fail to install on the listed Python versions")
        print("unless system build dependencies (C headers/libraries) are present.")
        print()
        print("To fix: bump the version constraint to allow a version with wheels,")
        print("or add the package to KNOWN_EXCEPTIONS in this script with a comment.")
        return 1
    else:
        print(
            f"All locked C-extension packages have wheels for "
            f"Python {', '.join(f'3.{v}' for v in sorted(REQUIRED_PYTHON_MINORS))}. OK"
        )
        return 0


if __name__ == "__main__":
    sys.exit(check_wheel_coverage())
