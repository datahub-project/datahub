#!/usr/bin/env python3
"""
Flag locked C-extension packages missing wheels for supported Python versions.

Parses uv.lock wheel filenames. Handles abi3, py3-none-any, and standard cpXY tags.
Exits non-zero if gaps are found (unless the package is in KNOWN_EXCEPTIONS).
"""

import re
import sys
from pathlib import Path

import toml

METADATA_INGESTION_DIR = Path(__file__).resolve().parent.parent

# Update when requires-python or Docker base image changes.
REQUIRED_PYTHON_MINORS = {10, 11, 12}

KNOWN_EXCEPTIONS = {
    "kerberos",  # Unmaintained, source-only. Needs replacement with krb5/gssapi.
    "python-ldap",  # Source-only, requires OpenLDAP headers.
    "scipy",  # 1.17+ dropped 3.10; Docker uses 3.10.
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


def get_supported_minors(wheels, max_minor):
    """Return (is_pure_python, set of supported 3.X minor versions)."""
    minors = set()
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

    for pkg in parse_uv_lock():
        if pkg["name"] in KNOWN_EXCEPTIONS or not pkg["wheels"]:
            continue
        is_pure, minors = get_supported_minors(pkg["wheels"], max_minor)
        if is_pure:
            continue
        missing = REQUIRED_PYTHON_MINORS - minors
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
