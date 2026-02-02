#!/usr/bin/env python3
"""
Remove test/mock private keys and certificates from Python packages.

This script scans Python site-packages directories for private key files
that are shipped as test fixtures by various packages (e.g., tornado, moto).
These files trigger security scanner alerts but are not needed at runtime.

Usage:
    python remove_package_keys.py [--dry-run] [--verbose] [paths...]

If no paths are provided, scans default locations:
    - /home/nonroot/.venv (distroless)
    - /home/datahub/.venv (wolfi/ubuntu)
    - /opt/datahub/venvs/* (bundled venvs)
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Generator

# Known test/mock key patterns to remove
# Format: (package_subpath_pattern, description)
KNOWN_KEY_PATTERNS = [
    # Tornado test keys
    ("tornado/test/test.key", "Tornado test SSL key"),
    ("tornado/test/test.crt", "Tornado test SSL cert"),
    # Moto mock proxy keys (AWS mock library)
    ("moto/moto_proxy/ca.key", "Moto proxy CA key"),
    ("moto/moto_proxy/cert.key", "Moto proxy cert key"),
    ("moto/moto_proxy/ca.crt", "Moto proxy CA cert"),
    ("moto/moto_proxy/cert.crt", "Moto proxy cert"),
    # httpbin (sometimes bundled)
    ("httpbin/certs/server.key", "httpbin test key"),
    # trustme (test CA library)
    ("trustme/*.key", "trustme test keys"),
    # aiohttp test certs
    ("aiohttp/test_*.key", "aiohttp test keys"),
    ("aiohttp/test_*.pem", "aiohttp test certs"),
    # requests/urllib3 test certs
    ("urllib3/contrib/_securetransport/*.key", "urllib3 test keys"),
    # General patterns for test directories
    ("*/test/*.key", "Test directory keys"),
    ("*/tests/*.key", "Tests directory keys"),
    ("*/testing/*.key", "Testing directory keys"),
]

# File extensions that are likely private keys
KEY_EXTENSIONS = {".key", ".pem"}

# Regex to detect private key content
PRIVATE_KEY_MARKERS = [
    rb"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----",
    rb"-----BEGIN ENCRYPTED PRIVATE KEY-----",
]


def is_private_key_file(filepath: Path) -> bool:
    """Check if a file contains private key material."""
    if filepath.suffix not in KEY_EXTENSIONS:
        return False

    try:
        # Read first 1KB to check for key markers
        content = filepath.read_bytes()[:1024]
        for marker in PRIVATE_KEY_MARKERS:
            if re.search(marker, content):
                return True
    except (OSError, PermissionError):
        pass

    return False


def find_site_packages(base_path: Path) -> Generator[Path, None, None]:
    """Find all site-packages directories under a base path."""
    for root, dirs, _ in os.walk(base_path):
        root_path = Path(root)
        if root_path.name == "site-packages":
            yield root_path
        # Prune search - don't descend into site-packages
        if "site-packages" in dirs:
            dirs[:] = ["site-packages"]


def find_keys_by_pattern(site_packages: Path, verbose: bool = False) -> Generator[Path, None, None]:
    """Find files matching known key patterns."""
    for pattern, description in KNOWN_KEY_PATTERNS:
        # Handle glob patterns
        if "*" in pattern:
            for match in site_packages.glob(pattern):
                if match.is_file():
                    if verbose:
                        print(f"  Pattern match ({description}): {match}")
                    yield match
        else:
            # Exact path
            key_path = site_packages / pattern
            if key_path.is_file():
                if verbose:
                    print(f"  Pattern match ({description}): {key_path}")
                yield key_path


def scan_for_private_keys(site_packages: Path, verbose: bool = False) -> Generator[Path, None, None]:
    """Scan site-packages for any private key files."""
    for root, _, files in os.walk(site_packages):
        root_path = Path(root)
        for filename in files:
            if any(filename.endswith(ext) for ext in KEY_EXTENSIONS):
                filepath = root_path / filename
                if is_private_key_file(filepath):
                    if verbose:
                        print(f"  Private key detected: {filepath}")
                    yield filepath


def get_default_paths() -> list[Path]:
    """Get default paths to scan based on environment."""
    paths = []

    # Distroless user home
    distroless_venv = Path("/home/nonroot/.venv")
    if distroless_venv.exists():
        paths.append(distroless_venv)

    # Ubuntu/Wolfi user home
    datahub_venv = Path("/home/datahub/.venv")
    if datahub_venv.exists():
        paths.append(datahub_venv)

    # Bundled venvs
    bundled_base = Path("/opt/datahub/venvs")
    if bundled_base.exists():
        for venv_dir in bundled_base.iterdir():
            if venv_dir.is_dir():
                paths.append(venv_dir)

    return paths


def remove_keys(paths: list[Path], dry_run: bool = False, verbose: bool = False) -> tuple[int, int]:
    """
    Remove private keys from the given paths.

    Returns:
        Tuple of (files_found, files_removed)
    """
    files_found = 0
    files_removed = 0

    for base_path in paths:
        if not base_path.exists():
            if verbose:
                print(f"Skipping non-existent path: {base_path}")
            continue

        print(f"Scanning: {base_path}")

        for site_packages in find_site_packages(base_path):
            if verbose:
                print(f"  Found site-packages: {site_packages}")

            # Collect all keys to remove (dedup with set)
            keys_to_remove: set[Path] = set()

            # Find by known patterns
            for key_path in find_keys_by_pattern(site_packages, verbose):
                keys_to_remove.add(key_path)

            # Scan for any other private keys
            for key_path in scan_for_private_keys(site_packages, verbose):
                keys_to_remove.add(key_path)

            # Remove the keys
            for key_path in sorted(keys_to_remove):
                files_found += 1
                action = "Would remove" if dry_run else "Removing"
                print(f"  {action}: {key_path}")

                if not dry_run:
                    try:
                        key_path.unlink()
                        files_removed += 1
                    except OSError as e:
                        print(f"    Error: {e}", file=sys.stderr)

    return files_found, files_removed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Remove test/mock private keys from Python packages"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be removed without actually removing"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed scan progress"
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Paths to scan (default: standard venv locations)"
    )

    args = parser.parse_args()

    paths = args.paths if args.paths else get_default_paths()

    if not paths:
        print("No paths to scan (none provided and no default paths exist)")
        return 0

    print("=" * 60)
    print("Removing test/mock private keys from Python packages")
    print("=" * 60)

    if args.dry_run:
        print("DRY RUN - no files will be removed\n")

    files_found, files_removed = remove_keys(paths, args.dry_run, args.verbose)

    print()
    print("=" * 60)
    print(f"Summary: Found {files_found} key file(s)")
    if args.dry_run:
        print(f"         Would remove {files_found} file(s)")
    else:
        print(f"         Removed {files_removed} file(s)")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
