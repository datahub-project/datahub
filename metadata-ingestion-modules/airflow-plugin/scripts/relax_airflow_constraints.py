#!/usr/bin/env python3
"""Rewrite Airflow constraint files so acryl-datahub's CVE floors can resolve.

Apache Airflow pins ``anyio`` and ``click`` below the versions published in
``acryl-datahub`` (``anyio>=4.14.2``, ``click>=8.3.3``). Pip intersects every
``-c`` file, so those pins make the plugin uninstallable. This copies each
constraint file and relaxes only those two pins.
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.request
from pathlib import Path

_REPLACEMENTS = (
    (re.compile(r"^anyio==.*$", re.MULTILINE), "anyio>=4.14.2"),
    (re.compile(r"^click==.*$", re.MULTILINE), "click>=8.3.3"),
)


def _read_constraint(source: str) -> str:
    if source.startswith("https://") or source.startswith("http://"):
        # The caller passes the Apache Airflow constraints URL from CI.
        with urllib.request.urlopen(source) as response:
            return response.read().decode()
    return Path(source).read_text()


def relax(text: str) -> str:
    for pattern, replacement in _REPLACEMENTS:
        text = pattern.sub(replacement, text)
    return text


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--constraint",
        action="append",
        default=[],
        help="Constraint file path or URL. Repeatable. Leading -c from Gradle is fine.",
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not args.constraint:
        sys.exit("at least one --constraint is required")
    combined = "\n".join(relax(_read_constraint(source)) for source in args.constraint)
    Path(args.output).write_text(combined + "\n")


if __name__ == "__main__":
    main()
