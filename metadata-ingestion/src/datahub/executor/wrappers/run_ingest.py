# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Python wrapper for DataHub ingestion with secret masking support.

Reads a JSON envelope from stdin containing __recipe_yaml__, __secrets__,
__report_out_file__, __debug_mode__ and __run_options__. Forwards to
`datahub ingest -c -` via stdin. Secrets never touch environment variables or disk.
"""

import sys
from pathlib import Path
from typing import Any

from datahub.executor.execution.wrapper_common import (
    activate_venv,
    build_datahub_stdin,
    check_cli_flag_support,
    read_stdin_envelope,
    register_secrets_for_masking,
    run_datahub_subprocess,
    setup_memory_limit,
    supports_stdin_envelope,
    validate_venv,
)


def run_option_flags(run_options: dict[str, Any], venv_datahub: Path) -> list[str]:
    """`ingest run` flags for the run options, or exit if the CLI lacks one.

    A run that asked for a dry run must never run for real, so a CLI that does
    not know a requested flag fails the run here.
    """
    flags: list[str] = []
    if run_options.get("dry_run"):
        flags.append("--dry-run")
    if run_options.get("preview"):
        flags.extend(
            [
                "--preview",
                "--preview-workunits",
                str(run_options.get("preview_workunits", 10)),
            ]
        )
    for flag in (f for f in flags if f.startswith("--")):
        if not check_cli_flag_support(venv_datahub, flag[2:]):
            print(
                f"Error: this version of datahub does not support {flag}; "
                "refusing to run without it.",
                file=sys.stderr,
            )
            sys.exit(1)
    return flags


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <venv_path>", file=sys.stderr)
        sys.exit(1)

    venv_path = sys.argv[1]

    raw_envelope, envelope = read_stdin_envelope()
    recipe_yaml = envelope["__recipe_yaml__"]
    secrets = envelope.get("__secrets__", {})
    report_out_file = envelope["__report_out_file__"]
    debug_mode = envelope.get("__debug_mode__", "false")
    run_options = envelope.get("__run_options__") or {}

    venv_python, venv_datahub = validate_venv(venv_path)
    activate_venv(venv_path)
    setup_memory_limit()
    register_secrets_for_masking(secrets)

    has_report_to = check_cli_flag_support(venv_datahub, "report-to")
    if has_report_to:
        print(
            "This version of datahub supports report-to functionality", file=sys.stderr
        )
        report_path = Path(report_out_file)
        if report_path.exists():
            report_path.unlink()
    else:
        print(
            "Warning: This version of datahub does not support --report-to",
            file=sys.stderr,
        )

    # Forward the envelope so the CLI registers the secrets for its own
    # masking (report file, logs); older CLIs get the recipe pre-resolved.
    if supports_stdin_envelope(venv_python):
        datahub_stdin = raw_envelope
    else:
        print(
            "Warning: venv CLI predates the stdin secrets envelope; "
            "its own output will not be masked",
            file=sys.stderr,
        )
        datahub_stdin = build_datahub_stdin(recipe_yaml, secrets)

    cmd = [str(venv_datahub)]
    if debug_mode.lower() == "true":
        cmd.append("--debug")
    cmd.extend(["ingest", "run", "-c", "-"])
    cmd.extend(run_option_flags(run_options, venv_datahub))
    if has_report_to:
        cmd.extend(["--report-to", report_out_file])

    sys.exit(run_datahub_subprocess(cmd, datahub_stdin))


if __name__ == "__main__":
    main()
