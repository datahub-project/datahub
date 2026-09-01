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
Python wrapper for DataHub test connection with secret masking support.

Reads a JSON envelope from stdin containing __recipe_yaml__, __secrets__,
and __report_out_file__. Forwards to `datahub ingest -c - --test-source-connection`
via stdin. Secrets never touch environment variables or disk.
"""

import json
import sys
from pathlib import Path

from datahub.executor.execution.wrapper_common import (
    FlagSupport,
    activate_venv,
    build_datahub_stdin,
    check_cli_flag_support,
    read_stdin_envelope,
    register_secrets_for_masking,
    run_datahub_subprocess,
    setup_memory_limit,
    validate_venv,
)


def create_probe_failure_report(report_out_file: str) -> None:
    """Create a report for a CLI that could not answer the capability probe."""
    report = {
        "internal_failure": True,
        "internal_failure_reason": (
            "The datahub CLI in this environment failed to run, so the connection was "
            "never tested. This is an environment problem rather than a recipe problem "
            "-- see the execution logs for the CLI's own error."
        ),
    }

    with open(report_out_file, "w") as f:
        json.dump(report, f, indent=2)


def create_unsupported_report(report_out_file: str) -> None:
    """Create a report indicating test-source-connection is not supported."""
    report = {
        "internal_failure": True,
        "internal_failure_reason": (
            "datahub library doesn't have test_connection feature. "
            "You are likely running an old version."
        ),
    }

    with open(report_out_file, "w") as f:
        json.dump(report, f, indent=2)

    print(
        "datahub ingest doesn't seem to have test_connection feature. "
        "You are likely running an old version",
        file=sys.stderr,
    )


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <venv_path>", file=sys.stderr)
        sys.exit(1)

    venv_path = sys.argv[1]

    _raw_envelope, envelope = read_stdin_envelope()
    recipe_yaml = envelope["__recipe_yaml__"]
    secrets = envelope.get("__secrets__", {})
    report_out_file = envelope["__report_out_file__"]

    _venv_python, venv_datahub = validate_venv(venv_path)
    activate_venv(venv_path)
    setup_memory_limit()
    register_secrets_for_masking(secrets)

    support = check_cli_flag_support(venv_datahub, "test-source-connection")
    if support is FlagSupport.PROBE_FAILED:
        create_probe_failure_report(report_out_file)
        sys.exit(1)
    if support is FlagSupport.UNSUPPORTED:
        create_unsupported_report(report_out_file)
        sys.exit(0)

    print(
        "This version of datahub supports test-source-connection functionality",
        file=sys.stderr,
    )
    report_path = Path(report_out_file)
    if report_path.exists():
        report_path.unlink()

    datahub_stdin = build_datahub_stdin(recipe_yaml, secrets)

    cmd = [
        str(venv_datahub),
        "ingest",
        "-c",
        "-",
        "--test-source-connection",
        "--report-to",
        report_out_file,
    ]

    sys.exit(run_datahub_subprocess(cmd, datahub_stdin))


if __name__ == "__main__":
    main()
