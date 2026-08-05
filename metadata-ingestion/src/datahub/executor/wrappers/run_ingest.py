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
__report_out_file__, and __debug_mode__. Forwards to `datahub ingest -c -` via stdin.
Secrets never touch environment variables or disk.
"""

import sys
from pathlib import Path

from datahub.executor.execution.wrapper_common import (
    activate_venv,
    build_datahub_stdin,
    check_cli_flag_support,
    read_stdin_envelope,
    register_secrets_for_masking,
    run_datahub_subprocess,
    setup_memory_limit,
    validate_venv,
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
    debug_mode = envelope.get("__debug_mode__", "false")

    _venv_python, venv_datahub = validate_venv(venv_path)
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

    datahub_stdin = build_datahub_stdin(recipe_yaml, secrets)

    cmd = [str(venv_datahub)]
    if debug_mode.lower() == "true":
        cmd.append("--debug")
    cmd.extend(["ingest", "run", "-c", "-"])
    if has_report_to:
        cmd.extend(["--report-to", report_out_file])

    sys.exit(run_datahub_subprocess(cmd, datahub_stdin))


if __name__ == "__main__":
    main()
