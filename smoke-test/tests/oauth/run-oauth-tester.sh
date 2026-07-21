#!/bin/bash
# Runs inside the `tester` container (mounted at /smoke). Invoked by run-oauth-ci.sh.
#
# The SDK's CLIENT-SIDE CODE under test comes from the triggering ref, so this
# canary gates client auth changes — env-based DATAHUB_AUTH_TYPE resolution,
# the sink env-merge, precedence semantics — not just the released surface.
# GMS still runs the pinned release images (its OAuth validation is shipped).
#
# Mechanism: a plain `pip install /repo/metadata-ingestion` cannot work here —
# the mount is read-only (setuptools writes egg-info into the source tree) and,
# more fundamentally, the generated datahub/metadata schema classes are
# gitignored and absent from a fresh CI checkout. Instead, install the released
# SDK (which ships the generated classes and all dependencies), then OVERLAY
# the repo's pure-Python source on top — never touching datahub/metadata.
# Limits: a ref that adds dependencies, entry points, or changes model codegen
# is not covered by the overlay; that would need a full codegen build.
#
# Assertions: pytest's exit code (via pipefail) is the pass/fail signal; the
# greps additionally assert that nothing SKIPPED — the tests skip themselves
# when env vars are missing or the SDK lacks the auth layer, and pytest exits 0
# on an all-skipped run, which must not count as green.
set -euo pipefail

# CLI_VERSION is passed in by run-oauth-ci.sh (canonical value from
# gradle/versioning/cliVersion.gradle). The overlay supplies the auth code under
# test, so the base only needs to ship the generated classes + dependencies.
pip install --quiet "acryl-datahub==${CLI_VERSION}" pytest requests
SITE_DATAHUB=$(python -c "import datahub, os; print(os.path.dirname(datahub.__file__))")
tar -C /repo/metadata-ingestion/src/datahub --exclude="./metadata" --exclude="*__pycache__*" -cf - . | tar -C "$SITE_DATAHUB" -xf -
python -c "from datahub.ingestion.auth.env import build_auth_config_from_env"  # overlay sanity check
pytest /smoke/test_oauth_cli_gms.py -v 2>&1 | tee /tmp/pytest-oauth.out
grep -qE "[0-9]+ passed" /tmp/pytest-oauth.out
! grep -qE "[0-9]+ skipped|no tests ran" /tmp/pytest-oauth.out
