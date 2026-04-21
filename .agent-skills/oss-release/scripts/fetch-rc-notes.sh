#!/usr/bin/env bash
# fetch-rc-notes.sh <RC_TAG> <STABLE_VERSION>
#
# Fetches the RC's release body from GitHub, rewrites every occurrence of
# the RC version to the stable version, and writes the result to the stable
# release's notes file. Prints the notes file path on stdout.
#
# Stable tags the same commit as the RC, so reusing the RC's changelog body
# is the default path. BUT the body contains the RC tag/version in several
# places (Release Info header, title, install command, sometimes body links)
# — without substitution the stable release would ship notes that say
# `v1.5.0.14rc1` throughout. This script rewrites them in-place before
# saving so the file is ready to feed `cut-release.sh`.
#
# Operators can still Read + edit the saved file before invoking
# cut-release.sh if they want to regenerate via the changelog skill.
#
# Usage:
#   .agent-skills/oss-release/scripts/fetch-rc-notes.sh v1.5.0.13rc2 v1.5.0.13
#
# Output:
#   stdout : notes file path (.agent-skills/oss-release/notes/release-notes-<stable>.md)
#   stderr : errors + a one-line substitution summary
#
# Exit codes:
#   0 — notes saved, non-empty
#   1 — usage error, gh failure, or empty release body

set -uo pipefail

RC_TAG="${1:-}"
STABLE_VERSION="${2:-}"

if [ -z "$RC_TAG" ] || [ -z "$STABLE_VERSION" ]; then
    echo "Usage: $0 <rc-tag> <stable-version>" >&2
    exit 1
fi

# Normalize: always tag with leading v, version without
RC_TAG="v${RC_TAG#v}"
STABLE_TAG="v${STABLE_VERSION#v}"
RC_VERSION_NO_V="${RC_TAG#v}"
STABLE_VERSION_NO_V="${STABLE_TAG#v}"

NOTES_DIR=".agent-skills/oss-release/notes"
if ! mkdir -p "$NOTES_DIR"; then
    echo "ERROR: could not create $NOTES_DIR" >&2
    exit 1
fi

NOTES_FILE="${NOTES_DIR}/release-notes-${STABLE_TAG}.md"

RC_BODY=$(gh release view "$RC_TAG" --repo acryldata/datahub --json body --jq '.body' 2>/dev/null || echo "")
if [ -z "$RC_BODY" ]; then
    echo "ERROR: could not fetch RC release notes for ${RC_TAG}" >&2
    echo "       Check 'gh release view ${RC_TAG} --repo acryldata/datahub' by hand." >&2
    exit 1
fi

# Rewrite RC version references → stable. Order matters: replace `v<ver>` first
# so `<ver>` inside it becomes `<stable_ver>` as part of the replacement, then
# catch remaining bare-version occurrences (e.g. `pip install acryl-datahub==<ver>`).
export RC_BODY RC_TAG STABLE_TAG RC_VERSION_NO_V STABLE_VERSION_NO_V NOTES_FILE

python3 <<'PYEOF'
import os, sys
body       = os.environ["RC_BODY"]
rc_tag     = os.environ["RC_TAG"]
stable_tag = os.environ["STABLE_TAG"]
rc_ver     = os.environ["RC_VERSION_NO_V"]
stable_ver = os.environ["STABLE_VERSION_NO_V"]

# Count before substitution. `rc_ver` is a substring of `rc_tag` (rc_tag = 'v' + rc_ver),
# so every rc_tag occurrence contains a rc_ver occurrence. Subtracting gives the count
# of "bare" rc_ver strings not preceded by 'v'.
count_tag      = body.count(rc_tag)
count_bare_ver = body.count(rc_ver) - count_tag

body = body.replace(rc_tag, stable_tag)
body = body.replace(rc_ver, stable_ver)

# Rewrite the standard template's "Release candidate build of..." opener —
# wrong tagline for a stable release. Match on the full template sentence so
# we don't accidentally touch user-authored prose that happens to say
# "Release candidate". If the template changes, this is a no-op.
rc_tagline = "Release candidate build of the `acryl-datahub` Python package and ingestion connectors."
stable_tagline = "Stable release of the `acryl-datahub` Python package and ingestion connectors."
tagline_count = body.count(rc_tagline)
body = body.replace(rc_tagline, stable_tagline)

sys.stderr.write(
    f"  Rewrote {count_tag} × {rc_tag} → {stable_tag}"
    f", {count_bare_ver} bare × {rc_ver} → {stable_ver}"
    f", {tagline_count} × 'Release candidate build' tagline\n"
)

with open(os.environ["NOTES_FILE"], "w") as f:
    f.write(body)
PYEOF

if [ ! -s "$NOTES_FILE" ]; then
    echo "ERROR: resulting notes file is empty" >&2
    rm -f "$NOTES_FILE"
    exit 1
fi

echo "$NOTES_FILE"
exit 0
