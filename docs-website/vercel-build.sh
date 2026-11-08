#!/bin/bash
# Vercel build command for the docs site (see vercel.json). Run from the repo root.
#
# Equivalent to `./gradlew -PuseSystemNode=true :docs-website:build`, except that
# the Gradle daemon is stopped before the Docusaurus build. The daemon keeps
# ~1.5 GB resident after docGen/codegen, and Docusaurus alone peaks at ~5.5 GB,
# which together came within a few hundred MB of the 8 GB build machine and
# caused intermittent OOM kills.

set -euxo pipefail

./gradlew -PuseSystemNode=true :docs-website:yarnGenerate
./gradlew --stop

cd docs-website
# The yarn that Gradle installed for yarnGenerate (version set in the root build.gradle).
yarn_bins=(.gradle/yarn/yarn-v*/bin/yarn)
if [ "${#yarn_bins[@]}" -ne 1 ] || [ ! -x "${yarn_bins[0]}" ]; then
  echo "Expected exactly one Gradle-installed yarn, found: ${yarn_bins[*]}" >&2
  exit 1
fi
# Same heap cap as the yarnBuild task in build.gradle.
NODE_OPTIONS=--max-old-space-size=3072 "${yarn_bins[0]}" run build
