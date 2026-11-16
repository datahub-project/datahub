#!/bin/bash
# Vercel build command for the docs site (see vercel.json).
#
# Equivalent to `./gradlew -PuseSystemNode=true :docs-website:build`, but tuned
# for Vercel's 8 GB build machine, where that command was intermittently
# OOM-killed (peak ~7.5 GB):
#  - The Gradle daemon is stopped before the Docusaurus build. It keeps ~1.5 GB
#    resident after the docGen/codegen tasks that yarnGenerate depends on.
#  - The Node heap is capped at 3 GB instead of the 10 GB that the yarnBuild task
#    uses. The live heap stays under ~2.5 GB, and with more room V8 grows the heap
#    and the process keeps that memory after GC.
# Together these bring the peak down to ~5.3 GB.

set -euxo pipefail

cd "$(dirname "$0")/.."

./gradlew -PuseSystemNode=true :docs-website:yarnGenerate
./gradlew --stop

cd docs-website
# The yarn that Gradle installed for yarnGenerate (version set in the root build.gradle).
yarn_bins=(.gradle/yarn/yarn-v*/bin/yarn)
if [ "${#yarn_bins[@]}" -ne 1 ] || [ ! -x "${yarn_bins[0]}" ]; then
  echo "Expected exactly one Gradle-installed yarn, found: ${yarn_bins[*]}" >&2
  exit 1
fi
NODE_OPTIONS=--max-old-space-size=3072 "${yarn_bins[0]}" run build
