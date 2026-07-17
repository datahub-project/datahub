#!/usr/bin/env bash
# Single pre-commit entrypoint for Playwright E2E TS files.
#
# Replaces the previous two-hook setup (playwright-e2e-lint-fix +
# playwright-e2e-prettier-write), which each paid for a separate Gradle
# configure phase on the same staged .ts files. Running both tasks in one
# Gradle invocation shares a single configure phase and a single yarnInstall.
set -euo pipefail

exec ./gradlew \
  :e2e-test:ui:playwright:lintFix \
  :e2e-test:ui:playwright:prettierWrite \
  -x generateGitPropertiesGlobal
