#!/bin/bash

# The following script is used to lint staged files in the datahub-web-react directory before a commit.
# Exit on error, undefined variables, and pipeline failures
set -euo pipefail

# Get a list of staged files in the datahub-web-react directory
STAGED_FILES=$(git diff --cached --name-only | grep -E '^datahub-web-react/.*\.(js|jsx|ts|tsx)$')

if [ -n "$STAGED_FILES" ]; then
  # Capture the git diff before running lint-staged
  BEFORE_DIFF=$(git diff)

  # Navigate to the datahub-web-react directory
  cd datahub-web-react || exit

  # Inform that linting is starting
  echo "🚀 Linting staged files in datahub-web-react before commit..."

  # Run lint-staged if there are any relevant staged files
  npx lint-staged

  # Navigate back to the original directory (suppress output to keep logs clean)
  cd - > /dev/null || exit

  # Capture the git diff after running lint-staged
  AFTER_DIFF=$(git diff)

  # If the diff changed, files were modified by the linter
  if [ "$BEFORE_DIFF" != "$AFTER_DIFF" ]; then
    # Write message to stderr so it appears after all hook output
    echo "" >&2
    echo "========================================" >&2
    echo "⚠️  FILES MODIFIED BY PRE-COMMIT HOOK" >&2
    echo "========================================" >&2
    echo "Please stage the changes and re-run the commit:" >&2
    echo "  git add <modified-files>" >&2
    echo "  git commit" >&2
    echo "========================================" >&2
    echo "" >&2
    exit 1
  fi
fi

exit 0