#!/bin/bash

# The following script is used to lint staged files in the datahub-web-react directory before a commit.
# Get a list of staged files in the datahub-web-react directory
STAGED_FILES=$(git diff --cached --name-only | grep -E '^datahub-web-react/.*\.(js|jsx|ts|tsx)$')

if [ -n "$STAGED_FILES" ]; then
  # Navigate to the datahub-web-react directory
  cd datahub-web-react || exit

  # Inform that linting is starting
  echo "🚀 Linting staged files in datahub-web-react before commit..."

  # Run lint-staged if there are any relevant staged files
  npx lint-staged

  # Navigate back to the original directory
  cd - || exit
fi