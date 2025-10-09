#!/bin/bash
# Wrapper script for pre-commit hooks that auto-fix issues.
# If the command modifies any tracked files, exit with status 1 to signal
# that the commit should be retried after re-staging the modified files.

set -euo pipefail

# Capture the git diff before running the command
BEFORE_DIFF=$(git diff)

# Run the provided command
"$@"

# Capture the git diff after running the command
AFTER_DIFF=$(git diff)

# If the diff changed, files were modified
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

exit 0
