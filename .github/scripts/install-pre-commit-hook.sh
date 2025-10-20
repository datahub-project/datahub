#!/bin/bash
# Custom pre-commit hook installer that filters out "Skipped" messages

set -euo pipefail

# First, install the standard pre-commit hooks
pre-commit install

# Now, create a wrapper that filters output
cat > .git/hooks/pre-commit-with-filter << 'EOF'
#!/usr/bin/env bash
# Wrapper script that filters pre-commit output

# Run the actual pre-commit hook and filter skipped messages
python3 -mpre_commit hook-impl \
    --config=.pre-commit-config.yaml \
    --hook-type=pre-commit \
    --hook-dir "$(dirname "$0")" \
    -- "$@" 2>&1 | grep -v "(no files to check)Skipped" || true

# Preserve the exit code from pre-commit
exit ${PIPESTATUS[0]}
EOF

# Make the wrapper executable
chmod +x .git/hooks/pre-commit-with-filter

echo "Pre-commit hook installed with output filtering"
echo "Skipped hooks will not be displayed in output"
