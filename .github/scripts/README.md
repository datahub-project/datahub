# Pre-commit Hook Scripts

This directory contains scripts for managing pre-commit hooks in the DataHub repository.

## Scripts

### `generate_pre_commit.py`

Auto-generates `.pre-commit-config.yaml` based on detected Java and Python projects in the repository.

**Usage:**

```bash
python3 .github/scripts/generate_pre_commit.py
```

This script is run automatically by CI and should be run whenever new modules are added.

### `pre-commit-wrapper.sh`

Wrapper script that ensures pre-commit hooks fail when they modify files (e.g., from auto-formatting).

**Behavior:**

- Captures git diff before running the formatting/linting command
- Runs the command (spotlessApply, lintFix, etc.)
- If files were modified, exits with status 1 and displays a warning
- This ensures you re-stage modified files before committing

### `enable-quiet-pre-commit.sh`

Optional script to enable quiet mode for pre-commit hooks, which filters out "(no files to check)Skipped" messages.

**Usage:**

```bash
.github/scripts/enable-quiet-pre-commit.sh
```

**Benefits:**

- Cleaner commit output
- Warning messages are more visible
- Only shows hooks that actually ran

**Note:** This modifies `.git/hooks/pre-commit` (local only, not committed). Each developer can choose to enable this individually.

### `web_react_lint.sh`

Custom linting script for datahub-web-react that:

- Runs lint-staged on TypeScript/JavaScript files
- Fails if files are modified by the linter
- Shows clear warning messages

## Workflow

When you commit changes:

1. **Pre-commit hooks run automatically**
2. **If hooks modify files (formatting fixes):**
   - Commit fails with exit code 1
   - You see a clear warning message
3. **Stage the modified files:**
   ```bash
   git add <modified-files>
   ```
4. **Re-run the commit:**
   ```bash
   git commit
   ```

This ensures all formatting fixes are included in your commit.

## Enabling Quiet Mode (Recommended)

To reduce noise in pre-commit output:

```bash
# Run once per clone
.github/scripts/enable-quiet-pre-commit.sh
```

This is optional but recommended for a cleaner developer experience.
