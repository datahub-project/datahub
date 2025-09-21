# OSS Diff Tool - Claude Notes

This document provides insights for Claude Code when working with the oss-diff tool.

## How OSS Diff Works

The oss-diff tool validates differences between OSS DataHub and the SaaS fork:

1. **Rules** (`oss-diff-rules.yml`) define what counts as a "diff" and set base limits
2. **Exceptions** (`oss-diff-exceptions.json`) provide file-specific overrides with exact numeric limits
3. **Line Filtering** (new feature) allows rules to specify regex patterns to ignore certain types of lines

## GitHub Workflow Behavior

The `oss-diff-checker.yml` workflow has conditional logic based on whether oss-diff files are modified:

### When NO oss-diff files changed:

- Automatically runs `--loosen` to update stale exceptions
- Usually results in clean passes

### When oss-diff files ARE changed (like code modifications):

- **Skips automatic `--loosen`** step
- Tests against current (potentially stale) exception limits
- Often results in many violations that need manual resolution

## Resolving "Worse Results" After Changes

If you modify oss-diff files and suddenly see many more violations (e.g., 1 → 132), this is **expected behavior**:

1. The workflow intentionally skips auto-loosening when oss-diff code changes
2. Exception limits become stale as OSS/SaaS diverge over time
3. **Solution**: Run `./oss-diff.py check --loosen` to update all exception limits
4. Commit the updated `oss-diff-exceptions.json` file

## Line Filtering Feature

Rules can now include `ignored_line_patterns` to exclude certain lines from diff calculations:

```yaml
- pattern: "docs-website/sidebars.js"
  max_removals: 0
  max_total: 10
  ignored_line_patterns:
    - "^\\s*//.*$" # JavaScript comment lines
```

This allows selective filtering while maintaining strict limits for meaningful changes.

## Best Practices

1. Always run `--loosen` after making oss-diff code changes
2. Test with the correct comparison: `upstream/master...HEAD` (not `acryl-main...HEAD`)
3. Use line filtering sparingly and only for truly ignorable content like comments
4. Commit exception updates as part of oss-diff enhancement PRs
