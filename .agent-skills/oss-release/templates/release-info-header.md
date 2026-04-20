# Release Info Header (required prepend)

Every release-notes file must start with this header. It's an `oss-release` convention
layered on top of the changelog skill's output (which doesn't know about it). Without
this header:

- The stale-notes detector in `prep` Step 3.5 has nothing to grep against on a future
  re-run (it relies on finding a SHA in the file).
- The GitHub release page lacks the canonical commit/version metadata that every prior
  release carries.

## Template

```markdown
# 🚀 Release Changelog: acryl-datahub <NEXT_VERSION> — <YYYY-MM-DD>

Release candidate build of the `acryl-datahub` Python package and ingestion connectors.

## 📌 Release Info

- **Version**: `<NEXT_VERSION>`
- **Base**: `<LATEST_STABLE>`
- **Commit**: [`<SHA_SHORT>`](https://github.com/acryldata/datahub/commit/<SHA_FULL>)

[ ... rest of changelog content from the changelog skill goes here ... ]
```

## Trailing installation block (also conventional)

Append at the end of the notes:

````markdown
## 📦 Installation

```bash
pip install acryl-datahub==<VERSION_NO_V_PREFIX>
```
````

## Substitutions

| Placeholder             | Source                                                          |
| ----------------------- | --------------------------------------------------------------- |
| `<NEXT_VERSION>`        | Computed in `prep` Step 3 (e.g. `v1.5.0.13rc1`)                 |
| `<LATEST_STABLE>`       | The value used in the preflight summary (e.g. `v1.5.0.12`)      |
| `<SHA_SHORT>`           | `git rev-parse --short=10 HEAD`                                 |
| `<SHA_FULL>`            | `git rev-parse HEAD`                                            |
| `<YYYY-MM-DD>`          | Today's date                                                    |
| `<VERSION_NO_V_PREFIX>` | `<NEXT_VERSION>` with leading `v` stripped (e.g. `1.5.0.13rc1`) |

For `finish` Step 6 (stable), the same template applies but the changelog body is
reused as-is from the RC's release notes (since the stable tags the same commit).
