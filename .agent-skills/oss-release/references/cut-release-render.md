# Cut-Release Output Rendering

After `cut-release.sh` runs (real or dry), re-render its raw output as a readable
Markdown block in your own message. The raw script output is not scannable on its own.

**Rules:**
- Always render — both dry-run and real-run.
- Don't re-run the script — reformat the output you already have.
- Don't summarize or drop sections — keep the rendering faithful.

## Common skeleton

```markdown
### <HEADER>

**Mode:** <MODE_LINE>

**Release parameters**

| Field | Value |
|---|---|
| Repo | `acryldata/datahub` |
| Version | `v<VERSION>` |
| Commit | `<SHA>` |
| Prior tag (for range) | `<PRIOR_TAG>` |
| Notes file | `<NOTES_FILE>` |
| Pre-release | `true` (or `false` for stable) |

**<COMMANDS_LABEL>**

```bash
# [1/3] Tag + push
git tag -a "v<VERSION>" "<SHA>" -m "Release v<VERSION>"
git push origin "v<VERSION>"

# [2/3] Publish GitHub release
gh release create v<VERSION> \
    --repo acryldata/datahub \
    --title v<VERSION> \
    --notes-file <NOTES_FILE> \
    --prerelease   # omit for stable
```

**Commit range** — `<PRIOR_TAG>..<SHA>` (N non-merge commits)

| SHA | Title |
|---|---|
| ... | ... |

### 📄 Notes Body Preview

> [wrap each line of the notes body in `> ` so it renders as a blockquote;
> do NOT alter the content of the notes]

**Status:** <STATUS_LINE>
<RELEASE_URL_LINE>   # only for real runs
```

## Placeholder fills per mode

| Placeholder | Dry-run | Real run |
|---|---|---|
| `<HEADER>` | `🧪 Dry-Run: \`v<VERSION>\`` | `🚀 Release Cut: \`v<VERSION>\`` |
| `<MODE_LINE>` | `DRY-RUN — no tags created, no releases published.` | `LIVE — tag pushed and release published.` |
| `<COMMANDS_LABEL>` | `Commands that WOULD run` (suppressed) | `Commands executed` |
| `<STATUS_LINE>` | `` `[3/3] Dry-run complete. Nothing was changed.` ✅ `` | `` `[3/3] Release published.` ✅ `` |
| `<RELEASE_URL_LINE>` | *(omit)* | `**Release URL:** <https://github.com/acryldata/datahub/releases/tag/v<VERSION>>` |

## Stable releases (no `--prerelease`)

For `finish` Step 6 (cutting stable):
- Omit `--prerelease` in the rendered commands code block
- Set `Pre-release` row in the parameters table to `false`
- Everything else identical

On a real run, also paste the release URL as a plain link at the end of your message
so it's copy-pastable from any rendering.
