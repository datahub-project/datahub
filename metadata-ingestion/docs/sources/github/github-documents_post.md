### Capabilities

#### Context documents under a parent folder

```yaml
source:
  type: github-documents
  config:
    github_token: "${GITHUB_TOKEN}"
    repository: acme/handbook
    branch: main
    path_prefix: docs
    parent_document_urn: "urn:li:document:context-handbook"
    document_import_mode: NATIVE
    show_in_global_context: true
    stateful_ingestion:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

#### Read-only external references

```yaml
source:
  type: github-documents
  config:
    github_token: "${GITHUB_TOKEN}"
    repository: https://github.com/acme/handbook
    document_import_mode: EXTERNAL
    stateful_ingestion:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

#### Plain-body serialization

GitHub files are imported as **plain markdown or text**. Document metadata (title, tags, ownership, etc.) lives in DataHub aspects and `customProperties` — not in YAML frontmatter or other file headers.

Each imported file document stores these `customProperties` keys:

| Key                       | Purpose                                     |
| ------------------------- | ------------------------------------------- |
| `import_source_id`        | Stable external key for upserts             |
| `content_hash`            | SHA-256 of raw file body (change detection) |
| `extraction_algo_version` | Bump when hash algorithm changes            |
| `github_blob_sha`         | Git blob SHA at last import                 |
| `github_commit_sha`       | Branch HEAD at last import                  |

Cloud sync-back may additionally write `last_exported_content_hash` to prevent re-import loops after export.

#### Stateful ingestion

Enable `stateful_ingestion.enabled: true` (recommended for scheduled syncs) to soft-delete documents that disappear from the GitHub tree between runs. Unchanged file bodies are skipped when a graph connection is available and the stored `content_hash` matches.

### Limitations

- Maximum file size is 1 MB per file (larger files are skipped).
- Requires network access from the ingestion executor to `api.github.com`.
- Binary formats are not parsed; use text/markdown files.
- GitHub may truncate recursive tree listings for very large repositories; narrow `path_prefix` or split across sources.
- Imports are capped by `max_files` (default 500) per run.

### Troubleshooting

- **401 / 403 from GitHub**: Verify the token, repository name, and permissions.
- **Branch not found**: Confirm the branch exists and is spelled correctly.
- **No files imported**: Check `path_prefix` and `file_extensions` filters.
- **Documents not removed after GitHub delete**: Ensure `stateful_ingestion.enabled: true` and the pipeline has a graph connection for checkpoint storage.
- **Partial import on large repos**: Check ingestion report warnings for `github-tree-truncated` or `github-files-truncated`.
