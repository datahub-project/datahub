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

Files are imported as **plain markdown or text**, except OKF concepts (below). Other document metadata lives in DataHub aspects and `customProperties`.

#### OKF frontmatter

A markdown file whose YAML frontmatter has a `type` key is an OKF concept, for example:

```markdown
---
id: guide:mau
type: Guide
title: Monthly active users
owners: [alice@example.com]
tags: [metrics]
glossary_terms: [Active User]
audience: analysts
---

# Monthly active users
```

For these files the source:

- imports the markdown after the frontmatter block as the document body;
- stores the block in the `okfProperties` aspect: the original YAML verbatim, every key as JSON, the concept `id` and `type`, and the source file (repository, path, commit, blob SHA);
- maps `title` to the document title, `type` to its subtype, and `owners`, `tags` and `glossary_terms` to ownership, tags and glossary terms. Other keys stay only in `okfProperties`.

Mapped owners, tags and terms carry an attribution naming the file. A re-import adds the values the file lists and removes only values that the same file added earlier, so owners, tags and terms added in DataHub are kept. Removals need a graph connection; without one, values are only added.

An owner is mapped when it is an email address, a URN, or a user that exists in DataHub. A glossary term is mapped when it is a URN or the name of exactly one term. A value that cannot be mapped stays in `okfProperties` and is counted as `okf_unresolved_values` in the report. Frontmatter that cannot be parsed is imported as plain text, with a warning.

Set `enable_okf: false` to import every file as plain text.

Each imported file document stores these `customProperties` keys:

| Key                       | Purpose                                      |
| ------------------------- | -------------------------------------------- |
| `import_source_id`        | Stable external key for upserts              |
| `content_hash`            | SHA-256 of the whole file (change detection) |
| `extraction_algo_version` | Bump when hash algorithm changes             |
| `github_blob_sha`         | Git blob SHA at last import                  |
| `github_commit_sha`       | Branch HEAD at last import                   |

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
