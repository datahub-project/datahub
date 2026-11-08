### Capabilities

#### Context documents under a parent folder

```yaml
source:
  type: github-documents
  config:
    github_token: "${GITHUB_TOKEN}"
    repository: example-org/analytics
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
    repository: https://github.com/example-org/analytics
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

#### Open Knowledge Format bundles

You can import [Open Knowledge Format (OKF) 0.2](https://github.com/GoogleCloudPlatform/open-knowledge-format/blob/main/SPEC.md) concepts alongside ordinary documentation. A concept must be a Markdown file inside an OKF bundle and have a `type` key in its YAML frontmatter. A `type` key alone does not enable OKF handling for a Hugo page or another document outside a bundle.

The source discovers bundle roots from `index.md` files whose frontmatter contains `okf_version`:

```markdown
---
okf_version: "0.2"
---

# Analytics knowledge bundle

- [Monthly active users](guides/mau.md)
```

An index may define a bundle at the repository root or in a subdirectory, such as `context/index.md`. If bundles nest, the deepest enclosing root determines each concept's identity and version. The source checks ancestor indexes from the existing GitHub tree, even when they sit above `path_prefix`. It fetches each needed index once per run.

For bundles without a versioned index, declare roots in your ingestion recipe:

```yaml
source:
  type: github-documents
  config:
    github_token: "${GITHUB_TOKEN}"
    repository: example-org/analytics
    okf_bundle_roots: [context]
```

Use `okf_bundle_roots: [""]` for a repository-root bundle. Roots must be repository-relative POSIX paths. Explicit roots supplement discovered roots. A bundle without a declared version has no typed `okfVersion`. Unknown version strings are preserved and consumed on a best-effort basis.

A concept at `context/guides/mau.md` can contain:

```markdown
---
id: guide:mau
type: Guide
title: Monthly active users
owners: [alice@example.com]
tags: [metrics]
glossary_terms: [Active User]
status: stable
stale_after: 2026-09-23T00:00:00Z
generated: { by: reference_agent/model, at: 2026-06-20T22:53:05Z }
verified: { by: human:alice, at: 2026-06-25T09:00:00Z }
audience: analysts
---

# Monthly active users
```

With `context/` as its bundle root, this file has `conceptId: guides/mau`. The identity is scoped to `bundle.location` and `bundle.rootPath`. For GitHub, `bundle.location` is the canonical repository URI, such as `https://github.com/example-org/analytics`, without a branch or trailing slash. `rootPath` is relative to that location, without leading or trailing slashes, and is `.` for a repository-root bundle (an empty value could not be used as a search filter). The optional producer extension `id: guide:mau` becomes `declaredId`. Resolve a declared ID within its bundle, rather than treating it as a global identifier. Only the bundle-root index's `okf_version` supplies `bundle.okfVersion`. Concept-level version keys remain extensions in the raw frontmatter.

For each concept the source:

- imports the text after the frontmatter block as the document body;
- stores the original YAML block, including delimiters, in required `okfProperties.raw` as the only stored copy of the frontmatter. Typed fields are read from it, and write-back edits it in place;
- records bundle identity, path-based `conceptId`, optional `declaredId`, type, and description. The optional `revision` holds the branch/ref, commit, and blob SHA read at ingestion, providing the merge base for write-back;
- maps `title` to the document title, `type` to its subtype, and `owners`, `tags`, and `glossary_terms` to native associations;
- stores `status`, `staleAfter`, `generated`, and `verified` as OKF metadata only. These fields do not change the document's native status, owners, or permissions.

The file location is the bundle location followed by its root path and `conceptId` with `.md` appended, joining path components with slashes. The GitHub document source and existing `github_*` custom properties still retain the import location and revision details. `mappingVersion` is recorded in value attribution and content hashes, rather than in `okfProperties`. Bundle location and root path support exact search; concept and declared IDs also support case-insensitive and text search on Search V3.

GraphQL exposes a read-only, nullable `okfProperties.frontmatter` field containing the keys as a JSON object parsed from `raw` when read. It returns null if parsing fails. Dates remain strings. Ingestion validates JSON-compatible YAML values, rejecting duplicate keys and non-string keys, but stores no JSON copy. Unknown keys remain in `raw`.

Status strings are kept as written, including unknown values. An omitted status remains unset in the aspect. OKF consumers can apply the spec's implicit `stable` default. Timestamps require ISO 8601 datetimes with an explicit offset and are stored as epoch milliseconds. An invalid timestamp produces a warning and leaves the typed timestamp null. Its original value stays in raw YAML. `generated.by` and `verified[].by` retain the producer's actor strings. A single `verified` object is normalized to a one-element list.

`index.md` and `log.md` are reserved at every level inside a bundle. They remain imported documents and receive no concept aspect or native OKF mapping, even if they contain `type`. A bundle-root `index.md` has its valid frontmatter block removed from the body. Other index files and log files retain their full text. Files outside bundles, non-Markdown files, and files without `type` retain their original text. Malformed concept frontmatter is also imported verbatim with a warning.

Mapped owners, tags, and terms carry attribution with `origin: okf`, `bundle` (the canonical location URI), `rootPath`, `conceptId`, `key`, and `mappingVersion`. The commit is included only when known. Re-import adds listed values and removes only values with matching origin, bundle location, root path, and concept ID. DataHub-added associations and values attributed to another bundle, root, or concept are preserved. Removals require a graph connection. Without one, or when a key has unresolved values, reconciliation for that key is add-only.

An owner maps when it is an email address, a URN, or a user that exists in DataHub. Tag names map directly to tag URNs without an existence check. A glossary term maps when it is a URN or the name of exactly one catalog term. Values that cannot be mapped remain in raw frontmatter and increment `okf_unresolved_values`. Invalid typed OKF values increment `okf_invalid_values` and produce warnings.

Set `enable_okf: false` to disable bundle discovery, frontmatter stripping, and all OKF mapping. Content hashes cover the complete raw file and the mapping/bundle metadata, so frontmatter edits, bundle version changes, and mapping changes trigger re-ingestion.

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
