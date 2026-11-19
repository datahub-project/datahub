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

Files are imported as **plain markdown or text** by default. Set `enable_okf: true` to import OKF concepts (below). Other document metadata lives in DataHub aspects and `customProperties`.

#### Open Knowledge Format bundles

Opt in with `enable_okf: true` to import [Open Knowledge Format (OKF) 0.2](https://github.com/GoogleCloudPlatform/open-knowledge-format/blob/main/SPEC.md) concepts alongside ordinary documentation. A concept must be a Markdown file inside an OKF bundle and have a `type` key in its YAML frontmatter. A `type` key alone does not enable OKF handling for a Hugo page or another document outside a bundle.

With OKF enabled, the source discovers bundle roots from `index.md` files whose frontmatter contains `okf_version`:

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
    enable_okf: true
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

With `context/` as its bundle root, this file has `conceptId: guides/mau`. The identity is scoped to `bundle.location` and `bundle.rootPath`. For GitHub, `bundle.location` is the canonical repository URI, such as `https://github.com/example-org/analytics`, without a branch or trailing slash. `rootPath` is relative to that location, without leading or trailing slashes, and is `.` for a repository-root bundle (an empty value could not be used as a search filter). The optional producer extension `id: guide:mau` becomes `declaredId`. Resolve a declared ID within its bundle, rather than treating it as a global identifier. Only the bundle-root index's `okf_version` supplies `bundle.okfVersion`. OKF permits unversioned bundles; declare these in `okf_bundle_roots`. Index files with frontmatter but no `okf_version` increment `okf_unversioned_index_files` and produce an info entry suggesting this setting. Concept-level version keys remain extensions in the raw frontmatter.

For each concept the source:

- imports the text after the frontmatter block as the document body;
- stores the original YAML block, including delimiters, in `okfProperties.raw` as the only stored copy of the frontmatter. Typed fields are read from it, and write-back edits it in place;
- records bundle identity, path-based `conceptId`, optional `declaredId`, type, and description. The optional `revision` holds the branch/ref, commit, and blob SHA read at ingestion, providing the merge base for write-back;
- maps `title` to the document title, `type` to its subtype, and `owners`, `tags`, and `glossary_terms` to native associations;
- stores `status`, `staleAfter`, `generated`, and `verified` as OKF metadata only. These fields do not change the document's native status, owners, or permissions.

The file location is the bundle location followed by its root path and `conceptId` with `.md` appended, joining path components with slashes. The GitHub document source and existing `github_*` custom properties still retain the import location and revision details. `mappingVersion` is recorded in value attribution and source hashes, rather than in `okfProperties`. Bundle location and root path support exact search through `okfBundleLocation` and `okfBundleRootPath`. `okfConceptId` and `okfDeclaredId` also support case-insensitive and text search on Search V3; `okfType` and `okfStatus` provide type and lifecycle filters.

GraphQL exposes a read-only, nullable `okfProperties.frontmatter` field containing the keys as a JSON object parsed from `raw` when read. It returns null if parsing fails. Dates remain strings. Ingestion validates JSON-compatible YAML values, rejecting duplicate keys, non-string keys, anchors, and aliases, but stores no JSON copy. Frontmatter blocks are limited to 64 KiB, including delimiters. Unsafe or oversized frontmatter is imported as plain text with a parse warning. Unknown keys remain in `raw`.

Status strings are kept as written, including unknown values. An omitted status remains unset in the aspect. OKF consumers can apply the spec's implicit `stable` default. Timestamps require ISO 8601 datetimes with an explicit offset and are stored as epoch milliseconds. An invalid timestamp produces a warning and leaves the typed timestamp null. Its original value stays in raw YAML. `generated.by` and `verified[].by` retain the producer's actor strings. A single `verified` object is normalized to a one-element list.

`index.md` and `log.md` are reserved at every level inside a bundle. They remain imported documents and receive no concept aspect or native OKF mapping, even if they contain `type`. All index and log files retain their full text, including frontmatter. Files outside bundles, non-Markdown files, and files without `type` retain their original text. Malformed concept frontmatter is also imported verbatim with a warning.

Mapped owners, tags, and terms carry attribution with `origin: okf`, `bundle` (the canonical location URI), `rootPath`, `conceptId`, `key`, and `mappingVersion`. The commit is included only when known. On each document, re-import adds listed values and removes previously imported values identified by `origin: okf`; the bundle and revision fields describe provenance and do not limit reconciliation. Changes to the bundle root or ingestion source do not leave old copies behind. DataHub-added associations without OKF attribution are preserved. A null or empty association key means no values. Unresolved values prevent missing-value removals for that key, while resolved values can still move from an old source key to the current source key. Without a graph connection, imports emit additions only and cannot remove existing values.

An owner maps when it is an email address, a `corpuser` or `corpGroup` URN, or a user that exists in DataHub. Tag names map directly to tag URNs without an existence check. A glossary term maps when it is a URN or the name of exactly one catalog term. Values that cannot be mapped remain in raw frontmatter and increment `okf_unresolved_values`. Invalid typed OKF values increment `okf_invalid_values` and produce warnings. Catalog changes alone do not retry unresolved values on unchanged files; edit the file to trigger a retry. Owner-existence and term-name lookups are cached for each run.

If a file stops being a concept, a graph connection lets the source UPSERT an empty `okfProperties` record, remove its OKF-attributed associations, and UPSERT an empty subtype list only when the stored subtype still equals the previous OKF type. The source never emits aspect DELETEs. An aspect without `raw` is neutral and is treated like an absent aspect; GraphQL returns null for it. Subsequent runs leave an already neutral aspect alone. All top-level fields are optional to permit this neutral value; when `raw` is present, `bundle`, `conceptId`, and `type` are set. A subtype edited in DataHub is preserved. Without a graph, previous OKF metadata cannot be cleared. Setting `enable_okf: false` disables discovery and mapping and also clears previous OKF state when a graph is available.

The source resolves the configured branch head once and lists and reads files at that commit SHA. It stops if the head cannot be resolved, so the content, blob SHA, and revision commit refer to one snapshot. GitHub links and branch custom properties retain the configured branch.

`content_hash` is the sync-back contract: it hashes exactly `documentInfo.contents.text` plus the extraction version, including only the body for a concept. `source_content_hash` hashes the complete source file and, for enabled OKF files inside a bundle, the mapping version and bundle metadata. Frontmatter edits and bundle changes trigger re-ingestion; mapping changes trigger re-ingestion only inside bundles.

Each imported file document stores these `customProperties` keys:

| Key                       | Purpose                                                                 |
| ------------------------- | ----------------------------------------------------------------------- |
| `import_source_id`        | Stable external key for upserts                                         |
| `content_hash`            | Hash of stored document text (sync-back contract)                       |
| `source_content_hash`     | Hash of raw source and applicable OKF interpretation (change detection) |
| `extraction_algo_version` | Bump when hash algorithm changes                                        |
| `github_blob_sha`         | Git blob SHA at last import                                             |
| `github_commit_sha`       | Pinned commit SHA read during this import                               |

Cloud sync-back may additionally write `last_exported_content_hash` to prevent re-import loops after export. The source skips a file when its new source hash matches stored `source_content_hash`, or its whole-file text hash matches `last_exported_content_hash`. Documents without `source_content_hash` are re-imported once unless the sync-back loop check matches.

#### Server compatibility

OKF import is disabled by default. Set `enable_okf: true` only when your DataHub server supports the `okfProperties` aspect. The config flag alone controls OKF import; the source does not check the server registry or silently disable OKF. Enabling OKF on a server without the aspect fails the ingestion run when the server rejects the writes.

#### Stateful ingestion

Enable `stateful_ingestion.enabled: true` (recommended for scheduled syncs) to soft-delete documents that disappear from the GitHub tree between runs. Unchanged files are skipped when a graph connection is available and the source-hash or sync-back loop check matches.

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
