### Capabilities

#### Context documents from self-managed GitLab

```yaml
source:
  type: gitlab-documents
  config:
    gitlab_token: "${GITLAB_TOKEN}"
    base_url: https://gitlab.example.com
    repository: data-platform/analytics/handbook
    branch: main
    path_prefix: docs
    parent_document_urn: "urn:li:document:context-handbook"
    document_import_mode: NATIVE
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
  type: gitlab-documents
  config:
    gitlab_token: "${GITLAB_TOKEN}"
    repository: https://gitlab.com/example-group/analytics
    document_import_mode: EXTERNAL
    stateful_ingestion:
      enabled: true
```

Documents link back to the file in GitLab (`<project URL>/-/blob/<branch>/<path>`), and folder documents link to the folder (`/-/tree/`).

#### Shared behaviour with `github-documents`

This source shares its import logic with the [GitHub documents source](https://docs.datahub.com/docs/generated/ingestion/sources/github), so hashing, unchanged-file skipping, stateful soft-deletion, and Open Knowledge Format bundles (`enable_okf: true`) work the same way. The differences are:

- Custom properties use the `gitlab_` prefix: `gitlab_repo`, `gitlab_branch`, `gitlab_file_path`, `gitlab_directory_path`, `gitlab_commit_sha`, and `gitlab_blob_sha`.
- The OKF `bundle.location` is the project URL, such as `https://gitlab.com/example-group/analytics`.
- With `include_organization_in_browse_path: true`, the browse path starts with the full group path (`data-platform/analytics`).

The source resolves the branch head once and reads the tree and files at that commit, so all documents in a run come from one snapshot.

### Limitations

- Maximum file size is 1 MB per file. Larger files are skipped with a `gitlab-file-not-read` warning.
- Requires network access from the ingestion executor to the GitLab instance.
- Binary formats are not parsed; use text/markdown files.
- Imports are capped by `max_files` (default 500) per run.
- Document IDs are built from the project path, not the instance. Two sources that ingest the same project path from different GitLab instances into one DataHub overwrite each other's documents.
- Changes made in DataHub are not written back to GitLab.

### Troubleshooting

- **404 from GitLab**: GitLab answers 404, not 403, for private projects the token cannot see. Check the project path (including subgroups), the branch name, and that the token's user or bot is a project member.
- **401 / 403 from GitLab**: Verify the token has not expired and has the `read_api` scope. Project and group tokens need at least the Reporter role.
- **Wrong instance**: When `repository` is a URL and `base_url` is also set, the URL must be on that instance.
- **No files imported**: Check `path_prefix` and `file_extensions` filters.
- **`gitlab-file-not-read` warnings**: The file could not be downloaded, was too large, or was not text. A file that failed to download keeps its previous import, so a temporary GitLab error does not remove the document.
- **Non-JSON response errors**: A proxy or SSO login page answered instead of the GitLab API. Let the ingestion executor reach the API directly with the token.
- **Documents not removed after a GitLab delete**: Ensure `stateful_ingestion.enabled: true` and the pipeline has a graph connection for checkpoint storage.
