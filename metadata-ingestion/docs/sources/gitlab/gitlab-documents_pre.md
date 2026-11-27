### Overview

The `gitlab-documents` source ingests files from a GitLab project branch using the GitLab REST API (v4). It works with GitLab.com and self-managed GitLab, and is designed for scheduled, repeatable imports of documentation into DataHub, such as context documents nested under a parent folder.

### Prerequisites

#### GitLab access token

Create a personal, project, or group access token with the `read_api` scope. The `read_repository` scope is not enough, because the source reads the branch and repository tree through the API. Project and group access tokens need at least the Reporter role.

Store the token in a DataHub secret and reference it from your ingestion recipe.

#### Project access

Set `repositories` to a list of project full paths, including any subgroups (`group/subgroup/project`), or project URLs. For self-managed GitLab, set `base_url` to the instance URL, or use a project URL and the instance is inferred from it. To ingest every project in a group instead, set `group`; see [Several projects from one source](#several-projects-from-one-source).

A project access token only reaches its own project, so use a group access token with at least the Reporter role to cover several projects or a group.

`repository`, which names a single project, is deprecated. Use `repositories` instead.

#### Supported imports

- Import `.md`, `.txt`, and other configured extensions
- Preserve folder hierarchy as parent-child documents
- Optionally attach imported trees under a parent document URN
- Import as native (editable) or external (read-only) documents
- Schedule recurring syncs via ingestion sources
