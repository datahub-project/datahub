


# GitLab

## Overview

GitLab hosts source files, documentation, and knowledge bases in git repositories. Learn more in the [official GitLab documentation](https://docs.gitlab.com/).

The DataHub `gitlab-documents` integration ingests markdown and text files from a GitLab project on GitLab.com or self-managed GitLab as Document entities. Folder structure in the project is preserved as parent-child document relationships in DataHub.

## Concept Mapping

| Source Concept       | DataHub Concept | Notes                                                          |
| -------------------- | --------------- | -------------------------------------------------------------- |
| Project folder       | Document        | Optional folder documents for navigation.                      |
| Markdown / text file | Document        | Native (editable) or external (read-only) depending on config. |
| Project + branch     | Custom metadata | Stored on documents for traceability.                          |


## Module `gitlab-documents`
![Alpha](https://img.shields.io/badge/support%20status-Alpha-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `gitlab-documents` source ingests files from a GitLab project branch using the GitLab REST API (v4). It works with GitLab.com and self-managed GitLab, and is designed for scheduled, repeatable imports of documentation into DataHub, such as context documents nested under a parent folder.

### Prerequisites

#### GitLab access token

Create a personal, project, or group access token with the `read_api` scope. The `read_repository` scope is not enough, because the source reads the branch and repository tree through the API. Project and group access tokens need at least the Reporter role.

Store the token in a DataHub secret and reference it from your ingestion recipe.

#### Project access

Set `repository` to the project's full path, including any subgroups (`group/subgroup/project`), or paste the project URL. For self-managed GitLab, set `base_url` to the instance URL, or use a project URL and the instance is inferred from it.

#### Supported imports

- Import `.md`, `.txt`, and other configured extensions
- Preserve folder hierarchy as parent-child documents
- Optionally attach imported trees under a parent document URN
- Import as native (editable) or external (read-only) documents
- Schedule recurring syncs via ingestion sources


### Install the Plugin
```shell
pip install 'acryl-datahub[gitlab-documents]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: gitlab-documents
  config:
    gitlab_token: "${GITLAB_TOKEN}"
    # Omit base_url for GitLab.com.
    base_url: https://gitlab.example.com
    repository: acme/handbook
    branch: main
    path_prefix: docs
    file_extensions:
      - .md
      - .txt
    parent_document_urn: null
    create_repo_root_document: true
    max_files: 500
    document_import_mode: NATIVE
    show_in_global_context: true
    stateful_ingestion:
      enabled: true

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">gitlab_token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | GitLab personal, project, or group access token with the read_api scope. Project and group tokens need at least the Reporter role.  |
| <div className="path-line"><span className="path-main">repository</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Project to ingest, as its full path ('group/subgroup/project') or URL ('https://gitlab.com/group/project').  |
| <div className="path-line"><span className="path-main">base_url</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | GitLab instance URL. Set this for self-managed GitLab, for example 'https://gitlab.example.com'. Inferred from repository when that is a URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">https://gitlab.com</span></div> |
| <div className="path-line"><span className="path-main">branch</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Branch to read files from. <div className="default-line default-line-with-docs">Default: <span className="default-value">main</span></div> |
| <div className="path-line"><span className="path-main">create_repo_root_document</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When True, create a folder document named after the repository and nest imported files beneath it. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">document_import_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "NATIVE", "EXTERNAL"  |
| <div className="path-line"><span className="path-main">enable_okf</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Opt in to importing OKF concepts inside discovered or configured bundles. Requires a server that supports the okfProperties aspect; enabling this on an unsupported server fails the run. Disabled by default to import every file verbatim without OKF metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_organization_in_browse_path</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include the repository owner (GitHub organization, GitLab group path) as the top-most entry in the browse path, above the repository name. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_repository_in_browse_path</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include the repository name as a top-level entry in the browse path. When a repository root document is created, that document already provides the repository entry; this adds it for configurations that skip the root document or nest under a configured parent document. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_files</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of matching files to import per run. Additional matches are skipped with a report warning. <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
| <div className="path-line"><span className="path-main">parent_document_urn</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional parent document URN. Top-level imported items are nested beneath this document while preserving the repository folder hierarchy. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">path_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Only ingest files under this path (e.g. 'docs'). <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">show_in_global_context</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether imported documents appear in global search and navigation. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">file_extensions</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | File extensions to include (include the leading dot).  |
| <div className="path-line"><span className="path-prefix">file_extensions.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">okf_bundle_roots</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Additional bundle roots relative to the repository, for bundles without a versioned index.md. Use an empty string for the repository root. Directories with an index.md declaring okf_version are also discovered.  |
| <div className="path-line"><span className="path-prefix">okf_bundle_roots.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "DocumentImportMode": {
      "description": "Whether ingested documents are native (editable) or external (read-only references).",
      "enum": [
        "NATIVE",
        "EXTERNAL"
      ],
      "title": "DocumentImportMode",
      "type": "string"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Configuration for ingesting markdown and text documents from a GitLab project.",
  "properties": {
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful Ingestion Config"
    },
    "repository": {
      "description": "Project to ingest, as its full path ('group/subgroup/project') or URL ('https://gitlab.com/group/project').",
      "title": "Repository",
      "type": "string"
    },
    "branch": {
      "default": "main",
      "description": "Branch to read files from.",
      "title": "Branch",
      "type": "string"
    },
    "path_prefix": {
      "default": "",
      "description": "Only ingest files under this path (e.g. 'docs').",
      "title": "Path Prefix",
      "type": "string"
    },
    "file_extensions": {
      "description": "File extensions to include (include the leading dot).",
      "items": {
        "type": "string"
      },
      "title": "File Extensions",
      "type": "array"
    },
    "max_files": {
      "default": 500,
      "description": "Maximum number of matching files to import per run. Additional matches are skipped with a report warning.",
      "minimum": 1,
      "title": "Max Files",
      "type": "integer"
    },
    "create_repo_root_document": {
      "default": true,
      "description": "When True, create a folder document named after the repository and nest imported files beneath it.",
      "title": "Create Repo Root Document",
      "type": "boolean"
    },
    "parent_document_urn": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Optional parent document URN. Top-level imported items are nested beneath this document while preserving the repository folder hierarchy.",
      "title": "Parent Document Urn"
    },
    "document_import_mode": {
      "$ref": "#/$defs/DocumentImportMode",
      "default": "NATIVE",
      "description": "NATIVE imports editable documents in DataHub. EXTERNAL imports read-only references that link back to the repository."
    },
    "show_in_global_context": {
      "default": true,
      "description": "Whether imported documents appear in global search and navigation.",
      "title": "Show In Global Context",
      "type": "boolean"
    },
    "enable_okf": {
      "default": false,
      "description": "Opt in to importing OKF concepts inside discovered or configured bundles. Requires a server that supports the okfProperties aspect; enabling this on an unsupported server fails the run. Disabled by default to import every file verbatim without OKF metadata.",
      "title": "Enable Okf",
      "type": "boolean"
    },
    "okf_bundle_roots": {
      "description": "Additional bundle roots relative to the repository, for bundles without a versioned index.md. Use an empty string for the repository root. Directories with an index.md declaring okf_version are also discovered.",
      "items": {
        "type": "string"
      },
      "title": "Okf Bundle Roots",
      "type": "array"
    },
    "include_repository_in_browse_path": {
      "default": true,
      "description": "Include the repository name as a top-level entry in the browse path. When a repository root document is created, that document already provides the repository entry; this adds it for configurations that skip the root document or nest under a configured parent document.",
      "title": "Include Repository In Browse Path",
      "type": "boolean"
    },
    "include_organization_in_browse_path": {
      "default": false,
      "description": "Include the repository owner (GitHub organization, GitLab group path) as the top-most entry in the browse path, above the repository name.",
      "title": "Include Organization In Browse Path",
      "type": "boolean"
    },
    "gitlab_token": {
      "description": "GitLab personal, project, or group access token with the read_api scope. Project and group tokens need at least the Reporter role.",
      "format": "password",
      "title": "Gitlab Token",
      "type": "string",
      "writeOnly": true
    },
    "base_url": {
      "default": "https://gitlab.com",
      "description": "GitLab instance URL. Set this for self-managed GitLab, for example 'https://gitlab.example.com'. Inferred from repository when that is a URL.",
      "title": "Base Url",
      "type": "string"
    }
  },
  "required": [
    "repository",
    "gitlab_token"
  ],
  "title": "GitLabDocumentsSourceConfig",
  "type": "object"
}
```





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

This source shares its import logic with the [GitHub documents source](/docs/generated/ingestion/sources/github), so hashing, unchanged-file skipping, stateful soft-deletion, and Open Knowledge Format bundles (`enable_okf: true`) work the same way. The differences are:

- Custom properties use the `gitlab_` prefix: `gitlab_repo`, `gitlab_branch`, `gitlab_file_path`, `gitlab_directory_path`, `gitlab_commit_sha`, and `gitlab_blob_sha`.
- The OKF `bundle.location` is the project URL, such as `https://gitlab.com/example-group/analytics`.
- With `include_organization_in_browse_path: true`, the browse path starts with the full group path (`data-platform/analytics`).

#### Several GitLab instances or overlapping project paths

Document IDs are built from the project path. Two sources whose projects produce the same IDs overwrite each other's documents unless each sets a different `platform_instance`. This happens in two cases:

- The same project path on two GitLab instances, such as gitlab.com and a self-managed server. Give each instance its own name, such as `gitlab-com` and `self-managed`.
- Project paths that differ only by `.` and `/`, such as `data/platform.docs` and `data/platform/docs`. IDs join path segments with `.`, so these two produce the same IDs even on one server. Give the two sources different instance names, such as `self-managed-platform-docs` and `self-managed-platform`. One name for the whole server is not enough here.

```yaml
source:
  type: gitlab-documents
  config:
    gitlab_token: "${GITLAB_TOKEN}"
    base_url: https://gitlab.example.com
    repository: data/platform.docs
    platform_instance: self-managed
```

The instance is part of every document ID, and it becomes the documents' data platform instance. Without it, the data platform instance is the project path. Instance names cannot be blank or contain `.`, `,`, `(` or `)`; surrounding spaces are removed. Changing `platform_instance` on an existing source creates new documents; with stateful ingestion enabled, the old ones are soft-deleted.

The source resolves the branch head once and reads the tree and files at that commit, so all documents in a run come from one snapshot.

### Limitations

- Maximum file size is 1 MB per file. Larger files are skipped with a `Repository file not read` warning.
- Requires network access from the ingestion executor to the GitLab instance.
- Binary formats are not parsed; use text/markdown files.
- Imports are capped by `max_files` (default 500) per run.
- Document IDs are built from the project path. Without `platform_instance`, two sources whose projects produce the same IDs overwrite each other's documents. See [Several GitLab instances or overlapping project paths](#several-gitlab-instances-or-overlapping-project-paths).
- Changes made in DataHub are not written back to GitLab.

### Troubleshooting

- **404 from GitLab**: GitLab answers 404, not 403, for private projects the token cannot see. Check the project path (including subgroups), the branch name, and that the token's user or bot is a project member.
- **401 / 403 from GitLab**: Verify the token has not expired and has the `read_api` scope. Project and group tokens need at least the Reporter role.
- **Wrong instance**: When `repository` is a URL and `base_url` is also set, the URL must be on that instance.
- **No files imported**: Check `path_prefix` and `file_extensions` filters.
- **`Repository file not read` warnings**: The file could not be downloaded, was too large, or was not text. A file that failed to download keeps its previous import, so a temporary GitLab error does not remove the document.
- **`Repository tree truncated` warning**: GitLab returned a full page of the project tree without pagination headers, and a follow-up request showed more entries exist. This usually means a proxy stripped the headers. If the follow-up request failed, the warning's context includes the API error. The run imports what it listed and skips stale document removal, so nothing is deleted. Let the executor reach the GitLab API directly.
- **Redirect errors**: The source does not follow redirects, so the token is never sent to another host. A redirect usually means a proxy or SSO login in front of the API, or a `base_url` that is not the address serving the API. Set `base_url` to that address.
- **Non-JSON or HTML response errors**: A proxy or SSO login page answered instead of the GitLab API. Let the ingestion executor reach the API directly with the token.
- **Documents not removed after a GitLab delete**: Ensure `stateful_ingestion.enabled: true` and the pipeline has a graph connection for checkpoint storage.


### Code Coordinates
- Class Name: `datahub.ingestion.source.gitlab_documents.gitlab_documents_source.GitLabDocumentsSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/gitlab_documents/gitlab_documents_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for GitLab, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
