


# GitHub

## Overview

GitHub hosts source files, documentation, and knowledge bases in git repositories. Learn more in the [official GitHub documentation](https://docs.github.com/).

The DataHub `github-documents` integration ingests markdown and text files from a GitHub repository as Document entities. Folder structure in the repository is preserved as parent-child document relationships in DataHub.

## Concept Mapping

| Source Concept       | DataHub Concept | Notes                                                          |
| -------------------- | --------------- | -------------------------------------------------------------- |
| Repository folder    | Document        | Optional folder documents for navigation.                      |
| Markdown / text file | Document        | Native (editable) or external (read-only) depending on config. |
| Repository + branch  | Custom metadata | Stored on documents for traceability.                          |


## Module `github-documents`
![Alpha](https://img.shields.io/badge/support%20status-Alpha-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `github-documents` source ingests files from a GitHub repository branch using the GitHub REST API. It is designed for scheduled, repeatable imports of documentation into DataHub—especially context documents nested under a parent folder.

### Prerequisites

#### GitHub access token

Create a GitHub Personal Access Token (classic or fine-grained) with read access to the target repository:

- `repo` (private repositories) or `contents: read` (fine-grained)

Store the token in a DataHub secret and reference it from your ingestion recipe.

#### Repository access

Ensure the token can read the repository, branch, and paths you configure.

#### Supported imports

- Import `.md`, `.txt`, and other configured extensions
- Preserve folder hierarchy as parent-child documents
- Optionally attach imported trees under a parent document URN
- Import as native (editable) or external (read-only) documents
- Schedule recurring syncs via ingestion sources


### Install the Plugin
```shell
pip install 'acryl-datahub[github-documents]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: github-documents
  config:
    github_token: "${GITHUB_TOKEN}"
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
| <div className="path-line"><span className="path-main">github_token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | GitHub access token (PAT or GitHub App installation token).  |
| <div className="path-line"><span className="path-main">repository</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Repository to ingest, as 'owner/repo' or 'https://github.com/owner/repo'.  |
| <div className="path-line"><span className="path-main">branch</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Branch to read files from. <div className="default-line default-line-with-docs">Default: <span className="default-value">main</span></div> |
| <div className="path-line"><span className="path-main">create_repo_root_document</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When True, create a folder document named after the repository and nest imported files beneath it. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">document_import_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "NATIVE", "EXTERNAL"  |
| <div className="path-line"><span className="path-main">enable_okf</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Opt in to importing OKF concepts inside discovered or configured bundles. Requires a server that supports the okfProperties aspect; enabling this on an unsupported server fails the run. Disabled by default to import every file verbatim without OKF metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_organization_in_browse_path</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include the GitHub organization/owner name as the top-most entry in the browse path, above the repository name. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_repository_in_browse_path</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include the repository name as a top-level entry in the browse path. When a repository root document is created, that document already provides the repository entry; this adds it for configurations that skip the root document or nest under a configured parent document. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_files</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of matching files to import per run. Additional matches are skipped with a report warning. <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
| <div className="path-line"><span className="path-main">parent_document_urn</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional parent document URN. Top-level imported items are nested beneath this document while preserving the GitHub folder hierarchy. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">path_prefix</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Only ingest files under this path (e.g. 'docs'). <div className="default-line default-line-with-docs">Default: <span className="default-value"></span></div> |
| <div className="path-line"><span className="path-main">show_in_global_context</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether imported documents appear in global search and navigation. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">document_mapping</span></div> <div className="type-name-line"><span className="type-name">DocumentMappingConfig</span></div> | Document entity mapping configuration.  |
| <div className="path-line"><span className="path-prefix">document_mapping.</span><span className="path-main">id_pattern</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern for generating document IDs <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;source&#95;type&#125;-&#123;directory&#125;-&#123;basename&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.</span><span className="path-main">status</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "PUBLISHED", "UNPUBLISHED" <div className="default-line default-line-with-docs">Default: <span className="default-value">PUBLISHED</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.</span><span className="path-main">id_normalization</span></div> <div className="type-name-line"><span className="type-name">IdNormalizationConfig</span></div> | Document ID normalization rules.  |
| <div className="path-line"><span className="path-prefix">document_mapping.id_normalization.</span><span className="path-main">lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Convert to lowercase <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.id_normalization.</span><span className="path-main">max_length</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum ID length <div className="default-line default-line-with-docs">Default: <span className="default-value">200</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.id_normalization.</span><span className="path-main">remove_special_chars</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Remove special characters except _ and - <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.id_normalization.</span><span className="path-main">replace_spaces_with</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Replace spaces with this character <div className="default-line default-line-with-docs">Default: <span className="default-value">-</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.</span><span className="path-main">source</span></div> <div className="type-name-line"><span className="type-name">SourceConfig</span></div> | Document source configuration.  |
| <div className="path-line"><span className="path-prefix">document_mapping.source.</span><span className="path-main">include_external_id</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include external ID in DocumentSource <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.source.</span><span className="path-main">include_external_url</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Include external URL in DocumentSource <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.source.</span><span className="path-main">type</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "NATIVE", "EXTERNAL" <div className="default-line default-line-with-docs">Default: <span className="default-value">EXTERNAL</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.</span><span className="path-main">title</span></div> <div className="type-name-line"><span className="type-name">TitleExtractionConfig</span></div> | Title extraction configuration.  |
| <div className="path-line"><span className="path-prefix">document_mapping.title.</span><span className="path-main">extract_from_content</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Try to extract title from document content <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.title.</span><span className="path-main">fallback_to_filename</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use filename as title if not found in content <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">document_mapping.title.</span><span className="path-main">max_length</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum title length <div className="default-line default-line-with-docs">Default: <span className="default-value">500</span></div> |
| <div className="path-line"><span className="path-main">file_extensions</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | File extensions to include (include the leading dot).  |
| <div className="path-line"><span className="path-prefix">file_extensions.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">hierarchy</span></div> <div className="type-name-line"><span className="type-name">HierarchyConfig</span></div> | Hierarchy configuration.  |
| <div className="path-line"><span className="path-prefix">hierarchy.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable parent-child relationships <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.</span><span className="path-main">parent_strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "folder", "none", "custom", "notion", "confluence" <div className="default-line default-line-with-docs">Default: <span className="default-value">folder</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.</span><span className="path-main">custom_mapping</span></div> <div className="type-name-line"><span className="type-name">One of CustomMappingConfig, null</span></div> | Custom mapping configuration <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.custom_mapping.</span><span className="path-main">rules</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Custom parent mapping rules  |
| <div className="path-line"><span className="path-prefix">hierarchy.custom_mapping.rules.</span><span className="path-main">CustomParentRule</span></div> <div className="type-name-line"><span className="type-name">CustomParentRule</span></div> | Custom parent mapping rule.  |
| <div className="path-line"><span className="path-prefix">hierarchy.custom_mapping.rules.CustomParentRule.</span><span className="path-main">parent_id</span>&nbsp;<abbr title="Required if CustomParentRule is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Parent document ID for matching files  |
| <div className="path-line"><span className="path-prefix">hierarchy.custom_mapping.rules.CustomParentRule.</span><span className="path-main">pattern</span>&nbsp;<abbr title="Required if CustomParentRule is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Glob pattern to match file paths  |
| <div className="path-line"><span className="path-prefix">hierarchy.</span><span className="path-main">folder_mapping</span></div> <div className="type-name-line"><span className="type-name">FolderMappingConfig</span></div> | Folder hierarchy mapping configuration.  |
| <div className="path-line"><span className="path-prefix">hierarchy.folder_mapping.</span><span className="path-main">create_parent_docs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Create Document entities for folders <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.folder_mapping.</span><span className="path-main">max_depth</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum hierarchy depth <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.folder_mapping.</span><span className="path-main">parent_id_pattern</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Pattern for parent document IDs <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;source&#95;type&#125;-&#123;directory&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">hierarchy.folder_mapping.</span><span className="path-main">root_parent</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Optional root document URN <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
    "CustomMappingConfig": {
      "additionalProperties": false,
      "description": "Custom parent mapping configuration.",
      "properties": {
        "rules": {
          "description": "Custom parent mapping rules",
          "items": {
            "$ref": "#/$defs/CustomParentRule"
          },
          "title": "Rules",
          "type": "array"
        }
      },
      "title": "CustomMappingConfig",
      "type": "object"
    },
    "CustomParentRule": {
      "additionalProperties": false,
      "description": "Custom parent mapping rule.",
      "properties": {
        "pattern": {
          "description": "Glob pattern to match file paths",
          "title": "Pattern",
          "type": "string"
        },
        "parent_id": {
          "description": "Parent document ID for matching files",
          "title": "Parent Id",
          "type": "string"
        }
      },
      "required": [
        "pattern",
        "parent_id"
      ],
      "title": "CustomParentRule",
      "type": "object"
    },
    "DocumentImportMode": {
      "description": "Whether ingested documents are native (editable) or external (read-only references).",
      "enum": [
        "NATIVE",
        "EXTERNAL"
      ],
      "title": "DocumentImportMode",
      "type": "string"
    },
    "DocumentMappingConfig": {
      "additionalProperties": false,
      "description": "Document entity mapping configuration.",
      "properties": {
        "id_pattern": {
          "default": "{source_type}-{directory}-{basename}",
          "description": "Pattern for generating document IDs",
          "title": "Id Pattern",
          "type": "string"
        },
        "id_normalization": {
          "$ref": "#/$defs/IdNormalizationConfig",
          "description": "ID normalization rules"
        },
        "title": {
          "$ref": "#/$defs/TitleExtractionConfig",
          "description": "Title extraction configuration"
        },
        "source": {
          "$ref": "#/$defs/SourceConfig",
          "description": "Source configuration"
        },
        "status": {
          "default": "PUBLISHED",
          "description": "Default publication status",
          "enum": [
            "PUBLISHED",
            "UNPUBLISHED"
          ],
          "title": "Status",
          "type": "string"
        }
      },
      "title": "DocumentMappingConfig",
      "type": "object"
    },
    "FolderMappingConfig": {
      "additionalProperties": false,
      "description": "Folder hierarchy mapping configuration.",
      "properties": {
        "create_parent_docs": {
          "default": true,
          "description": "Create Document entities for folders",
          "title": "Create Parent Docs",
          "type": "boolean"
        },
        "parent_id_pattern": {
          "default": "{source_type}-{directory}",
          "description": "Pattern for parent document IDs",
          "title": "Parent Id Pattern",
          "type": "string"
        },
        "max_depth": {
          "default": 10,
          "description": "Maximum hierarchy depth",
          "maximum": 50,
          "minimum": 1,
          "title": "Max Depth",
          "type": "integer"
        },
        "root_parent": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Optional root document URN",
          "title": "Root Parent"
        }
      },
      "title": "FolderMappingConfig",
      "type": "object"
    },
    "HierarchyConfig": {
      "additionalProperties": false,
      "description": "Hierarchy configuration.",
      "properties": {
        "enabled": {
          "default": true,
          "description": "Enable parent-child relationships",
          "title": "Enabled",
          "type": "boolean"
        },
        "parent_strategy": {
          "default": "folder",
          "description": "Parent document creation strategy. 'notion' extracts parent from Notion API metadata. 'confluence' extracts parent from Confluence page ancestors.",
          "enum": [
            "folder",
            "none",
            "custom",
            "notion",
            "confluence"
          ],
          "title": "Parent Strategy",
          "type": "string"
        },
        "folder_mapping": {
          "$ref": "#/$defs/FolderMappingConfig",
          "description": "Folder mapping configuration"
        },
        "custom_mapping": {
          "anyOf": [
            {
              "$ref": "#/$defs/CustomMappingConfig"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Custom mapping configuration"
        }
      },
      "title": "HierarchyConfig",
      "type": "object"
    },
    "IdNormalizationConfig": {
      "additionalProperties": false,
      "description": "Document ID normalization rules.",
      "properties": {
        "lowercase": {
          "default": true,
          "description": "Convert to lowercase",
          "title": "Lowercase",
          "type": "boolean"
        },
        "replace_spaces_with": {
          "default": "-",
          "description": "Replace spaces with this character",
          "title": "Replace Spaces With",
          "type": "string"
        },
        "remove_special_chars": {
          "default": true,
          "description": "Remove special characters except _ and -",
          "title": "Remove Special Chars",
          "type": "boolean"
        },
        "max_length": {
          "default": 200,
          "description": "Maximum ID length",
          "title": "Max Length",
          "type": "integer"
        }
      },
      "title": "IdNormalizationConfig",
      "type": "object"
    },
    "SourceConfig": {
      "additionalProperties": false,
      "description": "Document source configuration.",
      "properties": {
        "type": {
          "default": "EXTERNAL",
          "description": "Document source type: NATIVE for editable DataHub documents, EXTERNAL for read-only references.",
          "enum": [
            "NATIVE",
            "EXTERNAL"
          ],
          "title": "Type",
          "type": "string"
        },
        "include_external_url": {
          "default": true,
          "description": "Include external URL in DocumentSource",
          "title": "Include External Url",
          "type": "boolean"
        },
        "include_external_id": {
          "default": true,
          "description": "Include external ID in DocumentSource",
          "title": "Include External Id",
          "type": "boolean"
        }
      },
      "title": "SourceConfig",
      "type": "object"
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
    },
    "TitleExtractionConfig": {
      "additionalProperties": false,
      "description": "Title extraction configuration.",
      "properties": {
        "extract_from_content": {
          "default": true,
          "description": "Try to extract title from document content",
          "title": "Extract From Content",
          "type": "boolean"
        },
        "fallback_to_filename": {
          "default": true,
          "description": "Use filename as title if not found in content",
          "title": "Fallback To Filename",
          "type": "boolean"
        },
        "max_length": {
          "default": 500,
          "description": "Maximum title length",
          "title": "Max Length",
          "type": "integer"
        }
      },
      "title": "TitleExtractionConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Configuration for ingesting markdown and text documents from a GitHub repository.",
  "properties": {
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
    "github_token": {
      "description": "GitHub access token (PAT or GitHub App installation token).",
      "format": "password",
      "title": "Github Token",
      "type": "string",
      "writeOnly": true
    },
    "repository": {
      "description": "Repository to ingest, as 'owner/repo' or 'https://github.com/owner/repo'.",
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
      "description": "Optional parent document URN. Top-level imported items are nested beneath this document while preserving the GitHub folder hierarchy.",
      "title": "Parent Document Urn"
    },
    "document_import_mode": {
      "$ref": "#/$defs/DocumentImportMode",
      "default": "NATIVE",
      "description": "NATIVE imports editable documents in DataHub. EXTERNAL imports read-only references that link back to GitHub."
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
      "description": "Include the GitHub organization/owner name as the top-most entry in the browse path, above the repository name.",
      "title": "Include Organization In Browse Path",
      "type": "boolean"
    },
    "document_mapping": {
      "$ref": "#/$defs/DocumentMappingConfig"
    },
    "hierarchy": {
      "$ref": "#/$defs/HierarchyConfig",
      "description": "Parent-child relationship configuration."
    }
  },
  "required": [
    "github_token",
    "repository"
  ],
  "title": "GitHubDocumentsSourceConfig",
  "type": "object"
}
```





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


### Code Coordinates
- Class Name: `datahub.ingestion.source.github_documents.github_documents_source.GitHubDocumentsSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/github_documents/github_documents_source.py)


:::tip Questions?

If you've got any questions on configuring ingestion for GitHub, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
