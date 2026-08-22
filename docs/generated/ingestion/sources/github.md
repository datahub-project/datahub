


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
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


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
