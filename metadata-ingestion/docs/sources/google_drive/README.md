## Overview

Google Drive is Google Workspace's file storage and collaboration service, and Google Docs is its document editor. Learn more in the [official Google Drive documentation](https://support.google.com/drive).

The DataHub integration for Google Drive ingests Google Docs from the folders and shared drives you choose as DataHub Document entities. It keeps the folder hierarchy, owners, and links back to Drive, generates semantic-search embeddings inline, and supports stateful deletion detection.

## Concept Mapping

| Source Concept                 | DataHub Concept              | Notes                                                                                                                    |
| ------------------------------ | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Configured folder/shared drive | Document (subtype `Folder`)  | Emitted when `create_root_documents` is enabled (the default).                                                           |
| Folder                         | Document (subtype `Folder`)  | Empty body; parent of the documents and folders it contains.                                                             |
| Google Doc                     | Document                     | Body is the Doc exported as Markdown; with the default `EXTERNAL` import mode, links back to Drive via its external URL. |
| File owner                     | CorpUser (owner)             | Keyed by email address. Files in shared drives have no owners.                                                           |
| Folder path                    | Browse path, parent document | Every ancestor folder is a clickable browse-path entry; with `create_root_documents: false`, the root is a plain label.  |
| Google Workspace               | Platform instance (optional) | Set `platform_instance` to distinguish workspaces; URNs use globally unique file IDs.                                    |
