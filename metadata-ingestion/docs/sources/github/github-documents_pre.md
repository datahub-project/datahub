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
