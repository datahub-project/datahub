### Overview

:::caution Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

The SharePoint source ingests metadata from Microsoft SharePoint via the Microsoft Graph API. It supports two modes selected by the `mode` field:

- **`data_lake`** (default): Ingests structured files (CSV, Parquet, JSON, Avro, Excel, PDF) from document libraries as DataHub Dataset entities with inferred schema, container hierarchy, and dataset properties.
- **`document`**: Ingests SharePoint site pages and documents (Word, PDF, PowerPoint, Markdown) as DataHub Document entities with optional semantic embeddings for semantic search.

Both modes authenticate using an Azure AD service principal (client secret or certificate) via the Microsoft Graph API.

#### Key Features

##### 1. Data Lake Mode

- **Schema Inference**: Automatically infers column types and schema from CSV, Parquet, JSON, Avro, and Excel files
- **Container Hierarchy**: Emits a full Site → Library → Folder → Dataset container hierarchy for browse navigation
- **Filtering**: Allow/deny patterns for sites and document libraries; skip personal OneDrive sites
- **Stateful Ingestion**: Detects and soft-deletes stale dataset entities across runs

##### 2. Document Mode

- **Page Ingestion**: Extracts text from SharePoint site pages via the Graph API canvas layout
- **Document File Ingestion**: Parses Word, PDF, PowerPoint, and Markdown files using `unstructured`
- **Semantic Embeddings**: Optional vector embedding generation for semantic search (Cohere, AWS Bedrock)
- **Stateful Ingestion**: Incremental updates using content-hash change detection

#### Related Documentation

- [Microsoft Graph API](https://docs.microsoft.com/en-us/graph/overview)
- [Azure AD App Registration](https://docs.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app)
- [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md)

### Prerequisites

#### 1. Azure AD App Registration

Create an Azure AD service principal with access to the Microsoft Graph API:

1. Go to the [Azure Portal](https://portal.azure.com) → **Azure Active Directory** → **App registrations**
2. Click **New registration**, give it a name (e.g. `datahub-sharepoint`), and register
3. Note the **Application (client) ID** and **Directory (tenant) ID**

#### 2. API Permissions

Add the following **Application** permissions (not Delegated) under **API permissions** → **Microsoft Graph**:

| Permission       | Type        | Purpose                                |
| ---------------- | ----------- | -------------------------------------- |
| `Sites.Read.All` | Application | List and read all SharePoint sites     |
| `Files.Read.All` | Application | Read files from all document libraries |

Click **Grant admin consent** to apply the permissions.

#### 3. Authentication Credentials

Choose one of:

**Client secret** (simpler):

1. Under **Certificates & secrets** → **Client secrets**, click **New client secret**
2. Copy the secret value immediately — it is only shown once

**Certificate** (recommended for production):

1. Under **Certificates & secrets** → **Certificates**, upload a public certificate (`.pem`)
2. Store the private key file path accessible to the ingestion host

#### 4. Embedding Provider (Optional)

If you want semantic search in document mode, configure an embedding provider in your DataHub instance. Supported providers include Cohere (API key) and AWS Bedrock (IAM roles).

See [Semantic Search Configuration](../../../how-to/semantic-search-configuration.md) for setup details.
