### Overview

The `metabase` module ingests metadata from Metabase into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

To use this connector, you'll need:

- Metabase version v0.41+ (Models require v0.41+)
- Authentication credentials (either username/password or API key — **API key is recommended**)
- Appropriate permissions to access the Metabase API

#### Authentication

DataHub supports two authentication methods:

1. **API Key** (Recommended) — more secure, no password management required. Generate one under Account Settings → API Keys in your Metabase instance.
2. **Username/Password**
