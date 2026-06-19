### Overview

The `hightouch` module ingests metadata from Hightouch into DataHub. It is intended for
production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

#### 1. Generate a Hightouch API Key

1. Log in to your Hightouch account
2. Navigate to **Settings** → **API Keys**
3. Click **Create API Key**
4. Give your key a descriptive name (e.g., "DataHub Integration")
5. Select the appropriate permissions:
   - **Read access** to: Sources, Models, Syncs, Destinations, Sync Runs
6. Copy the generated API key (you won't be able to see it again)
7. Store the API key securely — you'll use it in the DataHub recipe configuration

#### 2. Required Permissions

The API key needs read access to:

- Sources
- Models
- Syncs
- Destinations
- Sync Runs (for execution history)
