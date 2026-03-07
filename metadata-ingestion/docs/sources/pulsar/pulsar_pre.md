### Overview

The `pulsar` module ingests metadata from Pulsar into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

- **Pulsar Instance**: Access with valid access token (if authentication enabled)
- **Version**: Pulsar 2.7.0 or later
- **Role**: `superUser` required to list all tenants

> **_NOTE:_** A `superUser` role is required for listing all existing tenants within a Pulsar instance.
