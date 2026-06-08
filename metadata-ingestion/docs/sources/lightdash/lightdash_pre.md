### Overview

The `lightdash` module ingests metadata from a [Lightdash](https://www.lightdash.com/) deployment into DataHub. It extracts Projects, Spaces, Saved Charts (with column-level lineage to the warehouse), and Dashboards. Ownership is best-effort from each entity's `updatedByUser`. Module-specific capabilities and limitations are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity from the ingestion host to the Lightdash `/api/v1/...` endpoints and a valid Personal Access Token (PAT). Tested against Lightdash Cloud and self-hosted Lightdash `>= v0.1500`.

#### Authentication

Generate a **Personal Access Token** following Lightdash's [personal access tokens documentation](https://docs.lightdash.com/references/personal_tokens).

Lightdash expects the PAT to be sent as `Authorization: ApiKey <token>` (NOT `Bearer <token>`); the connector handles the header formatting automatically.

#### Permissions

The PAT inherits the permissions of the user that created it. DataHub ingestion requires the user to have at least `viewer` access to every project you want to ingest. We recommend creating a dedicated `datahub-ingest` user that has `viewer` access to the relevant projects, rather than reusing a personal account.

#### Storing the token

The `personal_access_token` config field is a Pydantic `SecretStr` and is never logged or echoed back. Pass it through a secret reference such as `${LIGHTDASH_PAT}` in your recipe rather than committing it inline.
