### Overview

The `langfuse` module ingests metadata from a single Langfuse project into DataHub.

#### Compatibility

This connector targets the Langfuse Public API v2/v3 surface (`/api/public/v2/observations`, `/api/public/v2/prompts`, `/api/public/v3/scores`). It requires **self-hosted Langfuse v4 or newer**, or Langfuse Cloud. Some self-hosted v4 deployments run in "events_only" mode, which disables the legacy, unversioned `/api/public/traces`-style endpoints entirely; this connector never uses those endpoints, so it works correctly against both events_only and non-events_only v4 deployments. Older self-hosted deployments (pre-v4) that only expose the legacy v1 API are not supported.

#### Extracted Metadata Scope

The connector extracts, on every run:

- The Project as a container
- All Prompts and every version of each Prompt, linked via a shared Version Set

The connector extracts, within a configurable rolling time window (default: the last 7 days):

- Traces, and their `generation`-type Observations (LLM calls)
- Scores attached to those Traces/Observations

### Prerequisites

1. A running Langfuse instance (self-hosted v4+, or Langfuse Cloud).
2. A Langfuse **Public Key** and **Secret Key** for the project you want to ingest. Create these in the Langfuse UI under **Project Settings > API Keys**. The key pair is scoped to exactly one project; ingesting multiple projects requires one recipe per project.

No special role beyond the default API key permissions is required — the API key's Basic Auth credentials already scope every request to the correct project.
