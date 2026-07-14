### Overview

The `tibco-bw` module ingests deployment metadata for TIBCO integration
applications. It supports two runtimes behind a single `deployment` switch:

- `on_prem` — TIBCO ActiveMatrix BusinessWorks (BW / BWCE), read through the
  `bwagent` REST API. The connector walks domains, appspaces, appnodes, and
  deployed applications.
- `cloud` — TIBCO Cloud Integration (TCI), read through the public cloud REST
  API. The connector lists the caller's subscriptions and their deployed apps.

Each deployment scope (an appspace on-prem, or a subscription on cloud) is
modeled as a Data Flow, and each deployed application within it as a Data Job.

### Prerequisites

#### On-prem (ActiveMatrix BusinessWorks)

- Network access to the `bwagent` REST API (default port `8079`), e.g.
  `http://bw-host.example.com:8079`.
- A user with permission to read domains, appspaces, appnodes, and
  applications. Supplied via `username` / `password` (HTTP basic auth).

#### Cloud (TIBCO Cloud Integration)

- An OAuth access token for the TIBCO Cloud Integration API, supplied via
  `token`. The token's subscriptions determine which applications are visible.
- The `base_url` defaults to `https://api.cloud.tibco.com` and rarely needs to
  be overridden.
