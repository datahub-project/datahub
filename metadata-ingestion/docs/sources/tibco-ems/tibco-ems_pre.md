### Overview

The `tibco-ems` module ingests metadata from [TIBCO Enterprise Message Service (EMS)](https://www.tibco.com/products/tibco-enterprise-message-service) into DataHub. It extracts queues and topics as datasets and derives lineage between them from configured EMS bridges.

Metadata is read through the **EMS REST Proxy** (the admin/monitoring HTTP API shipped with EMS 10.x). The plugin first opens a server session via `POST /connect`, then reads:

- `GET /system/ems/queues` — the configured queues
- `GET /system/ems/topics` — the configured topics
- `GET /system/ems/configuration/bridges` — bridges, used to build lineage edges

Queues and topics are ingested as [Datasets](docs/generated/metamodel/entities/dataset.md) with the `Queue` and `Topic` subtypes respectively. Destination attributes (for example `global`, `secure`, message/byte limits, prefetch and expiration) are attached as dataset custom properties.

### Prerequisites

- **EMS REST Proxy**: reachable base URL for the EMS 10.x REST Proxy admin API.
- **Credentials**: an EMS user (basic auth) or bearer token with permission to view destinations and configuration.
