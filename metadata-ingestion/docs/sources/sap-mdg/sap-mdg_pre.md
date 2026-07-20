### Overview

The `sap-mdg` module ingests SAP Master Data Governance metadata by parsing the OData `$metadata` document of each configured service. Entity sets become datasets, entity type properties become schema fields (with EDM types mapped to DataHub types and SAP labels used as descriptions), and navigation properties with referential constraints become foreign keys. Both OData V2 and V4 services are supported.

### Prerequisites

#### Identify the OData services

Each MDG data model is published as one or more OData services under the SAP Gateway, typically at a path such as `/sap/opu/odata/sap/<SERVICE_NAME>`. List every service you want to ingest under `services`; the connector fetches `<base_url>/<service>/$metadata` for each.

#### Provide credentials

The connector needs a principal that can read the service metadata. Supply exactly one of:

- `username` + `password` for HTTP basic authentication,
- `token` for bearer (e.g. OAuth2) authentication, or
- `client_certificate_path` (+ `client_key_path`) for X.509 mutual TLS.

Set `sap_client` if the Gateway requires an explicit `sap-client` to be passed on each request.

#### TLS

By default the server certificate is verified. For a private certificate authority, point `ca_certificate_path` at the CA bundle rather than disabling `verify_ssl`.
