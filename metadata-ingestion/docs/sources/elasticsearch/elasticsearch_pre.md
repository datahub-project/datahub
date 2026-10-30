### Overview

The `elasticsearch` module ingests metadata from Elasticsearch into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin extracts the following:

- Metadata for indexes
- Column types associated with each index field

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Authentication

The connector supports one of the following authentication methods per recipe:

- **Basic authentication**: set `username` and `password`.
- **API key**: set `api_key` to an `[id, api_key]` pair or a base64-encoded `id:api_key` string.
- **AWS IAM (SigV4)**: for [Amazon OpenSearch Service](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/what-is.html) domains, set `aws_config`. Every request is signed with AWS Signature Version 4.

AWS IAM authentication cannot be combined with basic authentication or an API key. `aws_config.aws_region` is required and must match the domain's region. Credentials resolve from explicit `aws_access_key_id`/`aws_secret_access_key`, an `aws_profile`, an `aws_role` to assume, or the default AWS credential chain (environment variables, instance profile, ECS task role). Assumed-role credentials are renewed automatically before they expire, so long-running ingestion does not need special handling.

Amazon OpenSearch Service domains only accept HTTPS, so set `use_ssl: True` (and `verify_certs: True`) and point `host` at the domain endpoint on port 443, for example `search-my-domain-abc123.us-east-1.es.amazonaws.com:443`.

The IAM principal needs `es:ESHttpGet` on the domain (for example `arn:aws:es:us-east-1:123456789012:domain/my-domain/*`); the connector only issues read requests. If the domain uses fine-grained access control, also map the principal (as a backend role) to an OpenSearch role with these permissions:

- Cluster permissions: `cluster_monitor`, `indices:admin/template/get`, `indices:admin/index_template/get` (template reads are cluster-level actions in OpenSearch security)
- Index permissions on the indices to ingest: `indices_monitor`, `indices:admin/get`, `indices:admin/aliases/get`

OpenSearch Serverless collections are not supported.
