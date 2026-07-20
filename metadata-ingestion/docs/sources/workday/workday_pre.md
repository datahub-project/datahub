### Overview

[Workday](https://www.workday.com/) is a cloud platform for HR, finance, and analytics. This source ingests metadata from [Workday Prism Analytics](https://www.workday.com/en-us/products/prism-analytics/overview.html) via the Prism Analytics REST API (v3).

The tenant is ingested as a container. Prism tables are ingested as datasets with schema fields (including primary-key tags, nullability, and precision/scale/length when reported), row-count profiles, and last-refresh operation stats. Prism datasets (pipeline definitions) are ingested as datasets carrying their transformation logic (Data Prep Language) as view properties, and external data sources as datasets. Lineage flows from tables to the datasets and data sources they derive from — including column-level lineage from Prism dataset field mappings — and external data sources can additionally resolve to the upstream warehouse dataset they were loaded from via `data_source_platform_mapping`.

By default the connector fetches each Prism table/dataset's full definition via a per-object detail call (`extract_schema_details`), because Prism list endpoints omit schema fields, relationships, timestamps, and transformation logic. Disable it only if your tenant's list responses already include full detail.

When the API reports them, objects also carry catalog tags, business-glossary terms, an owning security group (as a `corpGroup` owner alongside the individual owner), and — via the `domain` config — a DataHub domain.

Three additional, off-by-default capabilities go beyond core Prism tables:

- `extract_buckets` ingests Prism buckets (upload staging areas) and links each as an upstream of the table it publishes to.
- `extract_business_objects` ingests the tenant's queryable business-object catalog (via the Workday Query Language metadata API) as datasets with schema, including object-to-object lineage from declared references.
- `extract_custom_reports` ingests Workday custom report (RaaS) definitions as report datasets, with a schema from their output fields and column-level lineage back to the business object they read from.

### Prerequisites

1. In Workday, run the **Register API Client for Integrations** task to create an OAuth 2.0 client with the **Client Credentials** grant type.
2. Link the API client to an **Integration System User (ISU)** whose security groups grant read access to Prism Analytics (and the functional areas behind the reports you want to ingest).
3. Note the generated **Client ID** and **Client Secret** (the secret is shown only once), your **tenant** name, and your Workday **services host** (for example `https://wd2-impl-services1.workday.com`).

The connector requests short-lived bearer tokens from `{base_url}/ccx/oauth2/{tenant}/token` using the client credentials and refreshes them automatically; the Client Credentials grant does not issue a refresh token.
