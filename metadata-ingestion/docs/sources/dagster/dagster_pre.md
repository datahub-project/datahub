### Prerequisites

The connector talks to the GraphQL endpoint served by the Dagster webserver (`dagster-webserver`, formerly `dagit`):

- **Dagster OSS**: point `host` at the webserver, e.g. `http://localhost:3000`. The OSS endpoint is unauthenticated, so secure it at the network/proxy layer.
- **Dagster+ (Cloud)**: set `is_cloud: true`, point `host` at your organization host (e.g. `https://my-org.dagster.cloud`), set the `deployment` (e.g. `prod` or a branch deployment), and provide a user `token`. A user/role with at least **Viewer** permission on the target deployment is sufficient for metadata extraction.

The connector issues read-only GraphQL queries; it never launches runs or mutates Dagster state.
