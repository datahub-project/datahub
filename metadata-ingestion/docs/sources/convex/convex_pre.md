### Overview

The `convex` module ingests table metadata from one or more Convex deployments. For each deployment it emits a container, and for each table a dataset with schema fields, document-reference descriptions, and an optional row-count profile.

Metadata is read through Convex's [streaming export API](https://docs.convex.dev/http-api/#streaming-export), which is available on every deployment. Nothing needs to be installed or deployed on the Convex side.

### Prerequisites

You need a deploy key for each deployment you want to ingest. A read-only key with the `deployment:data:view` scope is sufficient.

#### Steps to Get the Required Information

1. Open your project in the [Convex dashboard](https://dashboard.convex.dev/).
2. Go to **Settings** → **Deploy keys** and generate a key, or run `npx convex deployment token create`.
3. Copy the deployment URL from the same settings page. It looks like `https://happy-animal-123.convex.cloud`.
4. Expose the key to the ingestion process as an environment variable, and reference it from the recipe rather than writing it inline.

:::note

Convex adds bookkeeping fields (`_table`, `_component`, `_ts`, `_deleted`) to every streaming-export document. They are excluded from the emitted schema so that it matches the table as your application defines it. The `_id` and `_creationTime` fields, which Convex documents genuinely carry, are kept.

:::
