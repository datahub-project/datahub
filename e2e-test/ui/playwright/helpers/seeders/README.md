# Seeders

Utility classes for injecting test data into a running DataHub instance.
Each seeder targets a different API surface.

## When to use which seeder

| Seeder              | Transport                          | Use when                                                                                                                                                       |
| ------------------- | ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cli-seeder.ts`     | DataHub CLI (`datahub ingest`)     | You need to ingest complex entity aspects that the REST API cannot represent (e.g. file-based ingestion with transformers). Requires `datahub` CLI on `$PATH`. |
| `rest-seeder.ts`    | GMS REST `/entities?action=ingest` | Standard entity creation/update from an MCP array. No CLI dependency. Prefer this for direct API control in individual tests.                                  |
| `graphql-seeder.ts` | DataHub GraphQL API                | When you need to interact via GraphQL mutations (e.g. soft-delete/restore). Shares the browser session — no separate auth token needed.                        |

## Standard seeding (most tests)

For routine per-worker test data injection, use **`seedingFixture`** from
`fixtures/seeding.fixture.ts` instead of instantiating a seeder directly.
`seedingFixture` is file-based (REST), idempotent, and caches seed state so
each feature's data is only ingested once per run.

```typescript
// In a spec file — seeds from tests/search/fixtures/data.json
test.use({ featureName: "search" });
```

## Direct seeder usage (fine-grained control)

Instantiate a seeder directly only when a test needs precise control over
which entities are created and when they are cleaned up.

```typescript
import { RestSeeder } from "../../helpers/seeders/rest-seeder";

const seeder = new RestSeeder(page, gmsToken);
const mcps = await seeder.loadTestData(
  path.join(__dirname, "fixtures/data.json"),
);
const urns = await seeder.ingestMCPs(mcps);
await seeder.waitForSync(urns);
// ... test ...
await cleanup.deleteEntities(urns);
```

## Shared utilities

`helpers/seeder-utils.ts` contains shared helpers used by all three seeders:

- `extractUrn(mcp)` — extract the entity URN from an MCP (both snapshot and proposal formats)
- `waitForSync(request, gmsUrl, urns, token?, timeout?)` — poll GMS until all URNs are reachable
