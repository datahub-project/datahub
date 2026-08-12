/**
 * Shard-group fixture — pure worker-hash lever for CI shard balancing.
 *
 * Playwright's `--shard=N/M` slices tests by cumulative test *count*, not
 * duration, and — under fullyParallel — the order it slices is driven by
 * each test's worker hash, which is derived from the resolved value of every
 * worker-scoped option a test uses (including `featureName` from
 * seeding.fixture.ts). Directories with inconsistent or absent `featureName`
 * usage get pulled into shared hash buckets positioned wherever that bucket
 * first appears, scattering a single directory's tests across unrelated
 * shards (see docs/audit-reports/shard-rebalancing-analysis-2026-08-12.md).
 *
 * `shardGroup` exists solely to give every spec file a consistent,
 * intentional value to key that hash on, independent of `featureName` (which
 * also drives real data-seeding and must not be repurposed for this). It has
 * no runtime behavior — it is never read by a test or fixture body.
 *
 * Usage (set at file top-level, alongside featureName if present):
 *
 *   test.use({ shardGroup: 'g3', featureName: 'documents' });
 */

import { test as base } from '@playwright/test';

type ShardGroupFixtureOptions = {
  shardGroup: string | null;
};

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export const shardGroupFixture = base.extend<{}, ShardGroupFixtureOptions>({
  shardGroup: [null, { option: true, scope: 'worker' }],
});
