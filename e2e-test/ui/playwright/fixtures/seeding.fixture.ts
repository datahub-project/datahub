/**
 * Seeding fixture — per-worker test data injection with state file caching.
 *
 * Mirrors the pattern established by loginFixture:
 *
 *   1. A suite opts in by setting the `featureName` option at describe level.
 *   2. The fixture checks whether `.seeded/{featureName}.json` exists on disk.
 *   3a. State EXISTS  → data was already ingested this run; skip injection.
 *   3b. State MISSING → read `tests/{featureName}/fixtures/data.json`, POST
 *       each MCP to the GMS REST API, then write the state file so that other
 *       workers (and subsequent tests in this worker) skip ingestion.
 *
 * Worker-scoped means seeding happens AT MOST ONCE per worker process for a
 * given feature name, regardless of how many tests request it.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * Usage in a spec file (set at describe level, never inside a test):
 *
 *   import { test, expect } from '../../fixtures/base-test';
 *
 *   test.use({ featureName: 'search' });
 *   // ^ seeds from: tests/search/fixtures/data.json
 *   // ^ state file: .seeded/search.json
 *
 *   test.describe('Search tests', () => {
 *     test('should find results', async ({ page }) => {
 *       // Data is guaranteed to be present before this test runs
 *     });
 *   });
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * Skip seeding (e.g. on a pre-seeded local stack):
 *   PW_NO_SEED=1 npx playwright test
 *
 * Clear seeded-state flags to force re-ingestion:
 *   rm -rf e2e-test/ui/playwright/.seeded
 *
 * Suites that create their own data at runtime (e.g. via apiMock or direct
 * API calls) do NOT need to set featureName.
 */

import * as fs from 'fs';
import * as path from 'path';
import { test as base, request } from '@playwright/test';
import { readGmsToken } from './auth';
import type { UserCredentials } from './users';
import type { DataHubLogger } from '../utils/logger';

// ── Constants ─────────────────────────────────────────────────────────────────

const SEEDED_DIR = path.join(__dirname, '../.seeded');
const TESTS_DIR = path.join(__dirname, '../tests');

// ── Types ─────────────────────────────────────────────────────────────────────

interface Mcp {
  entityUrn?: string;
  proposedSnapshot?: Record<string, { urn?: string }>;
  [key: string]: unknown;
}

/** Shape written to the state file after a successful seed. */
interface SeedState {
  featureName: string;
  seededAt: string;
  entityCount: number;
}

type SeedingFixtureOptions = {
  /**
   * Feature name identifying the data to inject.
   * Must match the directory under `tests/` that contains
   * `fixtures/data.json` (e.g. `'search'`, `'business-attributes'`).
   *
   * Set to `null` (default) to skip seeding for the suite.
   */
  featureName: string | null;
};

/** Fixture type declarations consumed from other merged fixtures. */
type SeedingDeps = {
  /** Provided by loggerFixture via mergeTests in base-test.ts */
  logger: DataHubLogger;
  /** Provided by loginFixture via mergeTests in base-test.ts */
  user: UserCredentials;
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function stateFilePath(featureName: string): string {
  return path.join(SEEDED_DIR, `${featureName}.json`);
}

function dataFilePath(featureName: string): string {
  return path.join(TESTS_DIR, featureName, 'fixtures', 'data.json');
}

function extractUrn(mcp: Mcp): string | null {
  if (mcp.entityUrn) return mcp.entityUrn;
  if (mcp.proposedSnapshot) {
    const snapshot = Object.values(mcp.proposedSnapshot)[0];
    if (snapshot?.urn) return snapshot.urn;
  }
  return null;
}

async function ingestMcps(
  featureName: string,
  gmsToken: string,
  gmsUrl: string,
  logger: DataHubLogger,
): Promise<void> {
  const dataFile = dataFilePath(featureName);
  if (!fs.existsSync(dataFile)) {
    throw new Error(
      `Seed data file not found: ${dataFile}\n` +
        `Expected: tests/${featureName}/fixtures/data.json`,
    );
  }

  const mcps = JSON.parse(fs.readFileSync(dataFile, 'utf-8')) as Mcp[];
  logger.info('seeding feature data', { featureName, entityCount: mcps.length });

  const apiContext = await request.newContext({
    baseURL: gmsUrl,
    extraHTTPHeaders: {
      Authorization: `Bearer ${gmsToken}`,
      'Content-Type': 'application/json',
    },
  });

  try {
    const failures: string[] = [];

    for (const mcp of mcps) {
      const urn = extractUrn(mcp);
      const response = await apiContext.post(`${gmsUrl}/entities?action=ingest`, {
        data: { entity: { value: mcp.proposedSnapshot ?? mcp } },
        failOnStatusCode: false,
      });

      if (!response.ok()) {
        const body = await response.text();
        const label = urn ?? JSON.stringify(mcp).slice(0, 80);
        failures.push(`${label}: ${response.status()} ${body.slice(0, 200)}`);
        logger.warn('entity ingest failed', { urn, status: response.status() });
      } else {
        logger.info('entity ingested', { urn });
      }
    }

    if (failures.length > 0) {
      throw new Error(
        `Seeding '${featureName}' failed for ${failures.length} entities:\n` +
          failures.join('\n'),
      );
    }

    // Write state file so other workers (and next runs) skip re-seeding.
    fs.mkdirSync(SEEDED_DIR, { recursive: true });
    const state: SeedState = {
      featureName,
      seededAt: new Date().toISOString(),
      entityCount: mcps.length,
    };
    fs.writeFileSync(stateFilePath(featureName), JSON.stringify(state, null, 2));
    logger.info('seed state saved', { featureName, stateFile: stateFilePath(featureName) });
  } finally {
    await apiContext.dispose();
  }
}

// ── Fixture ───────────────────────────────────────────────────────────────────

export const seedingFixture = base.extend<SeedingDeps, SeedingFixtureOptions>({
  // ── Option: injectable feature name (worker-scoped) ───────────────────────
  featureName: [null, { option: true, scope: 'worker' }],

  // ── Worker-scoped auto fixture: seeds once per worker per feature ─────────
  // Using an internal name with underscore prefix to mark it as infrastructure.
  // Tests never destructure this — it runs automatically.
  _seedFeatureData: [
    async ({ featureName, user, logger }, use) => {
      if (!featureName) {
        // Suite does not need seeded data.
        await use();
        return;
      }

      if (process.env.PW_NO_SEED === '1') {
        logger.info('skipping seed (PW_NO_SEED=1)', { featureName });
        await use();
        return;
      }

      const stateFile = stateFilePath(featureName);
      if (fs.existsSync(stateFile)) {
        const state = JSON.parse(fs.readFileSync(stateFile, 'utf-8')) as SeedState;
        logger.info('reusing seeded data', {
          featureName,
          seededAt: state.seededAt,
          entityCount: state.entityCount,
        });
        await use();
        return;
      }

      // Slow path: seed data and save state.
      const baseUrl = process.env.BASE_URL ?? 'http://localhost:9002';
      const gmsUrl = baseUrl.replace(':9002', ':8080');
      const gmsToken = readGmsToken(user.username);

      await ingestMcps(featureName, gmsToken, gmsUrl, logger);

      await use();
    },
    { auto: true, scope: 'worker' },
  ],
});
