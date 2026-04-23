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
 * NOTE: This fixture is intentionally worker-scoped.  Worker-scoped fixtures
 * cannot depend on test-scoped fixtures such as `logger`.  A dedicated logger
 * instance is created directly via createLogger() using workerInfo.workerIndex.
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
import { test as base, request, type Browser } from '@playwright/test';
import { readGmsToken, gmsTokenPath } from './login';
import type { UserCredentials } from '../data/users';
import { LoginPage } from '../pages/login.page';
import { gmsUrl } from '../utils/constants';
import { extractUrn, type Mcp } from '../helpers/seeder-utils';
import { createLogger, type DataHubLogger } from '../utils/logger';

// ── GMS token bootstrap ───────────────────────────────────────────────────────

/**
 * Creates and saves a GMS personal access token for `user` using a headless
 * browser login. Called by the worker-scoped seeding fixture on first run,
 * before any test-scoped fixture (e.g. loginFixture.context) has had a chance
 * to create the token file.
 */
async function bootstrapGmsToken(browser: Browser, user: UserCredentials): Promise<string> {
  const tokenFile = gmsTokenPath(user.username);
  const baseURL = process.env.BASE_URL ?? 'http://localhost:9002';

  const ctx = await browser.newContext({ baseURL });
  const page = await ctx.newPage();
  try {
    const loginPage = new LoginPage(page);
    await loginPage.navigateToLogin();
    await loginPage.login(user.username, user.password);

    const cookies = await ctx.cookies();
    const actorCookie = cookies.find((c) => c.name === 'actor');
    if (!actorCookie) throw new Error(`'actor' cookie not found after login for '${user.username}'`);

    const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
    const apiCtx = await request.newContext({
      baseURL,
      extraHTTPHeaders: { Cookie: cookieHeader },
    });
    try {
      const resp = await apiCtx.post('/api/v2/graphql', {
        data: {
          query: `mutation createAccessToken($input: CreateAccessTokenInput!) {
            createAccessToken(input: $input) { accessToken metadata { id } }
          }`,
          variables: {
            input: {
              type: 'PERSONAL',
              actorUrn: actorCookie.value,
              duration: 'ONE_MONTH',
              name: `Playwright Test Token — ${user.username}`,
            },
          },
        },
      });
      if (!resp.ok()) throw new Error(`createAccessToken failed: ${resp.status()}`);
      const body = (await resp.json()) as {
        data?: { createAccessToken?: { accessToken?: string; metadata?: { id?: string } } };
      };
      const token = body.data?.createAccessToken?.accessToken;
      const tokenId = body.data?.createAccessToken?.metadata?.id;
      if (!token) throw new Error('Empty access token in response');
      fs.mkdirSync(path.dirname(tokenFile), { recursive: true });
      fs.writeFileSync(tokenFile, JSON.stringify({ token, tokenId, actorUrn: actorCookie.value }, null, 2));
      return token;
    } finally {
      await apiCtx.dispose();
    }
  } finally {
    await page.close();
    await ctx.close();
  }
}

// ── Constants ─────────────────────────────────────────────────────────────────

const SEEDED_DIR = path.join(__dirname, '../.seeded');
const TESTS_DIR = path.join(__dirname, '../tests');

/** Global shared data ingested before any feature-specific seeding. */
const GLOBAL_DATA_FILE = path.join(__dirname, '../test-data/data.json');
const GLOBAL_FEATURE_NAME = 'global-data';

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
  /** Internal worker-scoped auto fixture — not consumed by tests. */
  _seedFeatureData: void;
  /**
   * Worker-scoped user option declared here so the worker fixture can access
   * it. The actual value is provided by loginFixture via mergeTests.
   */
  user: UserCredentials;
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function stateFilePath(featureName: string): string {
  return path.join(SEEDED_DIR, `${featureName}.json`);
}

function dataFilePath(featureName: string): string {
  return path.join(TESTS_DIR, featureName, 'fixtures', 'data.json');
}

/**
 * Ingest MCPs from a data file into the GMS.
 *
 * @param throwOnFailure - When true (default), throws if any entity fails.
 *   Set to false for optional/global data where partial ingestion is acceptable.
 */
async function ingestMcps(
  featureName: string,
  gmsToken: string,
  gmsBaseUrl: string,
  logger: DataHubLogger,
  explicitDataFile?: string,
  throwOnFailure = true,
): Promise<void> {
  const dataFile = explicitDataFile ?? dataFilePath(featureName);
  if (!fs.existsSync(dataFile)) {
    throw new Error(
      `Seed data file not found: ${dataFile}\n` +
        `Expected: tests/${featureName}/fixtures/data.json`,
    );
  }

  // Strip the legacy "pegasus2avro." namespace prefix from Avro-translated class names so
  // that the GMS REST API accepts the snapshot format in current DataHub versions.
  const raw = fs.readFileSync(dataFile, 'utf-8').replace(/com\.linkedin\.pegasus2avro\./g, 'com.linkedin.');
  const mcps = JSON.parse(raw) as Mcp[];
  logger.info(`seeding '${featureName}'`, { entityCount: mcps.length });

  const apiContext = await request.newContext({
    baseURL: gmsBaseUrl,
    extraHTTPHeaders: {
      Authorization: `Bearer ${gmsToken}`,
      'Content-Type': 'application/json',
    },
  });

  try {
    const failures: string[] = [];

    for (const mcp of mcps) {
      let urn: string;
      try {
        urn = extractUrn(mcp);
      } catch {
        const label = JSON.stringify(mcp).slice(0, 80);
        failures.push(`${label}: could not extract URN`);
        continue;
      }

      // Legacy snapshot format uses /entities?action=ingest;
      // new MCP format (with entityUrn but no proposedSnapshot) uses /aspects?action=ingestProposal.
      const response = mcp.proposedSnapshot
        ? await apiContext.post(`${gmsBaseUrl}/entities?action=ingest`, {
            data: { entity: { value: mcp.proposedSnapshot } },
            failOnStatusCode: false,
          })
        : await apiContext.post(`${gmsBaseUrl}/aspects?action=ingestProposal`, {
            data: { proposal: mcp },
            failOnStatusCode: false,
          });

      if (!response.ok()) {
        const body = await response.text();
        failures.push(`${urn}: ${response.status()} ${body.slice(0, 200)}`);
        logger.warn('entity ingest failed', { urn, status: response.status() });
      } else {
        logger.info('ingested', { urn });
      }
    }

    if (failures.length > 0) {
      const msg = `Seeding '${featureName}' had ${failures.length} failed entities:\n${failures.join('\n')}`;
      if (throwOnFailure) {
        throw new Error(msg);
      } else {
        logger.warn(msg);
      }
    }

    // Write state file so other workers (and next runs) skip re-seeding.
    // Written even on partial failures (when throwOnFailure=false) so we don't retry endlessly.
    fs.mkdirSync(SEEDED_DIR, { recursive: true });
    const state: SeedState = {
      featureName,
      seededAt: new Date().toISOString(),
      entityCount: mcps.length,
    };
    fs.writeFileSync(stateFilePath(featureName), JSON.stringify(state, null, 2));
    logger.info('state saved', { featureName });
  } finally {
    await apiContext.dispose();
  }
}

// ── Fixture ───────────────────────────────────────────────────────────────────

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export const seedingFixture = base.extend<{}, SeedingFixtureOptions>({
  // ── Option: injectable feature name (worker-scoped) ───────────────────────
  featureName: [null, { option: true, scope: 'worker' }],

  // ── Worker-scoped auto fixture: seeds once per worker per feature ─────────
  // Using an internal name with underscore prefix to mark it as infrastructure.
  // Tests never destructure this — it runs automatically.
  _seedFeatureData: [
    async ({ featureName, user, browser }, use, workerInfo) => {
      const logger = createLogger('', {
        suite: 'seeding',
        test: featureName ?? 'global',
        worker: workerInfo.workerIndex,
        retry: 0,
      });

      if (process.env.PW_NO_SEED === '1') {
        logger.info('skipping all seeding (PW_NO_SEED=1)');
        await use();
        return;
      }

      const tokenFile = gmsTokenPath(user.username);
      const gmsToken = fs.existsSync(tokenFile)
        ? readGmsToken(user.username)
        : await bootstrapGmsToken(browser, user);

      // Track whether any fresh ingestion happened this run so we can wait
      // for the search index to catch up before tests start.
      let freshlySeeded = false;

      // Always seed global shared data (test-data/data.json) once per worker.
      if (fs.existsSync(GLOBAL_DATA_FILE)) {
        const globalStateFile = stateFilePath(GLOBAL_FEATURE_NAME);
        if (fs.existsSync(globalStateFile)) {
          const state = JSON.parse(fs.readFileSync(globalStateFile, 'utf-8')) as SeedState;
          logger.info('reusing global data', { seededAt: state.seededAt, entityCount: state.entityCount });
        } else {
          // throwOnFailure=false: global data has mixed-format MCPs; partial failures are non-blocking.
          await ingestMcps(GLOBAL_FEATURE_NAME, gmsToken, gmsUrl(), logger, GLOBAL_DATA_FILE, false);
          freshlySeeded = true;
        }
      }

      if (!featureName) {
        if (freshlySeeded) {
          logger.info('waiting 15s for search index to catch up');
          await new Promise<void>((resolve) => setTimeout(resolve, 15_000));
        }
        await use();
        return;
      }

      const stateFile = stateFilePath(featureName);
      if (fs.existsSync(stateFile)) {
        const state = JSON.parse(fs.readFileSync(stateFile, 'utf-8')) as SeedState;
        logger.info('reusing seeded data', { featureName, seededAt: state.seededAt, entityCount: state.entityCount });
        if (freshlySeeded) {
          logger.info('waiting 15s for search index to catch up');
          await new Promise<void>((resolve) => setTimeout(resolve, 15_000));
        }
        await use();
        return;
      }

      // Slow path: seed feature-specific data and save state.
      await ingestMcps(featureName, gmsToken, gmsUrl(), logger);
      freshlySeeded = true;

      logger.info('waiting 15s for search index to catch up');
      await new Promise<void>((resolve) => setTimeout(resolve, 15_000));

      await use();
    },
    { auto: true, scope: 'worker' },
  ],
});
