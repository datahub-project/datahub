/**
 * Seeding fixture — per-worker test data injection with state file caching.
 *
 * Mirrors the pattern established by loginFixture:
 *
 *   1. A suite opts in by setting the `featureName` option at describe level.
 *   2. The fixture claims `.seeded/{featureName}.json` via withArtifactLock.
 *   2a. State EXISTS  → data was already ingested this run; skip injection.
 *   2b. Lock acquired → read `tests/{featureName}/fixtures/data.json`, POST
 *       each MCP to the GMS REST API, wait for the search index to catch up,
 *       then write the state file so other workers skip ingestion.
 *   2c. Lock held by another worker → poll for the state file it is
 *       producing instead of duplicating the ingestion (see withArtifactLock;
 *       this is what makes seeding safe under workers_per_shard > 1).
 *
 * Worker-scoped means seeding is attempted AT MOST ONCE per worker process for
 * a given feature name, regardless of how many tests request it; across
 * workers sharing a filesystem, exactly one of them actually performs it.
 *
 * ── Local vs. CI: two different seeding strategies ──────────────────────────
 *
 * Locally, the lazy per-feature strategy above is fast for a dev iterating on
 * one feature: only the current suite's `featureName` gets ingested.
 *
 * In CI (`process.env.CI === 'true'`), Playwright's `fullyParallel` scheduler
 * groups tests onto workers by a hash of worker-scoped options — including
 * `featureName`, which is set inconsistently across spec files — and shard
 * assignment is now done per spec file (duration-based bin-packing) rather
 * than by feature directory. That means a shard/worker can no longer assume
 * "if a test doesn't declare featureName X, X's data was never needed here":
 * a test can end up depending on another feature's fixture data purely by
 * shard placement. So in CI, before the per-feature check above runs, a
 * worker seeds EVERY feature's `tests/{featureName}/fixtures/data.json` up front —
 * data locality no longer depends on which shard/worker a test lands on.
 * See `seedAllFeaturesForCi`.
 *
 * Naively looping "ingest feature → wait 15s for the index → write state"
 * over ~25 features would serialize 6+ minutes of pure waiting per shard,
 * making CI slower. Instead the batch path ingests every pending feature
 * back-to-back with NO per-feature wait (`ingestMcpsNoWait`), then performs
 * ONE index wait for the whole batch (longer than the single-feature default —
 * see `BATCH_INDEX_CATCH_UP_MS`), then writes all state files — see `ingestMcps`
 * vs. `ingestMcpsNoWait` below.
 *
 * The whole batch is itself guarded by a single withArtifactLock keyed to a
 * `.seeded/_all-features.json` marker, so only one worker per shard performs
 * it; siblings under `workers_per_shard > 1` poll for the marker instead of
 * duplicating the batch. After the batch, the normal per-feature
 * `withArtifactLock(stateFile, ...)` check still runs unconditionally — for a
 * suite that also declares `featureName`, that state file was already
 * written by the batch, so it short-circuits immediately at no extra cost.
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
import { DATAHUB_GRAPHQL_PATH, gmsUrl } from '../utils/constants';
import {
  extractUrn,
  normalizeMcp,
  extractComplexAspects,
  ingestComplexAspects,
  withArtifactLock,
  writeJsonAtomic,
  type Mcp,
} from '../helpers/seeder-utils';
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
      const resp = await apiCtx.post(DATAHUB_GRAPHQL_PATH, {
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
      writeJsonAtomic(tokenFile, { token, tokenId, actorUrn: actorCookie.value });
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

/**
 * Playwright fixture timeouts are static per-declaration (evaluated once at
 * module load), not runtime-conditional on the current test — so the CI vs.
 * local budget has to be computed here rather than inside the fixture body.
 * CI seeds every feature in one batch (see module docstring); local dev only
 * ever seeds the current suite's one feature.
 */
const SEED_TIMEOUT_MS = process.env.CI === 'true' ? 600_000 : 180_000;

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

/** Default wait for a single feature's (or global-data's) search/graph index catch-up. */
const INDEX_CATCH_UP_MS = 15_000;
/**
 * CI batch seeds ~25 features' worth of MCPs (and their complex-aspects second pass) back
 * to back in one burst, which backlogs the async MAE/graph-index pipeline more than the old
 * one-feature-at-a-time pattern did — relationship edges (e.g. glossaryRelatedTerms) were
 * observed lagging behind plain document search visibility under this load. Give the batch
 * a longer wait rather than inflating the single-feature default used everywhere else.
 */
const BATCH_INDEX_CATCH_UP_MS = 30_000;

/** Wait for the search/graph index to catch up. Shared by single-feature and batch ingestion. */
async function waitForIndexCatchUp(logger: DataHubLogger, waitMs: number = INDEX_CATCH_UP_MS): Promise<void> {
  logger.info(`waiting ${waitMs}ms for search index to catch up before marking seed complete`);
  await new Promise<void>((resolve) => setTimeout(resolve, waitMs));
}

/** Write the state file marking `featureName` as seeded. */
function writeSeedState(featureName: string, entityCount: number): void {
  const state: SeedState = {
    featureName,
    seededAt: new Date().toISOString(),
    entityCount,
  };
  writeJsonAtomic(stateFilePath(featureName), state);
}

/**
 * Ingest MCPs from a data file into the GMS, WITHOUT waiting for the search
 * index or writing the state file. Split out from `ingestMcps` so batch
 * seeding (`seedAllFeaturesForCi`) can ingest many features back-to-back and
 * pay the index-catch-up wait exactly once for the whole batch, instead
 * of once per feature.
 *
 * @param throwOnFailure - When true (default), throws if any entity fails.
 *   Set to false for optional/global data where partial ingestion is acceptable.
 * @returns the number of MCPs ingested, for the caller to use when writing state.
 */
async function ingestMcpsNoWait(
  featureName: string,
  gmsToken: string,
  gmsBaseUrl: string,
  logger: DataHubLogger,
  explicitDataFile?: string,
  throwOnFailure = true,
): Promise<number> {
  const dataFile = explicitDataFile ?? dataFilePath(featureName);
  if (!fs.existsSync(dataFile)) {
    throw new Error(`Seed data file not found: ${dataFile}\n` + `Expected: tests/${featureName}/fixtures/data.json`);
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

      // Normalise the MCP: strip explicit nulls (RestLi rejects null for optional fields)
      // and convert "aspect.json: {...}" shorthand to the required GenericAspect format.
      const normalized = normalizeMcp(mcp);

      // Legacy snapshot format uses /entities?action=ingest;
      // new MCP format (with entityUrn but no proposedSnapshot) uses /aspects?action=ingestProposal.
      const response = normalized.proposedSnapshot
        ? await apiContext.post(`${gmsBaseUrl}/entities?action=ingest`, {
            data: { entity: { value: normalized.proposedSnapshot } },
            failOnStatusCode: false,
          })
        : await apiContext.post(`${gmsBaseUrl}/aspects?action=ingestProposal`, {
            data: { proposal: normalized },
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

    // Second pass: re-ingest aspects that /entities?action=ingest silently drops
    // (Avro union types, null optional fields, enum union formats). extractComplexAspects
    // pulls only the affected aspects from the already-parsed MCPs and ingestComplexAspects
    // posts them via /aspects?action=ingestProposal which accepts them correctly.
    const complexAspects = extractComplexAspects(mcps);
    if (complexAspects.length > 0) {
      logger.info(`re-ingesting ${complexAspects.length} complex aspects via MCP endpoint`);
      await ingestComplexAspects(apiContext, gmsToken, complexAspects, logger);
    }

    return mcps.length;
  } finally {
    await apiContext.dispose();
  }
}

/**
 * Ingest a single feature's MCPs, then wait for the search index and write
 * its state file. This is the original single-feature seeding path (local
 * dev, and the CI per-feature short-circuit after the batch below) —
 * `ingestMcpsNoWait` + `waitForIndexCatchUp` + `writeSeedState` composed for
 * the one-feature-at-a-time case.
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
  const entityCount = await ingestMcpsNoWait(
    featureName,
    gmsToken,
    gmsBaseUrl,
    logger,
    explicitDataFile,
    throwOnFailure,
  );
  // Wait for the search index to catch up BEFORE marking this seed complete.
  // The state file's mere existence is the fixture's signal that data is safe
  // to read; writing it any earlier let workers that reuse it proceed against
  // not-yet-indexed data.
  await waitForIndexCatchUp(logger);
  // Write state file so other workers (and next runs) skip re-seeding.
  // Written even on partial failures (when throwOnFailure=false) so we don't retry endlessly.
  writeSeedState(featureName, entityCount);
  logger.info('state saved', { featureName });
}

/** Marker artifact for the CI batch-seed-everything path (see module docstring). */
const ALL_FEATURES_MARKER_FILE = path.join(SEEDED_DIR, '_all-features.json');

/** Feature directories under `tests/` that have their own `fixtures/data.json`. */
function discoverFeatureNames(): string[] {
  return fs
    .readdirSync(TESTS_DIR, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .filter((name) => fs.existsSync(dataFilePath(name)));
}

/**
 * CI-only: seed every feature's fixture data in one batch, guarded by a
 * single withArtifactLock so only one worker per shard performs it (see
 * module docstring for why CI needs this instead of per-feature lazy
 * seeding). Ingests every not-yet-seeded feature back-to-back with no
 * per-feature wait, pays the (longer, batch-sized) search/graph-index wait
 * exactly once for the whole batch, then writes each feature's state file
 * before writing the marker file itself.
 */
async function seedAllFeaturesForCi(gmsToken: string, gmsBaseUrl: string, logger: DataHubLogger): Promise<void> {
  await withArtifactLock(ALL_FEATURES_MARKER_FILE, async () => {
    const featureNames = discoverFeatureNames();
    const pending = featureNames.filter((name) => !fs.existsSync(stateFilePath(name)));

    if (pending.length > 0) {
      logger.info(`CI global seeding: batch-ingesting ${pending.length} feature(s)`, { features: pending });

      const entityCounts: Array<[string, number]> = [];
      for (const featureName of pending) {
        // throwOnFailure=false: one bad feature's fixture shouldn't block seeding the rest
        // for the whole shard — mirrors GLOBAL_DATA_FILE's non-blocking ingest below.
        const entityCount = await ingestMcpsNoWait(featureName, gmsToken, gmsBaseUrl, logger, undefined, false);
        entityCounts.push([featureName, entityCount]);
      }

      await waitForIndexCatchUp(logger, BATCH_INDEX_CATCH_UP_MS);

      for (const [featureName, entityCount] of entityCounts) {
        writeSeedState(featureName, entityCount);
      }
    }

    writeJsonAtomic(ALL_FEATURES_MARKER_FILE, {
      seededAt: new Date().toISOString(),
      featureCount: featureNames.length,
    });
    logger.info('CI global seeding complete', { featureCount: featureNames.length });
  });
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

      // Each artifact below is guarded by withArtifactLock: exactly one worker
      // produces it (ingest + wait for the search index) while any concurrent
      // workers_per_shard > 1 siblings poll for it instead of duplicating the
      // ingestion.
      const tokenFile = gmsTokenPath(user.username);
      await withArtifactLock(tokenFile, () => bootstrapGmsToken(browser, user).then(() => undefined));
      const gmsToken = readGmsToken(user.username);

      // Always seed global shared data (test-data/data.json) once per run.
      if (fs.existsSync(GLOBAL_DATA_FILE)) {
        const globalStateFile = stateFilePath(GLOBAL_FEATURE_NAME);
        await withArtifactLock(globalStateFile, () =>
          // throwOnFailure=false: global data has mixed-format MCPs; partial failures are non-blocking.
          ingestMcps(GLOBAL_FEATURE_NAME, gmsToken, gmsUrl(), logger, GLOBAL_DATA_FILE, false),
        );
        const state = JSON.parse(fs.readFileSync(globalStateFile, 'utf-8')) as SeedState;
        logger.info('global data ready', { seededAt: state.seededAt, entityCount: state.entityCount });
      }

      // CI: seed every feature up front, regardless of this suite's own featureName —
      // shard placement can no longer be relied on to co-locate a test with the feature
      // data it needs (see module docstring). Local dev never takes this branch.
      if (process.env.CI === 'true') {
        await seedAllFeaturesForCi(gmsToken, gmsUrl(), logger);
      }

      if (!featureName) {
        await use();
        return;
      }

      // In CI this is a no-op short-circuit: seedAllFeaturesForCi already wrote
      // this feature's state file, so withArtifactLock's existence check returns
      // immediately without re-ingesting.
      const stateFile = stateFilePath(featureName);
      await withArtifactLock(stateFile, () => ingestMcps(featureName, gmsToken, gmsUrl(), logger));
      const state = JSON.parse(fs.readFileSync(stateFile, 'utf-8')) as SeedState;
      logger.info('feature data ready', { featureName, seededAt: state.seededAt, entityCount: state.entityCount });

      await use();
    },
    // Local: worst case one worker produces token + global + one feature, each with
    // ingest + a 15s ES index wait under withArtifactLock. CI: worst case one worker
    // also produces the full batch seed of every feature (one shared, longer wait — see
    // BATCH_INDEX_CATCH_UP_MS) before its own feature's now-instant short-circuit.
    { auto: true, scope: 'worker', timeout: SEED_TIMEOUT_MS },
  ],
});
