/**
 * Logger fixture — auto-use structured logging for every test.
 *
 * Provides two fixtures, both with autoUse: true so they are always
 * initialised without needing to be listed in a test's parameter list:
 *
 *   logDir  — Absolute path unique to each test, derived from testInfo.outputDir.
 *              Also used by BasePage.screenshot() so logs and screenshots land
 *              in the same per-test folder.
 *
 *   logger  — Winston-backed DataHubLogger. Logs to console always; writes
 *              test.log + error.log inside logDir when running in CI.
 *              Depends on logDir so there is a single configuration point.
 *
 * Usage in page objects or fixtures:
 *   import type { DataHubLogger } from '../utils/logger';
 *
 * Usage in tests (fixture is auto-injected, no import needed beyond the
 * base-test that already merges loggerFixture):
 *   test('example', async ({ logger }) => { logger.info('hello'); });
 */

import * as path from 'path';
import { test as base } from '@playwright/test';
import { createLogger, type DataHubLogger } from '../utils/logger';

// ── Fixture types ─────────────────────────────────────────────────────────────

type LoggerFixtures = {
  /** Per-test log/screenshot directory derived from testInfo.outputDir. */
  logDir: string;
  /** Winston-backed structured logger. Auto-injected into every test. */
  logger: DataHubLogger;
};

// ── Fixture implementations ───────────────────────────────────────────────────

export const loggerFixture = base.extend<LoggerFixtures>({
  // logDir: unique output directory that Playwright already creates per test.
  // We extend it with a 'logs' sub-folder to keep log files separate from
  // trace/video artifacts that Playwright stores at the root.
  logDir: [
    async ({}, use, testInfo) => {
      const logDir = path.join(testInfo.outputDir, 'logs');
      await use(logDir);
    },
    { auto: true },
  ],

  // logger: built once per test, automatically closed after the test finishes.
  logger: [
    async ({ logDir }, use, testInfo) => {
      const testMeta = {
        suite: testInfo.titlePath.slice(0, -1).join(' > '),
        test: testInfo.title,
        worker: testInfo.workerIndex,
        retry: testInfo.retry,
      };
      const logger = createLogger(logDir, testMeta);
      logger.info('test started');
      await use(logger);
      logger.info('test finished', { status: testInfo.status });
    },
    { auto: true },
  ],
});

export type { DataHubLogger };
