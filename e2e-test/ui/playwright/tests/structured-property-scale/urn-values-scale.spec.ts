/**
 * Structured property URN-values scale smoke test.
 *
 * Runs against a seeded glossary term (helpers/seeders/urn-values-seeder.ts) that carries a
 * multi-valued string structured property, shown in the asset summary sidebar, whose values are
 * the URNs of N other glossary terms (default 5000). It is a plain string property with no
 * allowed-values constraint, a common shape in the wild, and the spec measures what a user feels
 * when opening such a term with the sidebar visible:
 *
 *   1. term page painted after navigation                         < 10 s
 *   2. the property's first value shows in the sidebar             < 10 s   (from navigation)
 *   3. GraphQL traffic settles (nothing in flight for 1 s)         < 10 s   (from navigation)
 *   4. no single GraphQL response is larger than                    5 MB
 *   5. filtering the sidebar list by name shows the last value     <  3 s   (from typing)
 *
 * Budgets are wall-clock and machine dependent; SCALE_BUDGET_FACTOR (default 1) scales the time
 * budgets for slower runners. Every GraphQL call is attached to the report as JSON.
 */
import { test, expect } from '../../fixtures/base-test';
import { GlossaryTermScalePage } from '../../pages/glossary-term-scale.page';
import { gmsUrl } from '../../utils/constants';
import {
  buildUrnValuesFixture,
  ensureUrnValuesFixture,
  type UrnValuesFixture,
} from '../../helpers/seeders/urn-values-seeder';

const FACTOR = Number(process.env.SCALE_BUDGET_FACTOR ?? 1);
const BUDGET_MS = {
  pagePainted: 10_000 * FACTOR,
  firstValue: 10_000 * FACTOR,
  settled: 10_000 * FACTOR,
  filtered: 3_000 * FACTOR,
} as const;
const MAX_RESPONSE_BYTES = 5 * 1024 * 1024;
// Keep waiting well past each budget so a slow run reports how slow it actually was,
// instead of a bare timeout; the budget assertion afterwards still fails the test.
const GRACE = 6;
const IDLE_MS = 1_000;

// Override with SCALE_URN_VALUES to bisect where the page falls over.
const VALUES = Number(process.env.SCALE_URN_VALUES ?? 5000);
const OPTIONS = { values: VALUES };
const fixture: UrnValuesFixture = buildUrnValuesFixture(OPTIONS);

test.describe('Structured property with many URN values', () => {
  test.setTimeout(5 * 60_000);

  test(`loads a glossary term whose property holds ${VALUES} term URNs within budget`, async ({
    page,
    playwright,
    gmsToken,
    logger,
    logDir,
  }, testInfo) => {
    const term = new GlossaryTermScalePage(page, logger, logDir);
    const timings: Record<string, number> = {};

    // Seed after the login fixture has run (it is what mints the GMS token the seeder uses).
    await test.step('seed fixture', async () => {
      const request = await playwright.request.newContext();
      try {
        const { seeded, seedMs } = await ensureUrnValuesFixture(request, gmsUrl(), gmsToken, OPTIONS);
        logger.info(seeded ? 'urn-values-scale: seeded fixture' : 'urn-values-scale: fixture already present', {
          values: VALUES,
          seedMs,
        });
      } finally {
        await request.dispose();
      }
    });

    const graphql = term.recordGraphql();
    const t0 = Date.now();

    // 1. Navigation → header painted.
    await test.step('term page painted', async () => {
      await term.gotoTerm(fixture.termUrn);
      await expect(term.entityHeader).toContainText(fixture.termName.slice(0, 20), {
        timeout: BUDGET_MS.pagePainted * GRACE,
      });
      timings.pagePaintedMs = Date.now() - t0;
      logger.info('urn-values-scale: page painted', { ms: timings.pagePaintedMs });
      expect.soft(timings.pagePaintedMs, 'term page painted').toBeLessThan(BUDGET_MS.pagePainted);
    });

    // 2. The sidebar section for the property shows its first value.
    await test.step('sidebar shows the first value', async () => {
      const first = fixture.valueTerms[0];
      const section = term.sidebarSectionContent(fixture.displayName);
      await expect(section).toBeVisible({ timeout: BUDGET_MS.firstValue * GRACE });
      await expect(term.propertyValue(fixture.displayName, first.urn)).toContainText(first.name, {
        timeout: BUDGET_MS.firstValue * GRACE,
      });
      timings.firstValueMs = Date.now() - t0;
      logger.info('urn-values-scale: first value visible', { ms: timings.firstValueMs });
      expect.soft(timings.firstValueMs, 'first value visible').toBeLessThan(BUDGET_MS.firstValue);
    });

    // 3 + 4. Everything the page fires on load has come back, and nothing was enormous.
    await test.step('GraphQL traffic settles', async () => {
      await graphql.settled(IDLE_MS, BUDGET_MS.settled * GRACE);
      timings.settledMs = Date.now() - t0 - IDLE_MS;
      const largest = graphql.calls.reduce((a, b) => (b.responseBytes > a.responseBytes ? b : a));
      logger.info('urn-values-scale: graphql settled', {
        ms: timings.settledMs,
        calls: graphql.calls.length,
        totalResponseBytes: graphql.calls.reduce((n, c) => n + c.responseBytes, 0),
        largest: { operation: largest.operation, responseBytes: largest.responseBytes, durationMs: largest.durationMs },
      });
      expect.soft(timings.settledMs, 'graphql settled').toBeLessThan(BUDGET_MS.settled);
      expect
        .soft(largest.responseBytes, `largest graphql response (${largest.operation})`)
        .toBeLessThan(MAX_RESPONSE_BYTES);
    });

    // 5. Find a specific value by typing instead of paging through thousands of chips.
    const last = fixture.valueTerms[fixture.valueTerms.length - 1];
    await test.step('filter finds the last value', async () => {
      const t0 = Date.now();
      await term.filterPropertyValues(fixture.displayName, last.name, BUDGET_MS.filtered * GRACE);
      await expect(term.propertyValue(fixture.displayName, last.urn)).toContainText(last.name, {
        timeout: BUDGET_MS.filtered * GRACE,
      });
      timings.filteredMs = Date.now() - t0;
      logger.info('urn-values-scale: filter found the last value', { ms: timings.filteredMs });
      expect.soft(timings.filteredMs, 'filter found the last value').toBeLessThan(BUDGET_MS.filtered);
    });

    await testInfo.attach('urn-values-scale-timings.json', {
      body: JSON.stringify(
        { values: VALUES, budgetFactor: FACTOR, budgetsMs: BUDGET_MS, ...timings, graphql: graphql.byOperation() },
        null,
        2,
      ),
      contentType: 'application/json',
    });
  });
});
