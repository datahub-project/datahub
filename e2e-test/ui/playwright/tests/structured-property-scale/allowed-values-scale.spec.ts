/**
 * Structured property allowed-values scale smoke test.
 *
 * Runs against a seeded glossary term (helpers/seeders/allowed-values-seeder.ts) that carries a
 * single-select string structured property whose definition lists a large number of allowed
 * values (default 5000), and measures what a user feels on such a term:
 *
 *   1. term page painted after navigation                        < 10 s
 *   2. Properties tab shows the assigned value                   < 10 s   (from navigation)
 *   3. Edit the value: dropdown open, filtered, option visible   <  5 s   (from the Edit click)
 *
 * Budgets are wall-clock and machine dependent; SCALE_BUDGET_FACTOR (default 1) scales all three
 * for slower runners. Measured timings are attached to the report as JSON.
 */
import { test, expect } from '../../fixtures/base-test';
import { GlossaryTermScalePage } from '../../pages/glossary-term-scale.page';
import { gmsUrl } from '../../utils/constants';
import {
  buildAllowedValuesFixture,
  ensureAllowedValuesFixture,
  type AllowedValuesFixture,
} from '../../helpers/seeders/allowed-values-seeder';

const FACTOR = Number(process.env.SCALE_BUDGET_FACTOR ?? 1);
const BUDGET_MS = {
  pagePainted: 10_000 * FACTOR,
  propertyValue: 10_000 * FACTOR,
  editDropdown: 5_000 * FACTOR,
} as const;
// Keep waiting well past each budget so a slow run reports how slow it actually was,
// instead of a bare timeout; the budget assertion afterwards still fails the test.
const GRACE = 4;

// Override with SCALE_ALLOWED_VALUES to bisect where the page falls over.
const VALUES = Number(process.env.SCALE_ALLOWED_VALUES ?? 5000);
const OPTIONS = { values: VALUES };
// Pure description of the fixture, so the test knows which values to expect.
const fixture: AllowedValuesFixture = buildAllowedValuesFixture(OPTIONS);

test.describe('Structured property with many allowed values', () => {
  test.setTimeout(3 * 60_000);

  test(`loads a glossary term whose property has ${VALUES} allowed values within budget`, async ({
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
        const { seeded, seedMs } = await ensureAllowedValuesFixture(request, gmsUrl(), gmsToken, OPTIONS);
        logger.info(seeded ? 'allowed-values-scale: seeded fixture' : 'allowed-values-scale: fixture already present', {
          values: VALUES,
          seedMs,
        });
      } finally {
        await request.dispose();
      }
    });

    // 1. Navigation → header painted.
    await test.step('term page painted', async () => {
      const t0 = Date.now();
      await term.gotoTerm(fixture.termUrn);
      // The header truncates long names with an ellipsis, so match on a prefix.
      await expect(term.entityHeader).toContainText(fixture.termName.slice(0, 20), {
        timeout: BUDGET_MS.pagePainted * GRACE,
      });
      timings.pagePaintedMs = Date.now() - t0;
      timings.navigationStartedAt = t0;
      logger.info('allowed-values-scale: page painted', { ms: timings.pagePaintedMs });
      expect.soft(timings.pagePaintedMs, 'term page painted').toBeLessThan(BUDGET_MS.pagePainted);
    });

    // 2. Properties tab shows the property and its assigned value.
    await test.step('Properties tab shows the assigned value', async () => {
      await term.openPropertiesTab(BUDGET_MS.propertyValue * GRACE);
      await expect(term.propertyRow(fixture.displayName)).toContainText(fixture.assignedValue, {
        timeout: BUDGET_MS.propertyValue * GRACE,
      });
      timings.propertyValueMs = Date.now() - timings.navigationStartedAt;
      logger.info('allowed-values-scale: property value visible', { ms: timings.propertyValueMs });
      expect.soft(timings.propertyValueMs, 'property value visible').toBeLessThan(BUDGET_MS.propertyValue);
    });

    // 3. Editing the value: the allowed-values dropdown opens and filters down to a chosen option.
    const target = fixture.allowedValues[fixture.allowedValues.length - 1];
    await test.step('edit dropdown opens and filters', async () => {
      const t0 = Date.now();
      await term.openEditModal(fixture.displayName, BUDGET_MS.editDropdown * GRACE);
      await expect(term.editModal).toBeVisible({ timeout: BUDGET_MS.editDropdown * GRACE });
      const searchable = await term.searchAllowedValues(target, BUDGET_MS.editDropdown * GRACE);
      // Without search the user has to wheel through a virtualised list to reach the value.
      let ticks = 0;
      if (!searchable) {
        ticks = await term.scrollDropdownUntilVisible(term.allowedValueOption(target), BUDGET_MS.editDropdown * GRACE);
      }
      await expect(
        term.allowedValueOption(target),
        `option ${target} reachable (searchable=${searchable})`,
      ).toBeVisible({ timeout: BUDGET_MS.editDropdown * GRACE });
      timings.editDropdownMs = Date.now() - t0;
      const rendered = await term.renderedOptions().count();
      logger.info('allowed-values-scale: edit dropdown reached the value', {
        ms: timings.editDropdownMs,
        rendered,
        searchable,
        ticks,
      });
      expect.soft(timings.editDropdownMs, 'edit dropdown filtered').toBeLessThan(BUDGET_MS.editDropdown);

      // Selecting the filtered option enables Update; we do not save, so the fixture stays stable.
      await term.allowedValueOption(target).click();
      await expect(term.modalUpdateButton).toBeEnabled();
      await term.closeModal();
    });

    await testInfo.attach('allowed-values-scale-timings.json', {
      body: JSON.stringify({ values: VALUES, budgetFactor: FACTOR, budgetsMs: BUDGET_MS, ...timings }, null, 2),
      contentType: 'application/json',
    });
  });
});
