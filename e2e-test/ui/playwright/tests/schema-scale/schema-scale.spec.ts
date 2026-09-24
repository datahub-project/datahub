/**
 * Schema tab scale smoke test.
 *
 * Runs against a large seeded dataset (helpers/seeders/large-dataset-seeder.ts; default
 * 1000 columns, each with a description, 5 glossary terms and 2 structured properties)
 * and measures the four interactions a user actually feels on a wide schema:
 *
 *   1. first rows painted after navigation            < 10 s
 *   2. every mounted description filled in            < 15 s   (from navigation)
 *   3. jump two thirds down: rows + descriptions      <  5 s   (from the scroll)
 *   4. open a column: its properties in the drawer    <  3 s   (from the click, incl. the Properties tab)
 *
 * Budgets are wall-clock and machine dependent; SCALE_BUDGET_FACTOR (default 1) scales
 * all four for slower runners. Measured timings are attached to the report as JSON.
 */
import { test, expect } from '../../fixtures/base-test';
import { SchemaScalePage } from '../../pages/schema-scale.page';
import { gmsUrl } from '../../utils/constants';
import { buildLargeDataset, ensureLargeDataset, type LargeDataset } from '../../helpers/seeders/large-dataset-seeder';

const FACTOR = Number(process.env.SCALE_BUDGET_FACTOR ?? 1);
const BUDGET_MS = {
  firstRows: 10_000 * FACTOR,
  firstPageDescriptions: 15_000 * FACTOR,
  scrolledViewport: 5_000 * FACTOR,
  drawerProperties: 3_000 * FACTOR,
} as const;
// Keep waiting well past each budget so a slow run reports how slow it actually was,
// instead of a bare timeout; the budget assertion afterwards still fails the test.
const GRACE = 4;

// 1000 columns x 2 structured properties is enough to exercise the UI paths; wider
// datasets mainly stress GMS memory instead. Override with SCALE_COLUMNS / SCALE_PROPERTIES.
const COLUMNS = Number(process.env.SCALE_COLUMNS ?? 1000);
const PROPERTIES_PER_FIELD = Number(process.env.SCALE_PROPERTIES ?? 2);
const DATASET_OPTIONS = { columns: COLUMNS, propertiesPerField: PROPERTIES_PER_FIELD };
// Pure description of the dataset, so the test knows which columns and values to expect.
const fixture: LargeDataset = buildLargeDataset(DATASET_OPTIONS);

test.describe('Schema tab at scale', () => {
  test.describe.configure({ mode: 'serial' });
  // Seeding a wide dataset plus four measured phases, each allowed to overrun its budget.
  test.setTimeout(4 * 60_000);

  test.beforeEach(async ({ playwright, gmsToken, logger }) => {
    const request = await playwright.request.newContext();
    try {
      const { seeded, seedMs } = await ensureLargeDataset(request, gmsUrl(), gmsToken, DATASET_OPTIONS);
      logger.info(seeded ? 'schema-scale: seeded large dataset' : 'schema-scale: large dataset already present', {
        columns: COLUMNS,
        seedMs,
      });
    } finally {
      await request.dispose();
    }
  });

  test(`renders, scrolls and opens a ${COLUMNS}-column schema within budget`, async ({
    page,
    logger,
    logDir,
  }, testInfo) => {
    const schema = new SchemaScalePage(page, logger, logDir);
    const timings: Record<string, number> = {};
    const firstField = fixture.fields[0];

    // 1. Navigation → first rows painted (via the lean structural query, not the full one).
    await test.step('first rows painted', async () => {
      const sawStructural = schema.waitForOperation('getDatasetSchemaStructural', BUDGET_MS.firstRows * GRACE);
      const t0 = Date.now();
      await schema.gotoSchemaTab(fixture.datasetUrn);
      await sawStructural;
      await expect(schema.row(firstField.fieldPath)).toBeVisible({ timeout: BUDGET_MS.firstRows * GRACE });
      timings.firstRowsMs = Date.now() - t0;
      timings.navigationStartedAt = t0;
      logger.info('schema-scale: first rows', { ms: timings.firstRowsMs });
      expect.soft(timings.firstRowsMs, 'first rows painted').toBeLessThan(BUDGET_MS.firstRows);

      // The tab badge already knows the full count, and the table is virtualised, not paged.
      await expect(schema.tabHeader).toContainText(String(COLUMNS));
      await expect(schema.paginationControls).toHaveCount(0);
    });

    // 2. Phase 2 metadata: every mounted row has its description, no skeletons left.
    await test.step('first page descriptions visible', async () => {
      await expect
        .poll(async () => schema.mountedRows(), {
          timeout: BUDGET_MS.firstPageDescriptions * GRACE,
          intervals: [100, 250],
        })
        .toMatchObject({ missingDescriptions: [], skeletons: 0 });
      timings.firstPageDescriptionsMs = Date.now() - timings.navigationStartedAt;
      logger.info('schema-scale: first page descriptions', { ms: timings.firstPageDescriptionsMs });

      // The text is the seeded description, not a placeholder.
      await expect(schema.description(firstField.fieldPath)).toContainText(firstField.description.slice(0, 40));
      expect
        .soft(timings.firstPageDescriptionsMs, 'first page descriptions')
        .toBeLessThan(BUDGET_MS.firstPageDescriptions);
    });

    // 3. Jump two thirds of the way down; the viewport must be re-rendered with metadata.
    const targetIndex = Math.floor((fixture.fields.length * 2) / 3);
    let clickedField = fixture.fields[targetIndex];
    await test.step('two-thirds scroll re-renders the viewport', async () => {
      const t0 = Date.now();
      await schema.scrollToFraction(2 / 3);

      // Rows within a viewport of the target index must be mounted, with descriptions.
      const nearTarget = new Set(
        fixture.fields.slice(Math.max(0, targetIndex - 40), targetIndex + 40).map((f) => f.fieldPath),
      );
      let mounted = await schema.mountedRows();
      await expect
        .poll(
          async () => {
            mounted = await schema.mountedRows();
            const inRange = mounted.fieldPaths.some((fp) => nearTarget.has(fp));
            return inRange && mounted.missingDescriptions.length === 0 && mounted.skeletons === 0;
          },
          { timeout: BUDGET_MS.scrolledViewport * GRACE, intervals: [50, 100, 250] },
        )
        .toBe(true);
      timings.scrolledViewportMs = Date.now() - t0;
      logger.info('schema-scale: scrolled viewport', {
        ms: timings.scrolledViewportMs,
        mountedRows: mounted.fieldPaths.length,
        firstMounted: mounted.fieldPaths[0],
      });

      // Virtualisation proof: the first row has been unmounted, not just scrolled off.
      await expect(schema.row(firstField.fieldPath)).toHaveCount(0);

      // Click a near-target row from the middle of the mounted window, i.e. one that is
      // actually on screen rather than at the edge of the virtualiser's overscan.
      const candidates = mounted.fieldPaths.filter((fp) => nearTarget.has(fp));
      const onScreen = candidates[Math.floor(candidates.length / 2)];
      expect(onScreen).toBeDefined();
      clickedField = fixture.fields.find((f) => f.fieldPath === onScreen)!;
      await expect(schema.description(clickedField.fieldPath)).toContainText(clickedField.description.slice(0, 40));
      expect.soft(timings.scrolledViewportMs, 'scrolled viewport').toBeLessThan(BUDGET_MS.scrolledViewport);
    });

    // 4. Open the column: its structured properties show in the drawer.
    await test.step('field drawer shows the structured properties', async () => {
      const t0 = Date.now();
      await schema.clickRow(clickedField.fieldPath, BUDGET_MS.drawerProperties * GRACE);
      await expect(schema.fieldDrawer).toBeVisible({ timeout: BUDGET_MS.drawerProperties * GRACE });
      await schema.openDrawerPropertiesTab(BUDGET_MS.drawerProperties * GRACE);
      const namespaces = new Set(clickedField.properties.map((p) => p.property.qualifiedName.split('.')[0]));
      for (const namespace of namespaces) {
        await schema.expandDrawerPropertyGroup(namespace, BUDGET_MS.drawerProperties * GRACE);
      }
      for (const { property, value } of clickedField.properties) {
        await expect(schema.drawerPropertyRow(property.displayName)).toContainText(value, {
          timeout: BUDGET_MS.drawerProperties * GRACE,
        });
      }
      timings.drawerPropertiesMs = Date.now() - t0;
      logger.info('schema-scale: drawer properties', {
        ms: timings.drawerPropertiesMs,
        field: clickedField.fieldPath,
        properties: clickedField.properties.map((p) => p.property.displayName),
      });
      expect.soft(timings.drawerPropertiesMs, 'drawer properties').toBeLessThan(BUDGET_MS.drawerProperties);
    });

    await testInfo.attach('schema-scale-timings.json', {
      body: JSON.stringify(
        { columns: fixture.fields.length, budgetFactor: FACTOR, budgetsMs: BUDGET_MS, ...timings },
        null,
        2,
      ),
      contentType: 'application/json',
    });
  });

  test('Phase 2 failure keeps the structural rows, shows the banner, and Retry recovers', async ({
    page,
    logger,
    logDir,
  }) => {
    const schema = new SchemaScalePage(page, logger, logDir);
    const firstField = fixture.fields[0];

    // Fail only the full-metadata query; the structural query goes through.
    let blockFull = true;
    const unblock = await schema.blockOperation('getDatasetSchema', () => blockFull);

    await schema.gotoSchemaTab(fixture.datasetUrn);

    // Structural rows render despite the failure: no endless skeletons.
    await expect(schema.row(firstField.fieldPath)).toBeVisible({ timeout: BUDGET_MS.firstRows * GRACE });
    await expect(schema.metadataErrorBanner).toBeVisible({ timeout: BUDGET_MS.firstPageDescriptions * GRACE });
    await expect(schema.retryButton()).toBeVisible();
    await expect.poll(async () => (await schema.mountedRows()).skeletons).toBe(0);

    // Clear the fault and retry: banner gone, descriptions filled in.
    blockFull = false;
    await schema.retryButton().click();
    await expect(schema.metadataErrorBanner).toHaveCount(0, { timeout: BUDGET_MS.firstPageDescriptions * GRACE });
    await expect
      .poll(async () => schema.mountedRows(), { timeout: BUDGET_MS.firstPageDescriptions * GRACE })
      .toMatchObject({ missingDescriptions: [], skeletons: 0 });
    await expect(schema.description(firstField.fieldPath)).toContainText(firstField.description.slice(0, 40));
    await unblock();
  });
});
