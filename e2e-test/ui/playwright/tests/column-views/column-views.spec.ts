import fs from 'fs';
import path from 'path';

import { expect, test } from '../../fixtures/base-test';
import { readGmsToken } from '../../fixtures/login';
import { GraphQLHelper } from '../../helpers/graphql-helper';
import { ColumnViewsPage } from '../../pages/column-views.page';
import { deleteEntities } from '../../utils/cleanup';
import { gmsUrl } from '../../utils/constants';
import { createScriptLogger } from '../../utils/logger';
import { sweepOrphanIndexDocuments } from '../../utils/index-consistency';
import { generateRandomString, withRandomSuffix } from '../../utils/random';

// Seeds tests/column-views/fixtures/data.json (a Hive dataset with four fields, two of them
// tagged) once per run, so the suite is self-contained wherever it runs.
const FEATURE = 'column-views';
test.use({ featureName: FEATURE });

// One worker, in order: the afterAll teardown below hard-deletes the seeded data, which is only
// safe once every test in this file has finished.
test.describe.configure({ mode: 'serial' });

/** Override to point the suite at other data (any dataset with a schema works). */
const DATASET_URN =
  process.env.COLUMN_VIEWS_DATASET_URN ||
  'urn:li:dataset:(urn:li:dataPlatform:hive,SamplePlaywrightColumnViewsDataset,PROD)';
const SEEDED_FIELDS = ['user_id', 'email', 'created_at', 'is_active'];
const SEEDED_URNS = [
  'urn:li:tag:PlaywrightColumnViews',
  DATASET_URN,
  ...SEEDED_FIELDS.map((f) => `urn:li:schemaField:(${DATASET_URN},${f})`),
];
const SEED_MARKER = path.join(__dirname, '../../.seeded', `${FEATURE}.json`);

// Headers of the built-in default layout that render for any dataset (Stats depends on usage data).
const DEFAULT_HEADERS = ['Type', 'Description', 'Tags', 'Glossary Terms'];

/** Everything a test created and handed to the `cleanup` fixture; re-checked against the index in afterAll. */
const createdUrns: string[] = [];
let anyFailed = false;

async function waitForIndexed(graphql: GraphQLHelper, urn: string, type: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const res = (await graphql.executeQuery(
      `query indexed($input: SearchAcrossEntitiesInput!) {
         searchAcrossEntities(input: $input) { searchResults { entity { urn } } }
       }`,
      {
        input: {
          types: [type],
          query: '*',
          start: 0,
          count: 1,
          orFilters: [{ and: [{ field: 'urn', values: [urn] }] }],
        },
      },
    )) as { data?: { searchAcrossEntities?: { searchResults: { entity: { urn: string } }[] } } };
    if (res.data?.searchAcrossEntities?.searchResults.some((r) => r.entity.urn === urn)) return;
    if (Date.now() > deadline) throw new Error(`${urn} did not appear in the search index within ${timeoutMs}ms`);
    await new Promise((r) => setTimeout(r, 1_000));
  }
}

test.describe('Column Views on the Schema tab', () => {
  let columnViews: ColumnViewsPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    columnViews = new ColumnViewsPage(page, logger, logDir);
    await columnViews.navigateToSchema(DATASET_URN);
  });

  test.afterEach(async ({}, testInfo) => {
    if (testInfo.status !== 'passed' && testInfo.status !== 'skipped') anyFailed = true;
  });

  // Hard-delete teardown: the seeded tag / dataset / schema fields via the entity API, then the
  // operations consistency API removes any search-index document that outlived a hard delete
  // (ours or the cleanup fixture's). Skipped when a test failed so the broken state stays
  // inspectable (same policy as the `cleanup` fixture).
  test.afterAll(async ({ playwright, user }) => {
    const logger = createScriptLogger(`${FEATURE}-teardown`);
    if (anyFailed) {
      logger.warn('a test failed — preserving seeded data for investigation', { urns: SEEDED_URNS.join(', ') });
      return;
    }
    const request = await playwright.request.newContext({
      extraHTTPHeaders: { Authorization: `Bearer ${readGmsToken(user.username)}` },
    });
    try {
      await deleteEntities(request, gmsUrl(), SEEDED_URNS, logger);
      const sweep = await sweepOrphanIndexDocuments(request, gmsUrl(), [...SEEDED_URNS, ...createdUrns], {
        sinceEpochMs: Date.now() - 24 * 60 * 60 * 1000, // seeds may predate this process
        logger,
      });
      expect(sweep.failures, 'orphan index documents that could not be removed').toEqual([]);
    } finally {
      await request.dispose();
    }
    if (fs.existsSync(SEED_MARKER)) fs.unlinkSync(SEED_MARKER); // next run re-seeds
  });

  test('default layout shows the legacy columns behind "Columns: Default"', async () => {
    await columnViews.expectActiveViewLabel('Default');
    for (const header of DEFAULT_HEADERS) {
      await expect(columnViews.header(header)).toBeVisible();
    }
  });

  test('ad hoc toggle hides a column, marks the layout modified, and Reset restores it', async () => {
    await columnViews.openPopover();
    await columnViews.toggleColumn('Tags');

    await expect(columnViews.header('Tags')).toBeHidden();
    await expect(columnViews.header('Glossary Terms')).toBeVisible();
    await columnViews.expectActiveViewLabel('Default', true);

    // Field-attribute columns the legacy table never had are offered for toggling too.
    for (const optional of ['Native type', 'Nullable', 'Primary Key']) {
      await columnViews.toggleColumn(optional);
      await expect(columnViews.header(optional)).toBeVisible();
    }

    await columnViews.resetButton.click();
    await expect(columnViews.header('Tags')).toBeVisible();
    await expect(columnViews.header('Native type')).toBeHidden();
    await columnViews.expectActiveViewLabel('Default');
  });

  test('save the ad hoc layout as a personal Column View; it survives a reload', async ({ page, cleanup }) => {
    const viewName = withRandomSuffix('Column View');

    await columnViews.openPopover();
    await columnViews.toggleColumn('Tags');
    const urn = await columnViews.saveAsColumnView(viewName);

    // Hard-deleted by the cleanup fixture after the test (kept if the test fails).
    cleanup.track(urn);
    createdUrns.push(urn);

    // The saved view is active, unmodified, and listed under MY COLUMN VIEWS.
    await columnViews.expectActiveViewLabel(viewName);
    await expect(columnViews.header('Tags')).toBeHidden();
    await columnViews.openPopover();
    await expect(columnViews.savedViewLink(viewName)).toBeVisible();

    // Selection is remembered per browser (localStorage), so a reload keeps it.
    await page.reload();
    await columnViews.expectActiveViewLabel(viewName);
    await expect(columnViews.header('Tags')).toBeHidden();

    // Leave the next test on the built-in default.
    await columnViews.selectBuiltInDefault();
  });

  test('a structured property added through the picker shows its display name, not its urn', async ({
    page,
    cleanup,
  }) => {
    const graphql = new GraphQLHelper(page);
    // Random affix on the qualified name: a property's search-index field is derived from it and
    // can never be re-created once indexed, so test properties must never reuse a name.
    const affix = generateRandomString();
    const displayName = `E2E Classification ${affix}`;
    const res = (await graphql.executeQuery(
      `mutation create($input: CreateStructuredPropertyInput!) {
         createStructuredProperty(input: $input) { urn }
       }`,
      {
        input: {
          id: `columnviews.e2e_${affix}`,
          qualifiedName: `columnviews.e2e_${affix}`,
          displayName,
          description: 'Created by the Column Views e2e suite',
          valueType: 'urn:li:dataType:datahub.string',
          cardinality: 'SINGLE',
          entityTypes: ['urn:li:entityType:datahub.schemaField', 'urn:li:entityType:datahub.dataset'],
        },
      },
    )) as { data?: { createStructuredProperty?: { urn: string } }; errors?: unknown[] };
    const propertyUrn = res.data?.createStructuredProperty?.urn;
    expect(propertyUrn, JSON.stringify(res.errors)).toBeTruthy();
    cleanup.track(propertyUrn as string); // hard-deleted after the test
    createdUrns.push(propertyUrn as string);
    await waitForIndexed(graphql, propertyUrn as string, 'STRUCTURED_PROPERTY');

    await columnViews.openPopover();
    await columnViews.addStructuredPropertyColumn(displayName);

    // Column header and chooser checkbox both use the friendly name; the urn is hover detail only.
    await expect(columnViews.header(displayName)).toBeVisible();
    const checkbox = columnViews.columnToggle(displayName);
    await expect(checkbox).toBeVisible();
    await expect(checkbox).not.toContainText('urn:li:');
    await columnViews.expectActiveViewLabel('Default', true);

    await columnViews.resetButton.click();
    await expect(columnViews.header(displayName)).toBeHidden();

    // GMS refuses to hard-delete a structured property that is not soft-deleted first (a hard
    // delete burns the property's index mapping name), so soft-delete here; the cleanup fixture's
    // hard delete then goes through. Skipped on failure along with the rest of the cleanup.
    const softDeleted = (await graphql.executeQuery(
      `mutation softDelete($input: BatchUpdateSoftDeletedInput!) { batchUpdateSoftDeleted(input: $input) }`,
      { input: { urns: [propertyUrn], deleted: true } },
    )) as { data?: { batchUpdateSoftDeleted?: boolean }; errors?: unknown[] };
    expect(softDeleted.data?.batchUpdateSoftDeleted, JSON.stringify(softDeleted.errors)).toBe(true);
  });
});
