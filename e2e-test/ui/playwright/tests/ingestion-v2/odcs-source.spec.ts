import { OdcsSource } from '@pages/ingestion/base/sources/OdcsSource';
import { test } from '../../fixtures/base-test';
import { IngestionV2Page } from '../../pages/ingestion/v2/ingestion-v2.page';

test.use({ featureName: 'ingestion-v2' });

test.describe('odcs ingestion source', () => {
  let ingestionPage: IngestionV2Page;

  test.beforeEach(async ({ page, apiMock, logger, logDir }) => {
    ingestionPage = new IngestionV2Page(page, logger, logDir);

    await apiMock.setFeatureFlags({
      showIngestionPageRedesign: true,
      ingestionOnboardingRedesignV1: false,
      showNavBarRedesign: true,
    });

    await ingestionPage.goto();
  });

  test('is selectable in the source picker and produces an odcs recipe', async ({ page, logger, logDir }) => {
    const odcs = new OdcsSource(page, logger, logDir);

    await ingestionPage.sourcesTab.openCreateSourceModal();
    // selectSourceType fails if the ODCS option is absent from the picker, which
    // guards the sources.json entry + logo wiring.
    await ingestionPage.sourcesTab.selectSourceType('Open Data Contract Standard');

    // The structured Path field must write back into the generated recipe.
    await odcs.fillPath('/tmp/contracts');
    await odcs.expectYamlRecipe(['type: odcs', 'path: /tmp/contracts']);

    await ingestionPage.sourcesTab.cancelCreateSourceModal();
  });

  test('accepts a remote object-store URI as the path', async ({ page, logger, logDir }) => {
    const odcs = new OdcsSource(page, logger, logDir);

    await ingestionPage.sourcesTab.openCreateSourceModal();
    await ingestionPage.sourcesTab.selectSourceType('Open Data Contract Standard');

    // The Path field now documents and accepts remote sources; an s3:// glob must
    // survive into the generated recipe unchanged.
    await odcs.fillPath('s3://my-bucket/contracts/*.odcs.yaml');
    await odcs.expectYamlRecipe(['type: odcs', 'path: s3://my-bucket/contracts/*.odcs.yaml']);

    await ingestionPage.sourcesTab.cancelCreateSourceModal();
  });
});
