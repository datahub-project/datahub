/**
 * Data Product Lifecycle Tests
 *
 * Covers the data product flow end to end against a real GMS:
 * - Create a data product from a domain's Data Products tab
 * - Attach an asset from the asset's sidebar, see it on the Assets tab, and remove it again
 * - Find the data product in search
 * - Delete it and confirm it is gone from search
 *
 * Seeds (tests/data-products/fixtures/data.json): the domain pw-data-product-domain and the dataset
 * pw_data_product_dataset. Each test creates its own data product with a unique name and cleans it up.
 */

import { test, expect } from '../../fixtures/base-test';
import { DataProductPage } from '../../pages/data-product.page';
import { SearchPage } from '../../pages/search.page';
import { TIMEOUTS } from '../../utils/constants';
import { withRandomSuffix } from '../../utils/random';
import { DomainTag, TestTag } from '../../utils/test-tags';

test.use({ featureName: 'data-products' });

const DOMAIN_URN = 'urn:li:domain:pw-data-product-domain';
const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,pw_data_product_dataset,PROD)';

test.describe('Data product lifecycle', () => {
  test(
    'creates a data product from a domain and shows it in the domain list',
    { tag: [TestTag.P0, DomainTag.Catalog] },
    async ({ page, logger, cleanup }) => {
      const dataProductPage = new DataProductPage(page, logger);
      const name = withRandomSuffix('PWDataProduct');

      await dataProductPage.openDomainDataProductsTab(DOMAIN_URN);
      const urn = await dataProductPage.createDataProduct(name);
      cleanup.track(urn);

      await dataProductPage.expectListedInDomain(urn);

      await dataProductPage.openProfile(urn);
      await dataProductPage.expectProfileName(name);
    },
  );

  test(
    'attaches a dataset to a data product, lists it on the Assets tab, then removes it',
    { tag: [DomainTag.Catalog] },
    async ({ page, logger, cleanup }) => {
      const dataProductPage = new DataProductPage(page, logger);
      const name = withRandomSuffix('PWAttachProduct');

      await dataProductPage.openDomainDataProductsTab(DOMAIN_URN);
      const urn = await dataProductPage.createDataProduct(name);
      cleanup.track(urn);

      await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}`);
      await page.waitForLoadState('networkidle');
      await dataProductPage.attachFromAssetSidebar(urn, name);

      await dataProductPage.expectAssetListed(urn, DATASET_URN);

      await page.goto(`/dataset/${encodeURIComponent(DATASET_URN)}`);
      await page.waitForLoadState('networkidle');
      await dataProductPage.removeFromAssetSidebar(name);

      await dataProductPage.expectAssetNotListed(urn, DATASET_URN);
    },
  );

  test(
    'finds a data product in search and no longer finds it after deletion',
    { tag: [DomainTag.Catalog] },
    async ({ page, logger, cleanup }) => {
      const dataProductPage = new DataProductPage(page, logger);
      const searchPage = new SearchPage(page, logger);
      const name = withRandomSuffix('PWSearchProduct');

      await dataProductPage.openDomainDataProductsTab(DOMAIN_URN);
      const urn = await dataProductPage.createDataProduct(name);
      cleanup.track(urn);

      // Search indexing is asynchronous; the search page reloads until the new data product is returned.
      await searchPage.navigateToHome();
      await searchPage.search(name);
      await searchPage.expectEntityInSearchResults(urn);

      await dataProductPage.openProfile(urn);
      await dataProductPage.deleteFromProfile();

      // Re-run the search until the deleted data product drops out of the results. Each search waits for
      // its own response, so an empty result set is the index's answer rather than a page still loading.
      await expect(async () => {
        await searchPage.navigateToHome();
        await searchPage.search(name);
        await expect(searchPage.getEntityPreviewLocator(urn)).toHaveCount(0);
      }).toPass({ timeout: TIMEOUTS.LONG });
    },
  );

  test('renames a data product from its profile', { tag: [DomainTag.Catalog] }, async ({ page, logger, cleanup }) => {
    const dataProductPage = new DataProductPage(page, logger);
    const name = withRandomSuffix('PWRenameProduct');
    const newName = withRandomSuffix('PWRenamedProduct');

    await dataProductPage.openDomainDataProductsTab(DOMAIN_URN);
    const urn = await dataProductPage.createDataProduct(name);
    cleanup.track(urn);

    await dataProductPage.openProfile(urn);
    await dataProductPage.renameFromProfile(newName);

    await dataProductPage.openProfile(urn);
    await dataProductPage.expectProfileName(newName);
  });
});
