/**
 * Container filter options whose names repeat (a "store" schema in two databases) must be told apart
 * in the value menu by the fewest parent containers that separate them; unique names stay plain. Long
 * paths collapse their middle levels to an ellipsis that shows the hidden levels on hover.
 */

import { expect, Page } from '@playwright/test';

import { test } from '../../fixtures/base-test';
import { SearchPage } from '../../pages/search.page';
import { GLOBAL_FEATURE_FLAGS } from '../../utils/test-feature-flags';
import { DomainTag } from '../../utils/test-tags';

test.use({ featureName: 'search-container-filter' });

const INVENTORY_CONTAINER = 'urn:li:container:pw_cfd_east_inventory';

const option = (page: Page, name: RegExp) => page.getByTestId('filter-dropdown').getByRole('checkbox', { name });

test.describe('Container filter disambiguation', () => {
  test.beforeEach(async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags(GLOBAL_FEATURE_FLAGS);
    const searchPage = new SearchPage(page, logger, logDir);
    await searchPage.navigateToHome();
    await page.goto(
      `/search?query=kibblecart&filter_container___false___EQUAL___0=${encodeURIComponent(INVENTORY_CONTAINER)}`,
    );
    await page.getByTestId(`active-filter-value-container-${INVENTORY_CONTAINER}`).click();
    await expect(option(page, /inventory/)).toBeVisible();
  });

  test('same-named containers show the parent that separates them', { tag: [DomainTag.Catalog] }, async ({ page }) => {
    await expect(option(page, /petshop_east\.store/)).toHaveCount(1);
    await expect(option(page, /petshop_west\.store/)).toHaveCount(1);
  });

  test(
    'containers that share a parent name show the level that separates them',
    { tag: [DomainTag.Catalog] },
    async ({ page }) => {
      await expect(option(page, /lakehouse_a\.\u2026\.supply/)).toHaveCount(1);
      await expect(option(page, /lakehouse_b\.\u2026\.supply/)).toHaveCount(1);
    },
  );

  test('a container with a unique name keeps its plain name', { tag: [DomainTag.Catalog] }, async ({ page }) => {
    await expect(option(page, /inventory/)).toHaveCount(1);
    await expect(option(page, /petshop_east\.inventory/)).toHaveCount(0);
  });

  test(
    'hovering a collapsed path shows the levels it hides, and the full path elsewhere',
    { tag: [DomainTag.Catalog] },
    async ({ page }) => {
      const path = page.getByTestId('filter-option-container-path').filter({ hasText: 'lakehouse_a' });

      await path.getByTestId('filter-option-container-path-elided').hover();
      await expect(page.getByRole('tooltip')).toHaveText('petshop_central');

      await path.getByText('supply').hover();
      await expect(page.getByRole('tooltip')).toHaveText('lakehouse_a.petshop_central.supply');
    },
  );
});
