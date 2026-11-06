/**
 * DataProductPage — page object for the data product lifecycle.
 *
 * Data products are created from a domain's Data Products tab, attached to assets from the
 * asset's own sidebar, and deleted from the data product profile.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { GraphQLHelper } from '../helpers/graphql-helper';
import { TIMEOUTS } from '../utils/constants';

export class DataProductPage extends BasePage {
  private readonly graphqlHelper: GraphQLHelper;

  readonly createButton: Locator;
  readonly createModal: Locator;
  readonly nameInput: Locator;
  readonly submitButton: Locator;
  readonly threeDotMenu: Locator;
  readonly entityMenuDeleteButton: Locator;
  readonly deleteConfirmButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.graphqlHelper = new GraphQLHelper(page);
    this.createButton = page.getByTestId('create-data-product-button');
    this.createModal = page.getByTestId('create-data-product-modal');
    this.nameInput = page.getByTestId('name-input');
    this.submitButton = page.getByTestId('submit-button');
    this.threeDotMenu = page.getByTestId('MoreVertOutlinedIcon');
    this.entityMenuDeleteButton = page.getByTestId('entity-menu-delete-button');
    this.deleteConfirmButton = page.getByRole('button', { name: 'Yes' });
  }

  /** The data products listed on a domain's Data Products tab. */
  async openDomainDataProductsTab(domainUrn: string): Promise<void> {
    await this.navigate(`/domain/${encodeURIComponent(domainUrn)}/Data Products`);
    await this.page.waitForLoadState('networkidle');
    await expect(this.createButton).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  /** Create a data product from the open Data Products tab and return its URN. */
  async createDataProduct(name: string): Promise<string> {
    await this.createButton.click();
    await this.nameInput.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
    await this.nameInput.fill(name);

    const responsePromise = this.graphqlHelper.waitForGraphQLResponse('createDataProduct');
    await this.submitButton.click();
    const response = await responsePromise;

    const created = (response.data as Record<string, { urn?: string }>).createDataProduct;
    if (!created?.urn) {
      throw new Error(`createDataProduct returned no URN: ${JSON.stringify(response)}`);
    }
    return created.urn;
  }

  async openProfile(dataProductUrn: string, tab?: string): Promise<void> {
    const suffix = tab ? `/${tab}` : '';
    await this.navigate(`/dataProduct/${encodeURIComponent(dataProductUrn)}${suffix}`);
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Attach the open asset to a data product from its sidebar. The section is titled "Data Product" or
   * "Data Products" depending on the multiple-data-products flag, and its add button has no test id, so
   * it is found as the first button in the header that sits above the section content.
   */
  async attachFromAssetSidebar(dataProductName: string): Promise<void> {
    const content = this.page.getByTestId(/^sidebar-section-content-Data Products?$/);
    await content.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    // eslint-disable-next-line playwright/no-raw-locators -- the section's add button is an icon button with no test id or accessible name; reach it through the header that wraps the content
    await content.locator('xpath=..').locator('button').first().click();

    // The select opens by default and renders its options in a portal outside the dialog element.
    const searchInput = this.page.getByPlaceholder('Search...', { exact: true });
    // The checkbox input itself is visually hidden, so select the option through its label.
    const option = this.page.getByText(dataProductName, { exact: true });

    // A data product that was just created is only offered once search has indexed it, so re-run the
    // query until it shows up.
    await expect(async () => {
      await searchInput.fill('');
      await searchInput.fill(dataProductName);
      await expect(option).toBeVisible({ timeout: TIMEOUTS.SHORT });
    }).toPass({ timeout: TIMEOUTS.LONG });
    await option.click();

    // The multi-select stays open over the footer, so click the title to close it before saving.
    const modal = this.page.getByRole('dialog', { name: /Set Data Products?/ });
    await modal.getByRole('heading', { name: /Set Data Products?/ }).click();
    await modal.getByRole('button', { name: 'Save' }).click();
    await this.toast.expectVisible('Updated Data Product', { timeout: TIMEOUTS.LONG });
  }

  /** Delete the open data product from its profile page's entity menu. */
  async deleteFromProfile(): Promise<void> {
    await this.threeDotMenu.click();
    await this.entityMenuDeleteButton.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.entityMenuDeleteButton.click();
    await this.deleteConfirmButton.click();
  }
}
