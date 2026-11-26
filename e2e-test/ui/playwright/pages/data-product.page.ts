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
  readonly profileName: Locator;
  readonly dataProductSidebarSection: Locator;
  readonly setDataProductButton: Locator;
  readonly dataProductSearchInput: Locator;
  readonly removeConfirmButton: Locator;
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
    this.profileName = page.getByTestId('entity-name-editable');
    // The section is titled "Data Product" or "Data Products" depending on the multiple-data-products flag.
    this.dataProductSidebarSection = page.getByTestId(/^sidebar-section-content-Data Products?$/);
    this.setDataProductButton = page.getByTestId('set-data-product-button');
    this.dataProductSearchInput = page.getByTestId('dropdown-search-input');
    this.removeConfirmButton = page.getByTestId('modal-confirm-button');
    this.threeDotMenu = page.getByTestId('MoreVertOutlinedIcon');
    this.entityMenuDeleteButton = page.getByTestId('entity-menu-delete-button');
    this.deleteConfirmButton = page.getByRole('button', { name: 'Yes' });
  }

  /** A data product's card in a list, such as a domain's Data Products tab. */
  getDataProductPreview(dataProductUrn: string): Locator {
    return this.page.getByTestId(`preview-${dataProductUrn}`);
  }

  /** A data product offered by the Set Data Product modal's select. */
  getDataProductOption(dataProductUrn: string): Locator {
    return this.page.getByTestId(`option-${dataProductUrn}`);
  }

  /** An asset's card on a data product's Assets tab. */
  getAssetPreview(assetUrn: string): Locator {
    return this.page.getByTestId(`preview-${assetUrn}`);
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

  async expectListedInDomain(dataProductUrn: string): Promise<void> {
    await expect(this.getDataProductPreview(dataProductUrn)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async openProfile(dataProductUrn: string): Promise<void> {
    await this.navigate(`/dataProduct/${encodeURIComponent(dataProductUrn)}`);
    await this.page.waitForLoadState('networkidle');
  }

  async expectProfileName(name: string): Promise<void> {
    // antd ellipsizes the visible title, but mirrors the full text in aria-label.
    await expect(this.profileName).toHaveAttribute('aria-label', name, { timeout: TIMEOUTS.LONG });
  }

  /**
   * Open the data product's Assets tab and return the asset URNs its search answered with. The tab renders
   * nothing while that search is in flight, so the list is judged by the response rather than by what has
   * been drawn so far. The tab also issues a count-only query (count 0), which is skipped.
   */
  async openAssetsTab(dataProductUrn: string): Promise<string[]> {
    const loaded = this.graphqlHelper.waitForGraphQLResponse(
      'listDataProductAssets',
      (variables) => variables.urn === dataProductUrn && ((variables.input as { count?: number }).count ?? 0) > 0,
    );
    await this.navigate(`/dataProduct/${encodeURIComponent(dataProductUrn)}/Assets`);
    const response = await loaded;
    const data = response.data as { listDataProductAssets?: { searchResults?: { entity: { urn: string } }[] } };
    return (data.listDataProductAssets?.searchResults ?? []).map((result) => result.entity.urn);
  }

  /** Reload the Assets tab until the asset is listed; the list reads the search index, which lags the attach. */
  async expectAssetListed(dataProductUrn: string, assetUrn: string): Promise<void> {
    await expect(async () => {
      expect(await this.openAssetsTab(dataProductUrn)).toContain(assetUrn);
      await expect(this.getAssetPreview(assetUrn)).toBeVisible({ timeout: TIMEOUTS.SHORT });
    }).toPass({ timeout: TIMEOUTS.LONG });
  }

  /** Reload the Assets tab until the asset is no longer listed. */
  async expectAssetNotListed(dataProductUrn: string, assetUrn: string): Promise<void> {
    await expect(async () => {
      expect(await this.openAssetsTab(dataProductUrn)).not.toContain(assetUrn);
      await expect(this.getAssetPreview(assetUrn)).toHaveCount(0);
    }).toPass({ timeout: TIMEOUTS.LONG });
  }

  /** Attach the open asset to a data product from its sidebar's Data Product section. */
  async attachFromAssetSidebar(dataProductUrn: string, dataProductName: string): Promise<void> {
    await this.setDataProductButton.click();

    // The select opens by default and renders its options in a portal outside the dialog element.
    const option = this.getDataProductOption(dataProductUrn);

    // A data product that was just created is only offered once search has indexed it, so re-run the
    // query until it shows up.
    await expect(async () => {
      await this.dataProductSearchInput.fill('');
      await this.dataProductSearchInput.fill(dataProductName);
      await expect(option).toBeVisible({ timeout: TIMEOUTS.SHORT });
    }).toPass({ timeout: TIMEOUTS.LONG });
    await option.click();

    // The multi-select stays open over the title and footer, and Escape would close the whole modal, so
    // save with the modal's own Enter shortcut, which clicks Save.
    await this.page.keyboard.press('Enter');
    await this.toast.expectVisible(/Updated Data Products?!/, { timeout: TIMEOUTS.LONG });
  }

  /** Remove the open asset from the data product named in its sidebar, confirming the dialog. */
  async removeFromAssetSidebar(dataProductName: string): Promise<void> {
    const chip = this.dataProductSidebarSection.getByText(dataProductName);
    // The sidebar reads the asset's data products from the graph index, which can lag the attach, so
    // reload until the data product is listed.
    await expect(async () => {
      await this.page.reload();
      await expect(chip).toBeVisible({ timeout: TIMEOUTS.SHORT });
    }).toPass({ timeout: TIMEOUTS.LONG });
    await chip.getByTestId('remove-icon').click();
    // The click leaves the pointer on the chip, whose hover card would open over the confirmation, so
    // park the pointer on the confirm button until the card is gone.
    await this.entityHoverCard.dismiss(this.removeConfirmButton, dataProductName);
    await this.removeConfirmButton.click();
    await this.toast.expectVisible(/Removed (from )?Data Product/, { timeout: TIMEOUTS.LONG });
    await expect(chip).toHaveCount(0, { timeout: TIMEOUTS.MEDIUM });
  }

  /** Rename the open data product from the editable title in its profile header. */
  async renameFromProfile(newName: string): Promise<void> {
    await this.profileName.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.profileName.getByRole('button', { name: 'Edit', exact: true }).click();

    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design inline-edit textarea is created dynamically and has no role or test id
    const input = this.page.locator('.ant-typography-edit-content textarea');
    await input.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    await input.fill(newName);
    await input.press('Enter');

    // antd ellipsizes the visible title, but mirrors the full text in aria-label.
    await expect(this.profileName).toHaveAttribute('aria-label', newName, { timeout: TIMEOUTS.MEDIUM });
  }

  /** Delete the open data product from its profile page's entity menu and wait for the deletion. */
  async deleteFromProfile(): Promise<void> {
    await this.threeDotMenu.click();
    await this.entityMenuDeleteButton.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.entityMenuDeleteButton.click();
    await this.deleteConfirmButton.click();
    await this.toast.expectVisible(/Deleted .+!/, { timeout: TIMEOUTS.LONG });
  }
}
