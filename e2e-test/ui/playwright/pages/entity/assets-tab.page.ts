/**
 * AssetsTabPage — page object for an entity's Assets tab, such as a domain's: the embedded search list
 * with its select mode, bulk actions and CSV download.
 */

import { readFile } from 'node:fs/promises';
import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';

export class AssetsTabPage extends BasePage {
  readonly editButton: Locator;
  readonly selectAllCheckbox: Locator;
  readonly tagsDropdown: Locator;
  readonly addTagsMenuItem: Locator;
  readonly tagModalSelect: Locator;
  readonly tagModalSearchInput: Locator;
  readonly addTagsFromModalButton: Locator;
  readonly downloadCsvButton: Locator;
  readonly downloadCsvInput: Locator;
  readonly csvModalDownloadButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.editButton = page.getByTestId('search-results-edit-button');
    this.selectAllCheckbox = page.getByTestId('search-select-all-checkbox');
    this.tagsDropdown = page.getByTestId('search-select-tags-dropdown');
    this.addTagsMenuItem = page.getByTestId('menu-item-Add tags');
    this.tagModalSelect = page.getByTestId('tag-term-modal-input');
    this.tagModalSearchInput = page.getByTestId('dropdown-search-input');
    this.addTagsFromModalButton = page.getByTestId('add-tag-term-from-modal-btn');
    this.downloadCsvButton = page.getByTestId('download-csv-button');
    this.downloadCsvInput = page.getByTestId('download-as-csv-input');
    this.csvModalDownloadButton = page.getByTestId('csv-modal-download-button');
  }

  /** An asset's row in the list. */
  getAssetRow(assetUrn: string): Locator {
    return this.page.getByTestId(`search-result-row-${assetUrn}`);
  }

  /** A tag offered by the Add Tags modal's select. */
  getTagOption(tagName: string): Locator {
    return this.page.getByTestId(`tag-term-option-${tagName}`);
  }

  /** The count shown by the select bar, e.g. "2 selected". */
  getSelectedCount(count: number): Locator {
    return this.page.getByText(`${count} selected`, { exact: true });
  }

  async open(entityType: string, entityUrn: string): Promise<void> {
    await this.navigate(`/${entityType}/${encodeURIComponent(entityUrn)}/Assets`);
  }

  /** The rows render only once the tab's search has answered, so this also waits for the list to load. */
  async expectAssetListed(assetUrn: string): Promise<void> {
    await expect(this.getAssetRow(assetUrn)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async enterSelectMode(): Promise<void> {
    await this.editButton.click();
  }

  async selectAll(): Promise<void> {
    await this.selectAllCheckbox.click();
  }

  async expectSelectedCount(count: number): Promise<void> {
    await expect(this.getSelectedCount(count)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /** Add a tag to every selected asset from the select bar, and wait until the batch has been applied. */
  async addTagToSelection(tagName: string): Promise<void> {
    await this.tagsDropdown.click();
    await this.addTagsMenuItem.click();

    await expect(this.tagModalSelect).toBeVisible();
    // Alchemy SimpleSelect: open the dropdown so the search input becomes available.
    await this.tagModalSelect.click();
    await this.tagModalSearchInput.fill(tagName);

    const option = this.getTagOption(tagName);
    await option.waitFor({ state: 'visible' });
    await option.click();

    // The open multi-select popover overlaps the modal footer and intercepts a pointer click on Add, so
    // dispatch the click directly, as DatasetPage.assignTag does for the same modal.
    await expect(this.addTagsFromModalButton).toBeEnabled();
    await this.addTagsFromModalButton.dispatchEvent('click');

    // The batch mutation runs after the modal closes; its toast is the completion signal.
    await this.toast.expectVisible('Added Tags!', { timeout: TIMEOUTS.LONG });
  }

  /** Download the list as a CSV named `filename` and return the file's contents. */
  async downloadCsv(filename: string): Promise<string> {
    await this.downloadCsvButton.click();
    await this.downloadCsvInput.fill(filename);

    const downloadPromise = this.page.waitForEvent('download');
    await this.csvModalDownloadButton.click();
    const download = await downloadPromise;

    expect(download.suggestedFilename()).toBe(filename);
    return readFile(await download.path(), 'utf-8');
  }
}
