/**
 * ApplicationsPage — consolidated page object for all application features
 *
 * Covers:
 * - Manage Applications page (/applications)
 * - Application Detail page (/application/{urn})
 * - Dataset Sidebar Application Section
 */

import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { TIMEOUTS, LOAD_STATES } from '../utils/constants';

export class ApplicationsPage extends BasePage {
  // ── Manage Applications Page ─────────────────────────────────────────
  readonly pageTitle: Locator;
  readonly searchInput: Locator;
  readonly createApplicationButton: Locator;
  readonly applicationsTable: Locator;
  readonly emptyStateText: Locator;
  readonly deleteConfirmationModal: Locator;
  readonly loadingDataText: Locator;
  readonly applicationNameInput: Locator;
  readonly applicationDescriptionInput: Locator;
  readonly createButtonModal: Locator;
  readonly deleteOption: Locator;
  readonly deleteConfirmTitle: Locator;
  readonly deleteConfirmButton: Locator;

  // ── Dataset Sidebar Section ──────────────────────────────────────────
  readonly addApplicationsButton: Locator;
  readonly applicationSection: Locator;
  readonly noApplicationText: Locator;
  readonly removeConfirmYesButton: Locator;
  readonly applicationSelectDropdown: Locator;
  readonly removeIcon: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    // Manage Applications Page
    this.pageTitle = page.getByTestId('page-title');
    this.searchInput = page.getByTestId('application-search-input');
    this.createApplicationButton = page.getByTestId('create-application-button');
    this.applicationsTable = page.getByTestId('applications-table');
    this.emptyStateText = page.getByTestId('applications-not-found');
    this.deleteConfirmationModal = page.getByRole('dialog');
    this.loadingDataText = page.getByText('loading data');
    this.applicationNameInput = page.getByTestId('application-name-input');
    this.applicationDescriptionInput = page.getByTestId('application-description-input');
    this.createButtonModal = page.getByTestId('create-button-modal');
    this.deleteOption = page.getByTestId('action-delete');
    this.deleteConfirmTitle = page.getByRole('heading', { name: /delete application/i });
    this.deleteConfirmButton = page.getByTestId('modal-confirm-button');

    // Dataset Sidebar
    this.addApplicationsButton = page.getByTestId('add-applications-button');
    this.applicationSection = page.getByTestId('sidebar-section-content-Applications');
    this.noApplicationText = page.getByText('No application yet');
    this.removeConfirmYesButton = page.getByRole('button', { name: /yes/i });
    this.applicationSelectDropdown = page.getByTestId('application-select');
    this.removeIcon = page.getByTestId('remove-icon');
  }

  // ════════════════════════════════════════════════════════════════════
  // Private Helper Methods for Dynamic Selectors
  // ════════════════════════════════════════════════════════════════════

  private getApplicationRow(name: string): Locator {
    return this.page.getByRole('row').filter({ hasText: name });
  }

  private getAssetElement(assetName: string): Locator {
    return this.page.getByText(assetName, { exact: false });
  }

  private getCountElement(countText: string): Locator {
    return this.page.getByText(countText);
  }

  private getApplicationElement(appName: string): Locator {
    return this.page.getByText(appName, { exact: true });
  }

  private getDropdownOption(appName: string): Locator {
    return this.page.getByText(appName, { exact: true });
  }

  private getOKButton(): Locator {
    return this.page.getByRole('button', { name: /ok/i, exact: false }).filter({ visible: true });
  }

  private getApplicationActionsDropdown(appRow: Locator): Locator {
    return appRow.getByTestId('MoreVertOutlinedIcon');
  }

  private getSuccessMessage(text: string): Locator {
    return this.page.getByText(new RegExp(text, 'i'));
  }

  // ════════════════════════════════════════════════════════════════════
  // Manage Applications Page Methods
  // ════════════════════════════════════════════════════════════════════

  async navigateToApplicationsPage(): Promise<void> {
    await this.page.goto('/applications');
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async verifyPageTitle(): Promise<void> {
    await expect(this.pageTitle).toContainText('Manage Applications');
  }

  async verifySearchInputPlaceholder(): Promise<void> {
    await expect(this.searchInput).toHaveAttribute('placeholder', 'Search applications...');
  }

  async verifyPageLoaded(): Promise<void> {
    await expect(this.loadingDataText).toBeHidden();
  }

  async searchApplication(query: string): Promise<void> {
    await this.searchInput.fill('');
    await this.searchInput.type(query);
    // Wait out the search-input debounce before the query fires; networkidle
    // below can't retry a request that hasn't started yet.
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async verifyNoSearchResults(): Promise<void> {
    await expect(this.emptyStateText).toContainText('No applications found for your search query');
  }

  async clickCreateApplicationButton(): Promise<void> {
    await this.createApplicationButton.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async enterApplicationName(name: string): Promise<void> {
    await this.applicationNameInput.fill(name);
  }

  async enterApplicationDescription(description: string): Promise<void> {
    await this.applicationDescriptionInput.fill(description);
  }

  async clickCreateButton(): Promise<void> {
    await this.createButtonModal.click();
    // Wait for modal to close; the caller uses waitForWritesToSync() afterwards
    // for the actual create-write consistency wait before checking the table.
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async verifyApplicationInTable(name: string): Promise<void> {
    const appRow = this.getApplicationRow(name);
    await expect(appRow).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async verifyApplicationNotInTable(name: string): Promise<void> {
    const appRow = this.getApplicationRow(name);
    await expect(appRow).toBeHidden();
  }

  async clickApplicationActions(name: string): Promise<void> {
    const appRow = this.getApplicationRow(name);
    const dropdown = this.getApplicationActionsDropdown(appRow);
    await dropdown.click();
  }

  async clickDeleteOption(): Promise<void> {
    await this.deleteOption.click();
  }

  async verifyDeleteConfirmationModal(): Promise<void> {
    await expect(this.deleteConfirmTitle).toBeVisible();
  }

  async confirmDeletion(): Promise<void> {
    await this.deleteConfirmButton.click();
    // Callers use waitForWritesToSync() afterwards for the actual
    // delete-write consistency wait before re-checking the table.
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async deleteApplication(name: string): Promise<void> {
    await this.clickApplicationActions(name);
    await this.clickDeleteOption();
    await this.verifyDeleteConfirmationModal();
    await this.confirmDeletion();
  }

  // ════════════════════════════════════════════════════════════════════
  // Application Detail Page Methods
  // ════════════════════════════════════════════════════════════════════

  async navigateToApplication(urn: string, tabName?: string): Promise<void> {
    const applicationUrn = encodeURIComponent(urn);
    const path = tabName ? `/application/${applicationUrn}/${tabName}` : `/application/${applicationUrn}`;
    await this.page.goto(path);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async verifyAssetInList(assetName: string): Promise<void> {
    // Wait for the GraphQL search query to complete; the visibility assertion
    // below retries rendering for EXTRA_LONG on its own.
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    const assetElement = this.getAssetElement(assetName);
    await expect(assetElement).toBeVisible({ timeout: TIMEOUTS.EXTRA_LONG });
  }

  async verifyAssetCount(countText: string): Promise<void> {
    const countElement = this.getCountElement(countText);
    await expect(countElement).toBeVisible();
  }

  // ════════════════════════════════════════════════════════════════════
  // Dataset Sidebar Application Section Methods
  // ════════════════════════════════════════════════════════════════════

  async navigateToDataset(urn: string): Promise<void> {
    // Properly encode URN with all special characters
    const datasetUrn = encodeURIComponent(urn);
    await this.page.goto(`/dataset/${datasetUrn}`);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async verifyApplicationSectionVisible(): Promise<void> {
    await expect(this.applicationSection).toBeVisible();
  }

  async verifyNoApplicationYetText(): Promise<void> {
    await expect(this.noApplicationText).toBeVisible();
  }

  async verifyNoApplicationYetTextNotVisible(): Promise<void> {
    await expect(this.noApplicationText).toBeHidden();
  }

  async verifyApplicationVisible(appName: string): Promise<void> {
    const appElement = this.getApplicationElement(appName);
    await expect(appElement).toBeVisible();
  }

  async verifyApplicationNotVisible(appName: string): Promise<void> {
    const appElement = this.getApplicationElement(appName);
    await expect(appElement).toBeHidden();
  }

  async clickAddApplicationsButton(): Promise<void> {
    await this.addApplicationsButton.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async clickSelectApplicationDropdown(): Promise<void> {
    await this.applicationSelectDropdown.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    // Brief settle for the modal open animation before the click; no assertion
    // to retry against here, so kept small rather than removed outright.
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
    await this.applicationSelectDropdown.click({ timeout: TIMEOUTS.MEDIUM });
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async searchApplicationInDropdown(appName: string): Promise<void> {
    // Find the input within the application-select dropdown
    const searchInput = this.applicationSelectDropdown.getByRole('combobox');

    // Wait for input to be ready
    await searchInput.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.page.waitForTimeout(TIMEOUTS.QUICK);

    // Fill the search input with application name
    await searchInput.fill(appName);
    // Wait for the filtered option to actually render instead of guessing the
    // debounce — selectApplicationFromDropdown() clicks it immediately after.
    await this.getDropdownOption(appName)
      .waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT })
      .catch(() => {});
  }

  async selectApplicationFromDropdown(appName: string): Promise<void> {
    const option = this.getDropdownOption(appName);
    await option.click();
    // No sleep: clickOKButton()'s own waitFor() tolerates the dropdown-close animation.
  }

  async clickOKButton(): Promise<void> {
    const okButton = this.getOKButton();

    // Wait for OK button to appear
    await okButton.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });

    // Click OK button
    await okButton.click({ timeout: TIMEOUTS.MEDIUM });

    // Wait for operation to complete
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    // Wait for success message
    await this.waitForSuccessMessage('Application set');
  }

  async waitForSuccessMessage(text: string): Promise<void> {
    const message = this.getSuccessMessage(text);
    await expect(message).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async addApplicationToDataset(appName: string): Promise<void> {
    await this.clickAddApplicationsButton();
    await this.clickSelectApplicationDropdown();
    await this.searchApplicationInDropdown(appName);
    await this.selectApplicationFromDropdown(appName);
    await this.clickOKButton();
  }

  async clickRemoveApplicationIcon(): Promise<void> {
    await this.removeIcon.click({ timeout: TIMEOUTS.MEDIUM });
  }

  async confirmRemoveApplication(): Promise<void> {
    await this.removeConfirmYesButton.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    // Wait for success message
    await this.waitForSuccessMessage('Removed Application');
  }

  async removeApplication(): Promise<void> {
    await this.clickRemoveApplicationIcon();
    await this.confirmRemoveApplication();
  }
}
