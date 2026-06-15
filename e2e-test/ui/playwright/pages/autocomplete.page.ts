import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import { TIMEOUTS, LOAD_STATES } from '../utils/constants';
import type { DataHubLogger } from '../utils/logger';

/**
 * AutocompletePage — Page Object for search bar autocomplete functionality
 *
 * Encapsulates all autocomplete-specific interactions:
 * - Typing in search bar to trigger dropdown
 * - Verifying autocomplete results appear
 * - Clicking results to navigate
 * - Clearing search input
 */
export class AutocompletePage extends BasePage {
  readonly searchInput: Locator;
  readonly autocompleteDropdown: Locator;
  readonly autocompleteOptions: Locator;
  readonly entityHeader: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.searchInput = page.getByTestId('search-input');
    this.autocompleteDropdown = page.getByTestId('search-bar-dropdown');
    this.autocompleteOptions = this.autocompleteDropdown.getByRole('option');
    this.entityHeader = page.getByTestId('entity-header-test-id');
  }

  /**
   * Dynamic selectors are in private methods instead of constructor
   */

  private getAutocompleteItemByUrn(urn: string): Locator {
    const testId = `autocomplete-item-${urn}`;
    return this.page.getByTestId(testId);
  }

  private getEntityHeaderByName(entityName: string): Locator {
    return this.entityHeader.getByText(entityName, { exact: false });
  }

  async navigateToHome(): Promise<void> {
    this.logger?.step('navigateToHome');
    await this.suppressOnboardingOverlay();
    await this.navigate('/');
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await this.searchInput.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
  }

  async typeInSearchBar(query: string): Promise<void> {
    this.logger?.step('typeInSearchBar', { query });
    await this.searchInput.click();
    await this.searchInput.type(query);
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
  }

  async clearSearchInput(): Promise<void> {
    this.logger?.step('clearSearchInput');
    await this.searchInput.clear();
  }

  async expectAutocompleteVisible(): Promise<void> {
    this.logger?.step('expectAutocompleteVisible');
    await expect(this.autocompleteDropdown).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectAutocompleteItemsVisible(minCount: number = 1): Promise<void> {
    this.logger?.step('expectAutocompleteItemsVisible', { minCount });
    await this.autocompleteOptions.waitFor({ state: 'attached', timeout: TIMEOUTS.MEDIUM });
    const items = await this.autocompleteOptions.all();
    expect(items.length).toBeGreaterThanOrEqual(minCount);
  }

  async expectAutocompleteResultByUrn(urn: string): Promise<void> {
    this.logger?.step('expectAutocompleteResultByUrn', { urn });
    await this.getAutocompleteItemByUrn(urn).waitFor({ state: 'attached', timeout: TIMEOUTS.MEDIUM });
  }

  async clickAutocompleteOptionByUrn(urn: string): Promise<void> {
    this.logger?.step('clickAutocompleteOptionByUrn', { urn });
    await this.getAutocompleteItemByUrn(urn).click({ timeout: TIMEOUTS.MEDIUM });
  }

  async expectNavigationToEntity(entityName: string): Promise<void> {
    this.logger?.step('expectNavigationToEntity', { entityName });
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await this.getEntityHeaderByName(entityName).waitFor({
      state: 'visible',
      timeout: TIMEOUTS.MEDIUM,
    });
    expect(this.page.url()).toContain(entityName);
  }

  async searchAndVerifyResult(entityName: string, entityUrn: string): Promise<void> {
    this.logger?.step('searchAndVerifyResult', { entityName, entityUrn });
    await this.typeInSearchBar(entityName);
    await this.expectAutocompleteVisible();
    await this.expectAutocompleteItemsVisible(1);
    await this.expectAutocompleteResultByUrn(entityUrn);
  }

  async searchAndNavigateToEntity(entityName: string, entityUrn: string): Promise<void> {
    this.logger?.step('searchAndNavigateToEntity', { entityName, entityUrn });
    await this.searchAndVerifyResult(entityName, entityUrn);
    await this.clickAutocompleteOptionByUrn(entityUrn);
    await this.expectNavigationToEntity(entityName);
  }

  private async suppressOnboardingOverlay(): Promise<void> {
    await this.page.addInitScript(() => {
      localStorage.setItem('skipOnboardingTour', 'true');
    });
  }
}
