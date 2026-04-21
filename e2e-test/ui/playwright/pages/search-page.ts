import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base-page';
import type { DataHubLogger } from '../utils/logger';

export class SearchPage extends BasePage {
  readonly searchInput: Locator;
  readonly searchBar: Locator;
  readonly searchResults: Locator;
  readonly filtersV1: Locator;
  readonly filtersV2: Locator;
  readonly advancedButton: Locator;
  readonly addFilterButton: Locator;
  readonly clearAllFiltersButton: Locator;
  readonly moreFiltersDropdown: Locator;
  readonly updateFiltersButton: Locator;
  readonly searchBarInput: Locator;
  readonly clearButton: Locator;
  readonly autocompleteDropdown: Locator;
  readonly filterDropdownMenu: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.searchInput = page.locator('[data-testid="search-input"]');
    this.searchBar = page.locator('[data-testid="search-bar"]');
    this.searchResults = page.locator('[data-testid="search-results"]');
    this.filtersV1 = page.locator('[data-testid="search-filters-v1"]');
    this.filtersV2 = page.locator('[data-testid="search-filters-v2"]');
    this.advancedButton = page.getByText('Advanced Filters');
    this.addFilterButton = page.getByText('Add Filter');
    this.clearAllFiltersButton = page.locator('[data-testid="clear-all-filters"]');
    this.moreFiltersDropdown = page.locator('[data-testid="more-filters-dropdown"]');
    this.updateFiltersButton = page.locator('[data-testid="update-filters"]');
    this.searchBarInput = page.locator('[data-testid="search-bar"]');
    this.clearButton = page.locator('[data-testid="button-clear"]');
    this.autocompleteDropdown = page.locator('[data-testid="search-bar-dropdown"]');
    this.filterDropdownMenu = page.locator('[data-testid="filter-dropdown"]');
  }

  async navigateToHome(): Promise<void> {
    // Suppress the ReactTour onboarding overlay before navigating so it does
    // not intercept pointer events on the search results page.
    await this.page.addInitScript(() => {
      localStorage.setItem('skipOnboardingTour', 'true');
    });
    await this.navigate('/');
    await this.page.waitForLoadState('networkidle');
    await this.searchInput.waitFor({ state: 'visible', timeout: 10000 });
    // Dismiss any remaining dialog (e.g. "Narrow your search" welcome modal).
    await this.dismissOnboardingOverlays();
  }

  /**
   * Dismiss any onboarding overlays that may block pointer events.
   * Handles both the WelcomeToDataHub dialog and the ReactTour overlay.
   */
  async dismissOnboardingOverlays(): Promise<void> {
    // Dismiss modal dialogs (WelcomeToDataHub, "Narrow your search").
    const modal = this.page.getByRole('dialog');
    if (await modal.isVisible()) {
      await this.page.keyboard.press('Escape');
      await modal.waitFor({ state: 'hidden', timeout: 5000 }).catch(() => {});
    }
    // Dismiss the ReactTour overlay if it is blocking pointer events.
    // The overlay renders as div#___reactour covering the full viewport.
    const reactTour = this.page.locator('#___reactour');
    if (await reactTour.isVisible()) {
      await this.page.evaluate(() => {
        localStorage.setItem('skipOnboardingTour', 'true');
      });
      // Click the close button if it exists, otherwise press Escape.
      const closeButton = reactTour.locator('button[aria-label="Close tour"]');
      if (await closeButton.isVisible()) {
        await closeButton.click();
      } else {
        await this.page.keyboard.press('Escape');
      }
      await reactTour.waitFor({ state: 'hidden', timeout: 5000 }).catch(() => {});
    }
  }

  async search(query: string): Promise<void> {
    await this.searchInput.fill(query);
    await this.searchInput.press('Enter');
    await this.page.waitForTimeout(2000);
  }

  async searchAndWait(query: string, waitTime: number = 5000): Promise<void> {
    await this.searchInput.waitFor({ state: 'visible' });
    await this.searchInput.fill(query);
    await this.searchInput.press('Enter');
    await this.page.waitForLoadState('networkidle');
    await this.page.waitForTimeout(waitTime);
  }

  async expectResultsCount(pattern: RegExp): Promise<void> {
    await expect(this.page.getByText(pattern)).toBeVisible();
  }

  async expectNoResults(): Promise<void> {
    await expect(this.page.getByText('of 0 results')).toBeVisible();
  }

  async expectHasResults(): Promise<void> {
    await expect(this.page.getByText('of 0 results')).not.toBeVisible();
    await expect(this.page.getByText(/of [0-9]+ result/)).toBeVisible();
  }

  async clickResult(resultName: string): Promise<void> {
    await this.page.getByText(resultName).first().click();
  }

  async clickAdvanced(): Promise<void> {
    await this.advancedButton.click();
  }

  async clickAddFilter(): Promise<void> {
    await this.addFilterButton.click();
  }

  async selectFilter(filterType: string): Promise<void> {
    await this.page.getByText(filterType).click({ force: true });
  }

  /**
   * Returns true if a filter dropdown is available — either as a top-level
   * button or inside the "More Filters" menu. Use this to skip tests that
   * depend on a filter that the backend may not return aggregations for.
   */
  async isFilterAvailable(filterName: string): Promise<boolean> {
    const filterDropdown = this.page.locator(`[data-testid="filter-dropdown-${filterName}"]`);
    if (await filterDropdown.isVisible()) return true;

    const moreFiltersBtn = this.moreFiltersDropdown;
    if (!(await moreFiltersBtn.isVisible())) return false;

    await moreFiltersBtn.click();
    await this.page.waitForTimeout(300);
    const moreFilterOption = this.page.locator(`[data-testid="more-filter-${filterName}"]`);
    const found = await moreFilterOption.isVisible();
    await this.page.keyboard.press('Escape');
    return found;
  }

  async selectFilterOption(filterName: string, optionLabel: string): Promise<void> {
    const filterDropdown = this.page.locator(`[data-testid="filter-dropdown-${filterName}"]`);

    // Some filters (e.g. Glossary Term, Tag) are only shown as top-level
    // dropdowns when the backend returns aggregations for those fields.
    // When not visible at the top level, fall back to the "More Filters" menu.
    const isTopLevel = await filterDropdown.isVisible();
    if (!isTopLevel) {
      const moreFiltersBtn = this.moreFiltersDropdown;
      const moreFiltersVisible = await moreFiltersBtn.isVisible();
      if (moreFiltersVisible) {
        await moreFiltersBtn.click();
        await this.page.waitForTimeout(500);
        const moreFilterOption = this.page.locator(`[data-testid="more-filter-${filterName}"]`);
        const moreFilterVisible = await moreFilterOption.isVisible();
        if (moreFilterVisible) {
          await moreFilterOption.click();
          await this.page.waitForTimeout(500);
          // After clicking the more-filter option, select the option within
          // the sub-dropdown that appears. Fall through to the selection logic.
          await this.selectFilterValue(optionLabel);
          return;
        }
        // Close the More Filters dropdown before failing
        await this.page.keyboard.press('Escape');
      }
      // Wait for top-level filter dropdown with extended timeout as last resort
      await filterDropdown.waitFor({ state: 'visible', timeout: 10000 });
    }

    await filterDropdown.click({ force: true });

    // Wait for the dropdown menu to appear
    await this.page.waitForTimeout(1000);

    await this.selectFilterValue(optionLabel);
  }

  /**
   * Select a value within an already-open filter dropdown.
   * Tries checkbox-based selection first, then falls back to text-based.
   */
  private async selectFilterValue(optionLabel: string): Promise<void> {
    // Wait for the filter dropdown container to appear.
    const dropdownMenu = this.filterDropdownMenu;
    await dropdownMenu.waitFor({ state: 'visible', timeout: 5000 }).catch(() => {
      // Some filter types don't use the standard filter-dropdown container.
    });

    const optionPattern = new RegExp(optionLabel.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));

    // Helper: check if a checkbox matching optionLabel is visible and check it.
    const tryCheckbox = async (): Promise<boolean> => {
      const checkboxOption = this.page.getByRole('checkbox', { name: optionPattern });
      const count = await checkboxOption.count();
      if (count === 0) return false;
      const target = count > 1 ? checkboxOption.first() : checkboxOption;
      await target.check({ force: true });
      return true;
    };

    // First pass: checkbox visible immediately (e.g. Type filter options).
    if (await tryCheckbox()) {
      await this.page.getByRole('button', { name: 'Update' }).click();
      return;
    }

    // Second pass: type in the dropdown search bar to narrow the list, then
    // re-try checkbox (Platform, Glossary Term, Tag options).
    // The dropdown search input carries data-testid="search-input"; use .last()
    // to avoid matching the AntD Select combobox that shares the same testid.
    const dropdownSearchInput = dropdownMenu.locator('[data-testid="search-input"]').last();
    if (await dropdownSearchInput.isVisible()) {
      await dropdownSearchInput.fill(optionLabel);
      await this.page.waitForTimeout(500);

      if (await tryCheckbox()) {
        await this.page.getByRole('button', { name: 'Update' }).click();
        return;
      }
    }

    // Fallback: click the matching text entry inside the dropdown.
    const textOption = dropdownMenu.getByText(optionLabel, { exact: true });
    const textCount = await textOption.count();
    if (textCount > 0) {
      await textOption.first().click();
      await this.page.getByRole('button', { name: 'Update' }).click();
      return;
    }

    throw new Error(`Could not find filter option: ${optionLabel}`);
  }

  async selectFilterOptionThroughMoreFilters(filterName: string, optionLabel: string): Promise<void> {
    await this.moreFiltersDropdown.click();
    const moreFilter = this.page.locator(`[data-testid="more-filter-${filterName}"]`);
    await moreFilter.click();

    // Handle multiple matches by using first occurrence in the filter dropdown
    const option = this.page.getByText(optionLabel, { exact: true });
    const count = await option.count();
    if (count > 1) {
      await option.first().click();
    } else {
      await option.click();
    }

    await this.page.getByRole('button', { name: 'Update' }).click();
  }

  async selectAdvancedFilter(filterName: string): Promise<void> {
    await this.page.locator(`[data-testid="adv-search-add-filter-${filterName}"]`).click({ force: true });
  }

  async fillTextFilter(text: string): Promise<void> {
    await this.page.locator('[data-testid="edit-text-input"]').fill(text);
    await this.page.locator('[data-testid="edit-text-done-btn"]').click({ force: true });
  }

  async clickAllFilters(): Promise<void> {
    await this.page.getByText('all filters').click();
  }

  async clickAnyFilter(): Promise<void> {
    await this.page.getByText('any filter').click({ force: true });
  }

  /**
   * Map filter labels to their field names and values
   * Returns { field, value } or null if it should match by text
   */
  private getFilterFieldMapping(filterLabel: string): { field: string; value?: string } | null {
    // Type filters - map to _entityType␞typeNames field
    const typeMapping: Record<string, string> = {
      'Datasets': 'DATASET',
      'Dataset': 'DATASET',
      'DATASET': 'DATASET',  // Support the actual value too
      'Dashboards': 'DASHBOARD',
      'Dashboard': 'DASHBOARD',
      'DASHBOARD': 'DASHBOARD',
      'Charts': 'CHART',
      'Chart': 'CHART',
      'CHART': 'CHART',
      'Data Jobs': 'DATA_JOB',
      'DATA_JOB': 'DATA_JOB',
      'Glossary Terms': 'GLOSSARY_TERM',
      'GLOSSARY_TERM': 'GLOSSARY_TERM',
    };

    if (typeMapping[filterLabel]) {
      return { field: '_entityType␞typeNames', value: typeMapping[filterLabel] };
    }

    // Platform filters - these use lowercase platform names
    const platformNames = ['Hive', 'Snowflake', 'BigQuery', 'Kafka', 'MySQL', 'PostgreSQL', 'HDFS'];
    if (platformNames.includes(filterLabel)) {
      // Platform filter values are URNs like urn:li:dataPlatform:hive
      // We'll search by the filter container and text content
      return { field: 'platform' };
    }

    // Tag filters - match by text since tag values can vary
    // Tags don't have a predictable pattern, so we'll match by text
    return null;
  }

  async expectActiveFilter(filterLabel: string): Promise<void> {
    const mapping = this.getFilterFieldMapping(filterLabel);

    if (mapping) {
      if (mapping.value) {
        // Specific field + value selector (e.g., Type filters)
        const selector = `[data-testid="active-filter-value-${mapping.field}-${mapping.value}"]`;
        await expect(this.page.locator(selector)).toBeVisible({ timeout: 10000 });
      } else {
        // Field container only (e.g., Platform filters) - check by field and text
        const container = this.page.locator(`[data-testid="active-filter-${mapping.field}"]`);
        await expect(container).toBeVisible({ timeout: 10000 });
        // Also verify the label text is present
        await expect(container.getByText(filterLabel)).toBeVisible();
      }
    } else {
      // Fallback: search by text content (for tags and other dynamic filters)
      // Look for any active filter containing this text
      const filterByText = this.page.locator('[data-testid*="active-filter"]').filter({ hasText: filterLabel });
      await expect(filterByText.first()).toBeVisible({ timeout: 10000 });
    }
  }

  async expectActiveFilterNotVisible(filterLabel: string): Promise<void> {
    const mapping = this.getFilterFieldMapping(filterLabel);

    if (mapping) {
      if (mapping.value) {
        // Specific field + value selector
        const selector = `[data-testid="active-filter-value-${mapping.field}-${mapping.value}"]`;
        await expect(this.page.locator(selector)).not.toBeVisible();
      } else {
        // Field container - check if it's not visible or doesn't contain the text
        const container = this.page.locator(`[data-testid="active-filter-${mapping.field}"]`);
        const count = await container.count();
        if (count > 0) {
          // Container exists but should not contain this label
          await expect(container.getByText(filterLabel)).not.toBeVisible();
        }
      }
    } else {
      // Fallback: search by text content
      const filterByText = this.page.locator('[data-testid*="active-filter"]').filter({ hasText: filterLabel });
      await expect(filterByText).not.toBeVisible();
    }
  }

  async removeActiveFilter(filterLabel: string): Promise<void> {
    const mapping = this.getFilterFieldMapping(filterLabel);

    if (mapping) {
      // Click the remove button for this field
      const removeButton = this.page.locator(`[data-testid="remove-filter-${mapping.field}"]`);
      await removeButton.click({ force: true });
    } else {
      // Fallback: find the filter by text and click its remove button
      // First find the active filter container with this text
      const filterContainer = this.page.locator('[data-testid*="active-filter"]').filter({ hasText: filterLabel }).first();
      // Then find the remove button within it
      const removeButton = filterContainer.locator('button').first();
      await removeButton.click({ force: true });
    }
  }

  async clearAllFilters(): Promise<void> {
    await this.clearAllFiltersButton.click({ force: true });
  }

  async expectUrlContains(urlPart: string): Promise<void> {
    await expect(this.page).toHaveURL(new RegExp(urlPart));
  }

  async expectUrlNotContains(urlPart: string): Promise<void> {
    const url = this.page.url();
    expect(url).not.toContain(urlPart);
  }

  async expectTextVisible(text: string): Promise<void> {
    await expect(this.page.getByText(text).first()).toBeVisible();
  }

  async expectFiltersV1Visible(): Promise<void> {
    await expect(this.filtersV1).toBeVisible();
  }

  async expectFiltersV1NotVisible(): Promise<void> {
    await expect(this.filtersV1).not.toBeVisible();
  }

  async expectFiltersV2Visible(): Promise<void> {
    await expect(this.filtersV2).toBeVisible();
  }

  async expectFiltersV2NotVisible(): Promise<void> {
    await expect(this.filtersV2).not.toBeVisible();
  }

  async searchInModal(searchTerm: string): Promise<void> {
    // Find the search input inside the filter dropdown modal
    const searchInputs = await this.page.locator('[data-testid="search-input"]').all();
    if (searchInputs.length > 1) {
      // Use the second search input (first is the main search bar)
      await searchInputs[1].fill(searchTerm);
    } else {
      await searchInputs[0].fill(searchTerm);
    }
  }

  async clickEntityResult(): Promise<void> {
    const firstResult = this.page.locator('[class*="entityUrn-urn"]').first();
    await firstResult.waitFor({ state: 'visible', timeout: 10000 });
    const link = firstResult.locator('a[href*="urn:li"]').first();
    await link.click();
  }

  async expectPaginationVisible(): Promise<void> {
    await expect(this.page.locator('.ant-pagination-next')).toBeVisible();
  }
}
