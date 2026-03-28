import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base-page';

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

  constructor(page: Page) {
    super(page);
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
    await this.navigate('/');
    await this.page.waitForLoadState('networkidle');
    await this.searchInput.waitFor({ state: 'visible', timeout: 10000 });
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

  async selectFilterOption(filterName: string, optionLabel: string): Promise<void> {
    const filterDropdown = this.page.locator(`[data-testid="filter-dropdown-${filterName}"]`);
    await filterDropdown.waitFor({ state: 'visible', timeout: 10000 });
    await filterDropdown.click({ force: true });

    // Wait for the dropdown menu to appear
    await this.page.waitForTimeout(1000);

    // Try checkbox-based selection first (for Type filters)
    const optionPattern = new RegExp(`^${optionLabel}(\\s+\\d+)?$`);
    const checkboxOption = this.page.getByRole('checkbox', { name: optionPattern });
    const checkboxCount = await checkboxOption.count();

    if (checkboxCount > 0) {
      // Checkbox found - use it
      if (checkboxCount > 1) {
        await checkboxOption.first().check();
      } else {
        await checkboxOption.check();
      }
    } else {
      // No checkbox - try text-based selection (for Platform filters)
      let textOption = this.page.getByText(optionLabel, { exact: true });
      let textCount = await textOption.count();

      // If option not found, try using the search input in the dropdown
      if (textCount === 0) {
        const searchInputs = await this.page.locator('[data-testid="search-input"]').all();
        if (searchInputs.length > 1) {
          // Use the search input in the dropdown (second one, first is main search bar)
          await searchInputs[1].fill(optionLabel);
          await this.page.waitForTimeout(500);
          textOption = this.page.getByText(optionLabel, { exact: true });
          textCount = await textOption.count();
        }
      }

      if (textCount > 1) {
        // Multiple matches - click the one inside the dropdown menu
        await textOption.nth(1).click();
      } else if (textCount === 1) {
        await textOption.click();
      } else {
        throw new Error(`Could not find filter option: ${optionLabel}`);
      }
    }

    await this.page.getByRole('button', { name: 'Update' }).click();
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
