import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './BasePage';

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

  constructor(page: Page) {
    super(page);
    this.searchInput = page.locator('[data-testid="search-input"]');
    this.searchBar = page.locator('[data-testid="search-bar"]');
    this.searchResults = page.locator('[data-testid="search-results"]');
    this.filtersV1 = page.locator('[data-testid="search-filters-v1"]');
    this.filtersV2 = page.locator('[data-testid="search-filters-v2"]');
    this.advancedButton = page.getByText('Advanced');
    this.addFilterButton = page.getByText('Add Filter');
    this.clearAllFiltersButton = page.locator('[data-testid="clear-all-filters"]');
    this.moreFiltersDropdown = page.locator('[data-testid="more-filters-dropdown"]');
    this.updateFiltersButton = page.locator('[data-testid="update-filters"]');
    this.searchBarInput = page.locator('[data-testid="search-bar"]');
  }

  async navigateToHome(): Promise<void> {
    await this.navigate('/');
  }

  async search(query: string): Promise<void> {
    await this.searchInput.fill(query);
    await this.searchInput.press('Enter');
    await this.page.waitForTimeout(2000);
  }

  async searchAndWait(query: string, waitTime: number = 5000): Promise<void> {
    await this.searchInput.fill(query);
    await this.searchInput.press('Enter');
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
    await this.page.getByText(resultName).click();
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
    const option = this.page.locator(`[data-testid="filter-option-${optionLabel}"]`);
    await option.waitFor({ state: 'visible', timeout: 10000 });
    await option.click({ force: true });
    await this.updateFiltersButton.click({ force: true, timeout: 5000 });
  }

  async selectFilterOptionThroughMoreFilters(filterName: string, optionLabel: string): Promise<void> {
    await this.moreFiltersDropdown.click();
    const moreFilter = this.page.locator(`[data-testid="more-filter-${filterName}"]`);
    await moreFilter.click();
    const option = this.page.getByText(optionLabel);
    await option.click();
    await this.updateFiltersButton.click({ force: true });
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

  async expectActiveFilter(filterLabel: string): Promise<void> {
    await expect(this.page.locator(`[data-testid="active-filter-${filterLabel}"]`)).toBeVisible();
  }

  async expectActiveFilterNotVisible(filterLabel: string): Promise<void> {
    await expect(this.page.locator(`[data-testid="active-filter-${filterLabel}"]`)).not.toBeVisible();
  }

  async removeActiveFilter(filterLabel: string): Promise<void> {
    await this.page.locator(`[data-testid="remove-filter-${filterLabel}"]`).click({ force: true });
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
    await expect(this.page.getByText(text)).toBeVisible();
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
    const searchBars = await this.page.locator('[data-testid="search-bar"]').all();
    if (searchBars.length > 1) {
      await searchBars[1].fill(searchTerm);
    } else {
      await searchBars[0].fill(searchTerm);
    }
  }

  async clickEntityResult(): Promise<void> {
    const firstResult = this.page.locator('[class*="entityUrn-urn"]').first();
    const link = firstResult.locator('a[href*="urn:li"] span[class^="ant-typography"]').last();
    await link.click();
  }

  async expectPaginationVisible(): Promise<void> {
    await expect(this.page.locator('.ant-pagination-next')).toBeVisible();
  }
}
