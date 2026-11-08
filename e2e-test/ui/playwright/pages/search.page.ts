import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { retryOnFail } from '@utils/retry';
import { TIMEOUTS } from '../utils/constants';
import { GraphQLHelper } from '../helpers/graphql-helper';

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
  private readonly visibleMoreFilterOptions: Locator;
  private readonly graphql: GraphQLHelper;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.searchInput = page.getByTestId('search-input');
    this.searchBar = page.getByTestId('search-bar');
    this.searchResults = page.getByTestId('search-results');
    this.filtersV1 = page.getByTestId('search-filters-v1');
    this.filtersV2 = page.getByTestId('search-filters-v2');
    this.advancedButton = page.getByText('Advanced Filters');
    this.addFilterButton = page.getByText('Add Filter');
    this.clearAllFiltersButton = page.getByTestId('clear-all-filters');
    this.moreFiltersDropdown = page.getByTestId('more-filters-dropdown');
    this.updateFiltersButton = page.getByTestId('update-filters');
    this.searchBarInput = page.getByTestId('search-bar');
    this.clearButton = page.getByTestId('button-clear');
    this.autocompleteDropdown = page.getByTestId('search-bar-dropdown');
    this.filterDropdownMenu = page.getByTestId('filter-dropdown');
    this.visibleMoreFilterOptions = page.getByTestId(/^more-filter-/).filter({ visible: true });
    this.graphql = new GraphQLHelper(page);
  }

  async navigateToHome(): Promise<void> {
    // Suppress the ReactTour onboarding overlay before navigating so it does
    // not intercept pointer events on the search results page.
    await this.page.addInitScript(() => {
      localStorage.setItem('skipOnboardingTour', 'true');
    });
    await this.navigate('/');
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
    // eslint-disable-next-line playwright/no-raw-locators -- ReactTour uses a generated CSS id; no data-testid available
    const reactTour = this.page.locator('#___reactour');
    if (await reactTour.isVisible()) {
      await this.page.evaluate(() => {
        localStorage.setItem('skipOnboardingTour', 'true');
      });
      // Click the close button if it exists, otherwise press Escape.
      const closeButton = reactTour.getByRole('button', { name: 'Close tour' });
      if (await closeButton.isVisible()) {
        await closeButton.click();
      } else {
        await this.page.keyboard.press('Escape');
      }
      await reactTour.waitFor({ state: 'hidden', timeout: 5000 }).catch(() => {});
    }
  }

  /**
   * Submit `query` from the search bar and wait for its results request to finish. Waits on
   * the request rather than the UI because the previous query's results stay on screen while
   * the next one loads.
   */
  async search(query: string): Promise<void> {
    await this.searchInput.waitFor({ state: 'visible' });
    const current = new URL(this.page.url());
    // Resubmitting the query a results page already shows doesn't send a new request.
    const alreadyShown = current.pathname.startsWith('/search') && current.searchParams.get('query') === query;
    const loaded = alreadyShown
      ? Promise.resolve()
      : this.graphql.waitForGraphQLResponse(
          'getSearchResultsForMultiple',
          (variables) => (variables.input as { query?: string } | undefined)?.query?.trim() === query.trim(),
        );
    await this.searchInput.fill(query);
    await this.searchInput.press('Enter');
    await loaded;
    // Facets render with the results; callers probe them with instant isVisible()/count() checks.
    await expect(this.page.getByText(/of [\d,]+\+? results?/)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectResultsCount(pattern: RegExp): Promise<void> {
    await expect(this.page.getByText(pattern)).toBeVisible();
  }

  async expectNoResults(): Promise<void> {
    await retryOnFail(
      async () => {
        await expect(this.page.getByText('of 0 results')).toBeVisible();
      },
      {
        onRetry: async () => {
          await this.page.reload();
          await this.page.waitForLoadState('networkidle');
          await this.dismissOnboardingOverlays();
        },
      },
    );
  }

  async expectHasResults(): Promise<void> {
    await retryOnFail(
      async () => {
        await expect(this.page.getByText('of 0 results')).toBeHidden();
        await expect(this.page.getByText(/of [\d,]+ results?/)).toBeVisible();
      },
      {
        onRetry: async () => {
          await this.page.reload();
          await this.page.waitForLoadState('networkidle');
          await this.dismissOnboardingOverlays();
        },
      },
    );
  }

  async getResultCount(): Promise<number> {
    // Extract count from text like "1-20 of 1234 results"
    const resultText = await this.page.getByText(/of [\d,]+ results?/).textContent();
    if (!resultText) return 0;
    const match = resultText.match(/of ([\d,]+) results?/);
    return match ? parseInt(match[1].replaceAll(',', ''), 10) : 0;
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

  /** Open the "More Filters" menu and wait for its options to render. */
  async openMoreFilters(): Promise<void> {
    await this.moreFiltersDropdown.click();
    await expect(this.visibleMoreFilterOptions).not.toHaveCount(0);
  }

  /**
   * Returns true if a filter dropdown is available — either as a top-level
   * button or inside the "More Filters" menu. Use this to skip tests that
   * depend on a filter that the backend may not return aggregations for.
   */
  async isFilterAvailable(filterName: string): Promise<boolean> {
    const filterDropdown = this.page.getByTestId(`filter-dropdown-${filterName}`);
    if (await filterDropdown.isVisible()) return true;

    const moreFiltersBtn = this.moreFiltersDropdown;
    if (!(await moreFiltersBtn.isVisible())) return false;

    await this.openMoreFilters();
    const moreFilterOption = this.page.getByTestId(`more-filter-${filterName}`);
    const found = await moreFilterOption.isVisible();
    await this.page.keyboard.press('Escape');
    return found;
  }

  async selectFilterOption(filterName: string, optionLabel: string): Promise<void> {
    const filterDropdown = this.page.getByTestId(`filter-dropdown-${filterName}`);

    // Some filters (e.g. Glossary Term, Tag) are only shown as top-level
    // dropdowns when the backend returns aggregations for those fields.
    // When not visible at the top level, fall back to the "More Filters" menu.
    const isTopLevel = await filterDropdown.isVisible();
    if (!isTopLevel) {
      const moreFiltersBtn = this.moreFiltersDropdown;
      const moreFiltersVisible = await moreFiltersBtn.isVisible();
      if (moreFiltersVisible) {
        await this.openMoreFilters();
        const moreFilterOption = this.page.getByTestId(`more-filter-${filterName}`);
        const moreFilterVisible = await moreFilterOption.isVisible();
        if (moreFilterVisible) {
          await moreFilterOption.click();
          await this.waitForFilterOptions();
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
    await this.waitForFilterOptions();

    await this.selectFilterValue(optionLabel);
  }

  /**
   * Wait for an open filter dropdown to render its options; the facet query behind it can take a
   * few seconds on a loaded backend.
   */
  private async waitForFilterOptions(): Promise<void> {
    await this.filterDropdownMenu
      .getByRole('checkbox')
      .or(this.filterDropdownMenu.getByTestId('search-input'))
      .first()
      .waitFor({ state: 'attached', timeout: 10_000 })
      .catch(() => {
        // Some filter types render neither; selectFilterValue has its own fallbacks.
      });
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
    const dropdownSearchInput = dropdownMenu.getByTestId('search-input').last();
    if (await dropdownSearchInput.isVisible()) {
      await dropdownSearchInput.fill(optionLabel);
      // The narrowed list can arrive via a GraphQL call; give the option a chance to render.
      await this.page
        .getByRole('checkbox', { name: optionPattern })
        .first()
        .waitFor({ state: 'attached', timeout: 5000 })
        .catch(() => {
          // Not narrowed into a checkbox; the text-match fallback below gets a try.
        });

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
    const moreFilter = this.page.getByTestId(`more-filter-${filterName}`);
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
    await this.page.getByTestId(`adv-search-add-filter-${filterName}`).click({ force: true });
  }

  async fillTextFilter(text: string): Promise<void> {
    await this.page.getByTestId('edit-text-input').fill(text);
    await this.page.getByTestId('edit-text-done-btn').click({ force: true });
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
      Datasets: 'DATASET',
      Dataset: 'DATASET',
      DATASET: 'DATASET', // Support the actual value too
      Dashboards: 'DASHBOARD',
      Dashboard: 'DASHBOARD',
      DASHBOARD: 'DASHBOARD',
      Charts: 'CHART',
      Chart: 'CHART',
      CHART: 'CHART',
      'Data Jobs': 'DATA_JOB',
      DATA_JOB: 'DATA_JOB',
      'Glossary Terms': 'GLOSSARY_TERM',
      GLOSSARY_TERM: 'GLOSSARY_TERM',
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
        await expect(this.page.getByTestId(`active-filter-value-${mapping.field}-${mapping.value}`)).toBeVisible({
          timeout: 10000,
        });
      } else {
        // Field container only (e.g., Platform filters) - check by field and text
        const container = this.page.getByTestId(`active-filter-${mapping.field}`);
        await expect(container).toBeVisible({ timeout: 10000 });
        // Also verify the label text is present
        await expect(container.getByText(filterLabel)).toBeVisible();
      }
    } else {
      // Fallback: search by text content (for tags and other dynamic filters)
      // Look for any active filter containing this text
      // eslint-disable-next-line playwright/no-raw-locators -- substring match on data-testid ([data-testid*=]); getByTestId requires exact match
      const filterByText = this.page.locator('[data-testid*="active-filter"]').filter({ hasText: filterLabel });
      await expect(filterByText.first()).toBeVisible({ timeout: 10000 });
    }
  }

  async expectActiveFilterNotVisible(filterLabel: string): Promise<void> {
    const mapping = this.getFilterFieldMapping(filterLabel);

    if (mapping) {
      if (mapping.value) {
        // Specific field + value selector
        await expect(this.page.getByTestId(`active-filter-value-${mapping.field}-${mapping.value}`)).toBeHidden();
      } else {
        // Field container - check if it's not visible or doesn't contain the text
        const container = this.page.getByTestId(`active-filter-${mapping.field}`);
        const count = await container.count();
        if (count > 0) {
          // Container exists but should not contain this label
          await expect(container.getByText(filterLabel)).toBeHidden();
        }
      }
    } else {
      // Fallback: search by text content
      // eslint-disable-next-line playwright/no-raw-locators -- substring match on data-testid ([data-testid*=]); getByTestId requires exact match
      const filterByText = this.page.locator('[data-testid*="active-filter"]').filter({ hasText: filterLabel });
      await expect(filterByText).toBeHidden();
    }
  }

  async removeActiveFilter(filterLabel: string): Promise<void> {
    const mapping = this.getFilterFieldMapping(filterLabel);

    if (mapping) {
      // Click the remove button for this field
      const removeButton = this.page.getByTestId(`remove-filter-${mapping.field}`);
      await removeButton.click({ force: true });
    } else {
      // Fallback: find the filter by text and click its remove button
      // First find the active filter container with this text
      // eslint-disable-next-line playwright/no-raw-locators -- substring match on data-testid ([data-testid*=]); getByTestId requires exact match
      const filterContainer = this.page
        .locator('[data-testid*="active-filter"]')
        .filter({ hasText: filterLabel })
        .first();
      // Then find the remove button within it
      const removeButton = filterContainer.getByRole('button').first();
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

  /**
   * Change the operator on an active filter chip (e.g. Domain equals → within).
   * @param fieldName URL/field name such as "domains" or "platform"
   * @param operatorLabel Visible operator text such as "within" or "equals"
   */
  async selectActiveFilterOperator(fieldName: string, operatorLabel: string): Promise<void> {
    const chip = this.page.getByTestId(`active-filter-${fieldName}`);
    await chip.waitFor({ state: 'visible', timeout: 10000 });
    await chip.getByTestId('active-filter-operator').click();
    await this.page.getByRole('menuitem', { name: new RegExp(operatorLabel, 'i') }).click();
    await this.page.waitForLoadState('networkidle');
  }

  async expectActiveFilterOperator(fieldName: string, operatorLabel: string): Promise<void> {
    const chip = this.page.getByTestId(`active-filter-${fieldName}`);
    await expect(chip.getByTestId('active-filter-operator')).toHaveText(new RegExp(operatorLabel, 'i'));
  }

  async expectTextVisible(text: string): Promise<void> {
    await expect(this.page.getByText(text).first()).toBeVisible({ timeout: 10000 });
  }

  async expectFiltersV1Visible(): Promise<void> {
    await expect(this.filtersV1).toBeVisible();
  }

  async expectFiltersV1NotVisible(): Promise<void> {
    await expect(this.filtersV1).toBeHidden();
  }

  async expectFiltersV2Visible(): Promise<void> {
    await expect(this.filtersV2).toBeVisible();
  }

  async expectFiltersV2NotVisible(): Promise<void> {
    await expect(this.filtersV2).toBeHidden();
  }

  async searchInModal(searchTerm: string): Promise<void> {
    // Find the search input inside the filter dropdown modal
    const searchInputs = await this.page.getByTestId('search-input').all();
    if (searchInputs.length > 1) {
      // Use the second search input (first is the main search bar)
      await searchInputs[1].fill(searchTerm);
    } else {
      await searchInputs[0].fill(searchTerm);
    }
  }

  async clickEntityResult(): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators -- CSS class substring match for generated class; no data-testid on entity result container
    const firstResult = this.page.locator('[class*="entityUrn-urn"]').first();
    await firstResult.waitFor({ state: 'visible', timeout: 10000 });
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute substring match; no semantic Playwright API for href filtering
    const link = firstResult.locator('a[href*="urn:li"]').first();
    await link.click();
  }

  async expectPaginationVisible(): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design pagination next button CSS class; no data-testid available
    await expect(this.page.locator('.ant-pagination-next')).toBeVisible();
  }

  // ── Tag-filtered search ───────────────────────────────────────────────────

  getEntityPreviewLocator(entityUrn: string): Locator {
    return this.page.getByTestId(`preview-${entityUrn}`);
  }

  /**
   * Open a search result by URN. Card click only selects the preview pane —
   * navigate via the entity name link inside the preview card.
   */
  async openResultByUrn(entityUrn: string): Promise<void> {
    this.logger?.step('openResultByUrn', { entityUrn });
    const preview = this.getEntityPreviewLocator(entityUrn);
    await expect(preview).toBeVisible({ timeout: TIMEOUTS.LONG });
    await preview.getByRole('link').first().click();
  }

  async searchByTag(tagUrn: string): Promise<void> {
    this.logger?.step('searchByTag', { tagUrn });
    await this.page.goto(
      `/search?filter_tags___false___EQUAL___0=${encodeURIComponent(tagUrn)}&page=1&query=%2A&unionType=0`,
    );
    await this.waitForPageLoad();
  }

  async expectEntityInSearchResults(entityUrn: string): Promise<void> {
    this.logger?.step('expectEntityInSearchResults', { entityUrn });
    await retryOnFail(
      async () => {
        await expect(this.getEntityPreviewLocator(entityUrn)).toBeVisible();
      },
      {
        onRetry: async () => {
          await this.reload();
        },
      },
    );
  }

  async expectEntityNotInSearchResults(entityUrn: string): Promise<void> {
    this.logger?.step('expectEntityNotInSearchResults', { entityUrn });
    await retryOnFail(
      async () => {
        await expect(this.getEntityPreviewLocator(entityUrn)).toBeHidden();
      },
      {
        onRetry: async () => {
          await this.reload();
        },
      },
    );
  }
}
