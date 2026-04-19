import { Page, Locator } from '@playwright/test';

export class SearchbarComponent {
  readonly searchInput: Locator;
  readonly searchButton: Locator;
  readonly autocomplete: Locator;

  constructor(private page: Page) {
    this.searchInput = page.locator('[data-testid="global-search-input"]');
    this.searchButton = page.locator('[data-testid="global-search-button"]');
    this.autocomplete = page.locator('[data-testid="search-autocomplete"]');
  }

  async search(query: string): Promise<void> {
    await this.searchInput.fill(query);
    await this.searchButton.click();
  }

  async waitForAutocomplete(): Promise<void> {
    await this.autocomplete.waitFor({ state: 'visible' });
  }

  async selectAutocompleteOption(index: number): Promise<void> {
    await this.autocomplete.locator(`[data-testid="autocomplete-option-${index}"]`).click();
  }
}
