import { Page, Locator } from '@playwright/test';

export class SearchbarComponent {
  readonly searchInput: Locator;
  readonly searchButton: Locator;
  readonly autocomplete: Locator;

  constructor(private page: Page) {
    this.searchInput = page.getByTestId('global-search-input');
    this.searchButton = page.getByTestId('global-search-button');
    this.autocomplete = page.getByTestId('search-autocomplete');
  }

  async search(query: string): Promise<void> {
    await this.searchInput.fill(query);
    await this.searchButton.click();
  }

  async waitForAutocomplete(): Promise<void> {
    await this.autocomplete.waitFor({ state: 'visible' });
  }

  async selectAutocompleteOption(index: number): Promise<void> {
    await this.autocomplete.getByTestId(`autocomplete-option-${index}`).click();
  }
}
