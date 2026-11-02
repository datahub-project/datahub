import { Locator, Page, expect } from '@playwright/test';
import { SearchPage } from './search.page';
import type { DataHubLogger } from '../utils/logger';

/** Reads the search results page in order, one URN per result card. */
export class SearchRankingPage extends SearchPage {
  readonly resultItems: Locator;
  readonly resultRange: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.resultItems = page.getByTestId('search-result-item');
    this.resultRange = page.getByText(/of [\d,]+ results?/);
  }

  /** URNs of the cards on the current page, top to bottom. */
  async urnsOnPage(): Promise<string[]> {
    await expect(this.resultItems.first()).toBeVisible({ timeout: 30_000 });
    const ids = await this.resultItems.evaluateAll((items) =>
      items.map((item) => item.querySelector('[data-testid^="preview-"]')?.getAttribute('data-testid') ?? ''),
    );
    return ids.map((id) => id.replace(/^preview-/, ''));
  }

  async totalResults(): Promise<number> {
    const text = (await this.resultRange.first().textContent()) ?? '';
    const match = text.replace(/\s+/g, ' ').match(/of ([\d,]+) results?/);
    return match ? Number(match[1].replaceAll(',', '')) : 0;
  }

  /**
   * Types the query into the search box, then follows the result pages until `depth` results are
   * read or the results run out. Returns the URNs in order, the total each page reported and a
   * screenshot of the first results page.
   */
  async readResults(query: string, depth: number): Promise<{ urns: string[]; totals: number[]; firstPage: Buffer }> {
    await this.navigateToHome();
    await this.searchAndWait(query, 1000);
    const urns: string[] = [];
    const totals: number[] = [];
    let firstPage: Buffer = Buffer.alloc(0);
    for (let pageNumber = 1; urns.length < depth; pageNumber += 1) {
      if (pageNumber > 1) {
        await this.navigate(`/search?query=${encodeURIComponent(query)}&page=${pageNumber}`);
        await this.waitForPageLoad();
      }
      const onPage = await this.urnsOnPage();
      if (pageNumber === 1) firstPage = await this.page.screenshot();
      totals.push(await this.totalResults());
      urns.push(...onPage);
      if (onPage.length === 0 || urns.length >= totals[totals.length - 1]) break;
    }
    return { urns: urns.slice(0, depth), totals, firstPage };
  }
}
