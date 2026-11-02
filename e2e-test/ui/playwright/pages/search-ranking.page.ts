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

  /**
   * URNs of the cards on the current page, top to bottom. The result wrapper names the served
   * entity in its `entityUrn-<urn>` class; a column card's preview names its parent dataset instead.
   */
  async urnsOnPage(): Promise<string[]> {
    await expect(this.resultItems.first()).toBeVisible({ timeout: 30_000 });
    return this.resultItems.evaluateAll((items) =>
      items.map((item) => {
        for (let node: Element | null = item; node; node = node.parentElement) {
          // The class attribute is space-separated, and a URN may hold spaces: read up to the
          // parenthesis that closes the URN, or to the next space for a URN without one.
          const attr = node.getAttribute('class') ?? '';
          const start = attr.indexOf('entityUrn-');
          if (start >= 0) {
            const rest = attr.slice(start + 'entityUrn-'.length);
            let depth = 0;
            for (let i = 0; i < rest.length; i += 1) {
              if (rest[i] === '(') depth += 1;
              if (rest[i] === ')') depth -= 1;
              if (
                (depth === 0 && rest[i] === ')') ||
                (depth === 0 && rest[i] === ' ' && !rest.slice(0, i).includes('('))
              ) {
                return rest[i] === ')' ? rest.slice(0, i + 1) : rest.slice(0, i);
              }
            }
            return rest;
          }
        }
        return '';
      }),
    );
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
