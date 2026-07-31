import { type Locator, type Page, expect } from '@playwright/test';
import type { DataHubLogger } from '../../../../utils/logger';
import { BaseTab } from './base.tab';

export class RunHistoryBaseTab extends BaseTab {
  readonly name: string;
  readonly path: string;
  readonly tabKey: string;

  readonly executionsTable: Locator;
  readonly sourceNameFilter: Locator;
  readonly dropdownSearchBar: Locator;
  readonly footerUpdateButton: Locator;
  readonly filterDropdown: Locator;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    super(page, logger, logDir);

    this.name = 'RunHistory';
    this.path = '/ingestion/run-history';
    this.tabKey = 'RunHistory';

    this.executionsTable = page.getByTestId('executions-table');
    this.sourceNameFilter = page.getByTestId('source-name-filter');
    this.dropdownSearchBar = page.getByTestId('dropdown-search-input');
    this.footerUpdateButton = page.getByTestId('footer-button-update');
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design dropdown portal has no data-testid or ARIA role
    this.filterDropdown = page.locator('body .ant-dropdown');
  }

  getExecutionRowLocator(executionUrn: string): Locator {
    return this.executionsTable.getByTestId(`execution-row-${executionUrn}`);
  }

  getExecutionSourceNameLocator(executionUrn: string): Locator {
    return this.getExecutionRowLocator(executionUrn).getByTestId('ingestion-source-name');
  }

  getFilterDropdownOption(sourceName: string): Locator {
    return this.filterDropdown.getByText(sourceName);
  }

  async filterBySource(sourceName: string): Promise<void> {
    this.logger?.step('filter by source', { sourceName });
    await this.sourceNameFilter.click();
    await this.dropdownSearchBar.fill(sourceName);
    await this.getFilterDropdownOption(sourceName).click();
    await this.footerUpdateButton.click();
  }

  async clickExecutionSourceNameLink(executionUrn: string): Promise<void> {
    this.logger?.step('click execution source name link', { executionUrn });
    await this.getExecutionSourceNameLocator(executionUrn).click();
  }

  async expectExecutionRowVisible(executionUrn: string): Promise<void> {
    await expect(this.getExecutionRowLocator(executionUrn)).toBeVisible();
  }

  async expectExecutionRowHidden(executionUrn: string): Promise<void> {
    await expect(this.getExecutionRowLocator(executionUrn)).toBeHidden();
  }
}
