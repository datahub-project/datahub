import { type Locator, type Page, expect } from '@playwright/test';
import { GraphQLHelper } from '../../../../helpers/graphql-helper';
import type { DataHubLogger } from '../../../../utils/logger';
import { BaseTab } from './base.tab';
import { ConfirmationModalComponent } from '@pages/common/confirmation-modal-component';
import { ToastComponent } from '@pages/common/toast-component';
import { LONG_TIMEOUT } from '@utils/constants';
import {
  DISPLAY_NAME_USERNAME_PASSWORD,
  DISPLAY_NAME_PRIVATE_KEY,
} from '@pages/ingestion/base/sources/SnowflakeSource';

const EXECUTION_STATUS_RUNNING = 'RUNNING';
const EXECUTION_STATUS_PENDING = 'PENDING';
const SOURCE_STATUS_PENDING = 'Pending';

export interface SnowflakeSourceDetails {
  account_id: string;
  warehouse_id: string;
  username: string;
  password?: string;
  passwordSecret?: string;
  private_key?: string;
  role: string;
  authentication_type: typeof DISPLAY_NAME_USERNAME_PASSWORD | typeof DISPLAY_NAME_PRIVATE_KEY;
}

export interface ScheduleOptions {
  enabled: boolean;
  hour: string;
}

export type SourceFormFiller = (page: Page) => Promise<void>;

export interface CreateIngestionSourceOptions {
  sourceType: string;
  fillForm?: SourceFormFiller;
  schedule?: ScheduleOptions;
  shouldRun?: boolean;
  cliVersion?: string;
}

export interface UpdateIngestionSourceOptions {
  sourceName?: string;
  displayName?: string;
  verifyForm?: SourceFormFiller;
  fillForm?: SourceFormFiller;
  schedule?: ScheduleOptions;
}

export interface SnowflakeIngestionSourceOptions {
  sourceDetails?: SnowflakeSourceDetails;
  schedule?: ScheduleOptions;
  shouldRun?: boolean;
  verifyYaml?: boolean;
}

export interface SnowflakeIngestionSourceUpdateOptions extends SnowflakeIngestionSourceOptions {
  sourceName?: string;
}

export const DEFAULT_SOURCE_DETAILS: SnowflakeSourceDetails = {
  account_id: 'test_account',
  warehouse_id: 'test_warehouse',
  username: 'test_user',
  password: 'test_password',
  role: 'test_role',
  authentication_type: DISPLAY_NAME_USERNAME_PASSWORD,
};

export abstract class SourcesBaseTab extends BaseTab {
  readonly name: string;
  readonly path: string;
  readonly tabKey: string;

  readonly createSourceButton: Locator;
  readonly sourcesSearchInput: Locator;
  readonly typeFilterSelect: Locator;
  readonly cliPill: Locator;
  readonly sourceTypeSearchInput: Locator;
  readonly passwordInput: Locator;
  readonly cronHoursSelect: Locator;
  readonly cancelModalCloseButton: Locator;
  readonly secretDropdown: Locator;
  abstract readonly sourceNameInput: Locator;
  abstract readonly saveButton: Locator;
  abstract readonly saveAndRunButton: Locator;
  abstract readonly cliVersionInput: Locator;

  protected readonly graphql: GraphQLHelper;
  protected readonly toast: ToastComponent;

  // Locators for the inline secret creation dialog (used in createSecretInlineForPassword).
  // Scoped to the ARIA dialog to avoid strict-mode violations when hidden modal instances
  // remain in the DOM (Ant Design's default behavior).
  protected readonly inlineSecretDialog: Locator;
  protected readonly inlineSecretNameInput: Locator;
  protected readonly inlineSecretValueInput: Locator;
  protected readonly inlineSecretDescriptionInput: Locator;
  protected readonly inlineSecretCreateButton: Locator;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    super(page, logger, logDir);

    this.name = 'Sources';
    this.path = '/ingestion/sources';
    this.tabKey = 'Sources';

    this.graphql = new GraphQLHelper(page);
    this.toast = new ToastComponent(page);

    this.inlineSecretDialog = page.getByRole('dialog', { name: 'Create a new Secret' });
    this.inlineSecretNameInput = this.inlineSecretDialog.getByTestId('secret-modal-name-input').getByRole('textbox');
    this.inlineSecretValueInput = this.inlineSecretDialog.getByTestId('secret-modal-value-input').getByRole('textbox');
    this.inlineSecretDescriptionInput = this.inlineSecretDialog
      .getByTestId('secret-modal-description-input')
      .getByRole('textbox');
    this.inlineSecretCreateButton = this.inlineSecretDialog.getByTestId('secret-modal-create-button');

    this.createSourceButton = page.getByTestId('create-ingestion-source-button');
    this.sourcesSearchInput = page.getByTestId('ingestion-sources-search');
    this.typeFilterSelect = page.getByTestId('ingestions-type-filter');
    this.cliPill = page.getByTestId('ingestion-source-cli-pill');
    this.sourceTypeSearchInput = page.getByTestId('source-type-search-input');
    // eslint-disable-next-line playwright/no-raw-locators -- recipe form field addressed by generated id; no data-testid
    this.passwordInput = page.locator('#password');
    // eslint-disable-next-line playwright/no-raw-locators -- cron builder Ant Design select; no data-testid or ARIA role
    this.cronHoursSelect = page.locator('.cron-builder-hours .ant-select');
    this.cancelModalCloseButton = page.getByTestId('connect-data-source-modal').getByTestId('modal-close-icon');
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design select dropdown portal; no data-testid or ARIA role
    this.secretDropdown = page.locator('.ant-select-dropdown').and(page.locator(':visible'));
  }

  async fillTextField(locator: Locator, value: string): Promise<void> {
    await locator.scrollIntoViewIfNeeded();
    await locator.clear();
    await locator.fill(value);
  }

  getSourceRow(sourceName: string): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- :visible pseudo-class disambiguates duplicate rows; no role/testid equivalent
    return this.page.getByTestId(`row-${sourceName}`).and(this.page.locator(':visible'));
  }

  getSourceCell(sourceName: string): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- first table cell selected by tag; cells carry no per-cell data-testid
    return this.getSourceRow(sourceName).locator('td').first();
  }

  getRunButtonLocator(sourceName: string): Locator {
    return this.getSourceRow(sourceName).getByTestId('run-ingestion-source-button');
  }

  getMoreOptionsButton(sourceName: string): Locator {
    return this.getSourceRow(sourceName).getByTestId('ingestion-more-options');
  }

  getDropdownMenuItem(label: string): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design dropdown menu portal; no data-testid or ARIA role
    return this.page.locator('body .ant-dropdown-menu').getByText(label);
  }

  getTypeFilterDropdownItem(value: string): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design dropdown portal; no data-testid or ARIA role
    return this.page.locator('body .ant-dropdown').getByText(value);
  }

  getLastRunCell(sourceName: string): Locator {
    return this.getSourceRow(sourceName).getByTestId('ingestion-source-last-run');
  }

  getSourceTypeOption(typeName: string): Locator {
    return (
      this.page
        .getByTestId(/^source-option-/)
        .filter({ hasText: typeName })
        // eslint-disable-next-line playwright/no-raw-locators -- :visible pseudo-class disambiguates duplicate nodes; no role/testid equivalent
        .and(this.page.locator(':visible'))
    );
  }

  getCronHoursClearButton(): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design select clear affordance; no data-testid or ARIA role
    return this.cronHoursSelect.locator('.ant-select-clear');
  }

  getScheduleHourOption(hour: string): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design time option keyed by title attribute; no data-testid
    return this.page.locator(`[title="${hour}"]`).and(this.page.locator(':visible'));
  }

  async clickRunButton(sourceName: string): Promise<void> {
    this.logger?.step('click run button', { sourceName });
    await this.getRunButtonLocator(sourceName).click();
  }

  async confirmExecution(): Promise<void> {
    this.logger?.step('confirm execution dialog');
    const modal = new ConfirmationModalComponent(this.page);
    await modal.waitForOpening();
    await modal.confirm();
  }

  async openMoreOptions(sourceName: string): Promise<void> {
    this.logger?.step('open more options', { sourceName });
    await this.getMoreOptionsButton(sourceName).click();
  }

  async clickDropdownItem(label: string): Promise<void> {
    await this.getDropdownMenuItem(label).click();
  }

  async selectTypeFilter(value: string): Promise<void> {
    this.logger?.step('select type filter', { value });
    await this.typeFilterSelect.click();
    await this.getTypeFilterDropdownItem(value).click();
  }

  async clickLastRunCell(sourceName: string): Promise<void> {
    this.logger?.step('click last run cell', { sourceName });
    await this.getLastRunCell(sourceName).click();
  }

  async clearSearch(): Promise<void> {
    this.logger?.step('clear search');
    const inputValue = await this.sourcesSearchInput.inputValue();
    if (inputValue) {
      const responsePromise = this.graphql.waitForGraphQLResponse('listIngestionSources');
      await this.sourcesSearchInput.clear();
      await responsePromise;
    } else {
      await this.sourcesSearchInput.clear();
    }
  }

  async search(sourceName: string): Promise<void> {
    this.logger?.step('search', { sourceName });
    await expect(this.sourcesSearchInput).toBeVisible();
    const responsePromise = this.graphql.waitForGraphQLResponse('listIngestionSources');
    await this.sourcesSearchInput.fill(sourceName);
    await responsePromise;
  }

  async expectSchedule(sourceName: string, scheduleText: string): Promise<void> {
    const row = this.getSourceRow(sourceName);
    // The schedule data-testid appears on both the <td> cell and the <span> inside it.
    // Use .first() to avoid strict-mode violations.
    await expect(row.getByTestId('schedule').first()).toContainText(scheduleText);
  }

  async selectSourceType(typeName: string): Promise<void> {
    this.logger?.step('select source type', { typeName });
    await this.sourceTypeSearchInput.fill(typeName);
    const option = this.getSourceTypeOption(typeName);
    await option.scrollIntoViewIfNeeded();
    await option.click();
  }

  abstract openCreateSourceModal(): Promise<void>;

  abstract setScheduleHour(hour: string): Promise<void>;

  abstract createIngestionSource(sourceName: string, options: CreateIngestionSourceOptions): Promise<string>;

  abstract updateIngestionSource(sourceUrn: string, options: UpdateIngestionSourceOptions): Promise<string>;

  async setSourceName(name: string): Promise<void> {
    this.logger?.step('set source name', { name });
    await this.sourceNameInput.clear();
    await this.sourceNameInput.fill(name);
  }

  protected extractUrn(resp: unknown, key: string): string {
    const data = (resp as { data?: Record<string, string | undefined> })?.data;
    return data?.[key] ?? '';
  }

  async saveSource(options?: { shouldRun?: boolean }): Promise<string> {
    this.logger?.step('save source');
    const saveButton = options?.shouldRun ? this.saveAndRunButton : this.saveButton;
    await saveButton.scrollIntoViewIfNeeded();
    const mutationRequest = this.graphql.waitForGraphQLResponse('createIngestionSource');
    const refetchRequest = this.graphql.waitForGraphQLResponse('getIngestionSource');
    await saveButton.click();
    const mutationResp = await mutationRequest;
    await refetchRequest;
    return this.extractUrn(mutationResp, 'createIngestionSource');
  }

  async saveAndRunSource(): Promise<string> {
    this.logger?.step('save and run source');
    await this.saveAndRunButton.scrollIntoViewIfNeeded();
    const mutationRequest = this.graphql.waitForGraphQLResponse('createIngestionSource');
    await this.saveAndRunButton.click();
    await this.toast.expectVisible('Successfully created ingestion source!');
    const mutationResp = await mutationRequest;
    return this.extractUrn(mutationResp, 'createIngestionSource');
  }

  async saveUpdatedSource(): Promise<string> {
    this.logger?.step('save updated source');
    const mutationRequest = this.graphql.waitForGraphQLResponse('updateIngestionSource');
    await this.saveButton.click();
    const mutationResp = await mutationRequest;
    return this.extractUrn(mutationResp, 'updateIngestionSource');
  }

  async createSecretInlineForPassword(name: string, value: string, description?: string): Promise<string> {
    this.logger?.step('create secret inline for password', { name });
    await expect(this.secretDropdown).toBeHidden();
    await this.passwordInput.click();
    await this.page.getByText('Create Secret').click();
    await this.inlineSecretDialog.waitFor({ state: 'visible' });
    await this.inlineSecretNameInput.fill(name);
    await this.inlineSecretValueInput.fill(value);
    if (description) {
      await this.inlineSecretDescriptionInput.fill(description);
    }
    const mutationResponse = this.graphql.waitForGraphQLResponse('createSecret');
    await this.inlineSecretCreateButton.click();
    const resp = await mutationResponse;
    await this.toast.expectVisible('Created secret!');
    return this.extractUrn(resp, 'createSecret');
  }

  /**
   * Cancel any RUNNING or PENDING execution for sourceName before deleting it.
   * Without this, the executor continues processing runs after deletion,
   * consuming memory and potentially causing OOM crashes when many runs pile up.
   */
  protected async cancelRunningExecution(sourceName: string): Promise<void> {
    const listQuery = `
      query listIngestionSources($input: ListIngestionSourcesInput!) {
        listIngestionSources(input: $input) {
          ingestionSources {
            urn
            name
            executions(start: 0, count: 1) {
              executionRequests {
                urn
                result { status }
              }
            }
          }
        }
      }`;

    let resp: Record<string, unknown>;
    try {
      resp = await this.graphql.executeQuery(listQuery, {
        input: { start: 0, count: 100, query: sourceName },
      });
    } catch {
      return;
    }

    type IngestionSourcesResponse = {
      data?: {
        listIngestionSources?: {
          ingestionSources?: Array<{
            urn: string;
            name: string;
            executions?: {
              executionRequests?: Array<{
                urn: string;
                result?: { status?: string };
              }>;
            };
          }>;
        };
      };
    };

    const sources = (resp as IngestionSourcesResponse).data?.listIngestionSources?.ingestionSources ?? [];
    const source = sources.find((s) => s.name === sourceName);
    if (!source) return;

    const lastExec = source.executions?.executionRequests?.[0];
    if (!lastExec) return;

    const status = lastExec.result?.status;
    if (status && status !== EXECUTION_STATUS_RUNNING && status !== EXECUTION_STATUS_PENDING) return;

    const cancelMutation = `
      mutation cancelIngestionExecutionRequest($input: CancelIngestionExecutionRequestInput!) {
        cancelIngestionExecutionRequest(input: $input)
      }`;

    try {
      await this.graphql.executeQuery(cancelMutation, {
        input: {
          ingestionSourceUrn: source.urn,
          executionRequestUrn: lastExec.urn,
        },
      });
    } catch {
      // Best effort — continue with delete even if cancel fails.
    }
  }

  async searchIfNotVisible(sourceName: string): Promise<void> {
    const isSourceVisible = await this.getSourceRow(sourceName).isVisible();
    if (!isSourceVisible) {
      await this.clearSearch();
      await this.search(sourceName);
      await this.expectSourceVisible(sourceName);
    }
  }

  async deleteIngestionSource(sourceUrn: string, sourceName: string): Promise<void> {
    this.logger?.step('delete ingestion source', { sourceUrn });
    await this.cancelRunningExecution(sourceName);
    await this.searchIfNotVisible(sourceName);
    await this.openMoreOptions(sourceName);
    await this.clickDropdownItem('Delete');
    const modal = new ConfirmationModalComponent(this.page);
    await modal.waitForOpening();
    await modal.confirm();
    await this.toast.expectVisible('Removed ingestion source');
  }

  async runIngestionSource(sourceUrn: string, sourceName: string): Promise<void> {
    this.logger?.step('run ingestion source', { sourceUrn });
    await this.search(sourceName);
    await this.expectSourceVisible(sourceName);
    await this.clickRunButton(sourceName);
    await this.confirmExecution();
  }

  async expectSourceVisible(sourceName: string): Promise<void> {
    await expect(this.getSourceRow(sourceName)).toBeVisible();
  }

  async expectSourceNotVisible(sourceName: string): Promise<void> {
    await expect(this.getSourceRow(sourceName)).toBeHidden();
  }

  async expectSourceStatusContains(sourceName: string, status: string): Promise<void> {
    const row = this.getSourceRow(sourceName);
    await expect(row.getByText(status, { exact: false })).toBeVisible({ timeout: LONG_TIMEOUT });
  }

  async expectSourceStatusPending(sourceName: string): Promise<void> {
    await this.expectSourceStatusContains(sourceName, SOURCE_STATUS_PENDING);
  }

  async cancelCreateSourceModal(): Promise<void> {
    this.logger?.step('cancel create source modal');
    await this.cancelModalCloseButton.click();
  }

  async expectCliPillVisible(): Promise<void> {
    await expect(this.cliPill).toBeVisible();
  }

  async expectCliPillNotVisible(): Promise<void> {
    await expect(this.cliPill).toBeHidden();
  }
}
