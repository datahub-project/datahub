import { type Locator, type Page, expect } from '@playwright/test';
import {
  SourcesBaseTab,
  UpdateIngestionSourceOptions,
  type CreateIngestionSourceOptions,
} from '../../base/tabs/sources-base.tab';
import type { DataHubLogger } from '../../../../utils/logger';
import { SnowflakeSourceV3 } from '../sources/SnowflakeSourceV3';
import { CustomSource } from '@pages/ingestion/base/sources/CustomSource';
import { LONG_TIMEOUT } from '@utils/constants';

export class SourcesV3Tab extends SourcesBaseTab {
  readonly scheduleEnabledSwitch: Locator;
  readonly expandCollapseButton: Locator;
  readonly runDetailsSummaryTab: Locator;
  readonly runDetailsLogsTab: Locator;
  readonly runDetailsRecipeTab: Locator;
  readonly runDetailsPageHeader: Locator;
  readonly runDetailsStatusPill: Locator;
  readonly manageDataSourcesBreadcrumb: Locator;
  readonly sourceNameInput: Locator;
  readonly saveButton: Locator;
  readonly saveAndRunButton: Locator;
  readonly cliVersionInput: Locator;
  readonly syncScheduleHeading: Locator;
  readonly cancelSourceButton: Locator;

  readonly customSource: CustomSource;
  readonly snowflakeSource: SnowflakeSourceV3;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    super(page, logger, logDir);

    this.scheduleEnabledSwitch = page.getByTestId('schedule-enabled-switch');
    this.expandCollapseButton = page.getByTestId('expand-collapse-button');

    this.runDetailsSummaryTab = page.getByTestId('run-details-summary-tab');
    this.runDetailsLogsTab = page.getByTestId('run-details-logs-tab');
    this.runDetailsRecipeTab = page.getByTestId('run-details-recipe-tab');
    this.runDetailsPageHeader = page.getByTestId('page-title');
    this.runDetailsStatusPill = page.getByTestId('run-details-status-pill');
    this.manageDataSourcesBreadcrumb = page.getByTestId('breadcrumb-back');

    this.sourceNameInput = page.getByTestId('data-source-name');
    this.saveButton = page.getByTestId('save-button');
    this.saveAndRunButton = page.getByTestId('save-and-run-button');
    this.cliVersionInput = page.getByTestId('cli-version-input').getByRole('textbox');
    this.syncScheduleHeading = page.getByTestId('sync-schedule-section');
    this.cancelSourceButton = page.getByTestId('cancel-ingestion-source-button');
    this.snowflakeSource = new SnowflakeSourceV3(page, logger);
    this.customSource = new CustomSource(page, logger);
  }

  getRunStatusCell(sourceName: string): Locator {
    return this.getSourceRow(sourceName).getByTestId('ingestion-source-table-status');
  }

  getSuccessStatusLocator(sourceName: string): Locator {
    return this.getRunStatusCell(sourceName).getByText('Success', { exact: false });
  }

  async openCreateSourceModal(): Promise<void> {
    this.logger?.step('open create source modal');
    await this.createSourceButton.click();
    await this.sourceTypeSearchInput.waitFor({ state: 'visible' });
  }

  async setScheduleHour(hour: string): Promise<void> {
    this.logger?.step('set schedule hour', { hour });
    const clearButton = this.getCronHoursClearButton();
    if (await clearButton.isVisible()) {
      await clearButton.click();
    }
    await this.cronHoursSelect.click();
    const option = this.getScheduleHourOption(hour);
    await expect(option).toBeVisible();
    await option.click();
    await expect(this.cronHoursSelect).toContainText(hour);
  }

  async enableSchedule(): Promise<void> {
    this.logger?.step('enable schedule toggle');
    const isScheduleEnabled = await this.scheduleEnabledSwitch.isChecked();
    if (!isScheduleEnabled) {
      await this.scheduleEnabledSwitch.click();
    }
  }

  async disableSchedule(): Promise<void> {
    this.logger?.step('disable schedule toggle');
    const isScheduleEnabled = await this.scheduleEnabledSwitch.isChecked();
    if (isScheduleEnabled) {
      await this.scheduleEnabledSwitch.click();
    }
  }

  async createIngestionSource(sourceName: string, options: CreateIngestionSourceOptions): Promise<string> {
    this.logger?.step('create ingestion source', { sourceName });
    const { sourceType, fillForm, schedule, shouldRun = false, cliVersion } = options;

    await this.openCreateSourceModal();

    await this.selectSourceType(sourceType);

    if (fillForm) {
      await fillForm(this.page);
    }

    await this.setSourceName(sourceName);

    await this.syncScheduleHeading.scrollIntoViewIfNeeded();
    if (schedule) {
      if (schedule.enabled) {
        await this.enableSchedule();
      } else {
        await this.disableSchedule();
      }
      await this.setScheduleHour(schedule.hour);
    }

    if (cliVersion) {
      await this.expandCollapseButton.click();
      await this.cliVersionInput.fill(cliVersion);
    }

    if (shouldRun) {
      await this.saveAndRunButton.scrollIntoViewIfNeeded();
      const mutationRequest = this.graphql.waitForGraphQLResponse('createIngestionSource');
      await this.saveAndRunButton.click();
      const mutationResp = await mutationRequest;
      const createdUrn = this.extractUrn(mutationResp, 'createIngestionSource');
      await expect(this.page.getByText(sourceName)).toBeVisible();
      await expect(this.getSuccessStatusLocator(sourceName)).toBeVisible({ timeout: LONG_TIMEOUT });
      return createdUrn;
    }
    return this.saveSource();
  }

  async updateIngestionSource(
    sourceUrn: string,
    options: UpdateIngestionSourceOptions & { displayName: string },
  ): Promise<string> {
    this.logger?.step('update ingestion source', { sourceUrn });

    const { sourceName: newSourceName, displayName, verifyForm, fillForm, schedule } = options;

    await this.searchIfNotVisible(displayName);
    await this.openMoreOptions(displayName);
    await this.clickDropdownItem('Edit');

    if (verifyForm) {
      await verifyForm(this.page);
    }

    if (newSourceName) {
      await this.sourceNameInput.focus();
      await this.sourceNameInput.fill(newSourceName);
    }

    await this.syncScheduleHeading.scrollIntoViewIfNeeded();
    if (schedule) {
      if (schedule.enabled) {
        await this.enableSchedule();
      } else {
        await this.disableSchedule();
      }
      await this.setScheduleHour(schedule.hour);
    }

    if (fillForm) {
      await fillForm(this.page);
    }

    const responsePromise = this.graphql.waitForGraphQLResponse('getIngestionSource');
    const updatedUrn = await this.saveUpdatedSource();
    await responsePromise;
    return updatedUrn;
  }

  async clickRunDetails(sourceName: string): Promise<void> {
    this.logger?.step('click run details', { sourceName });
    await this.getRunStatusCell(sourceName).click();
  }

  async expectRunDetailsPageVisible(): Promise<void> {
    await expect(this.runDetailsPageHeader).toBeVisible();
    await expect(this.runDetailsSummaryTab).toBeVisible();
    await expect(this.runDetailsLogsTab).toBeVisible();
    await expect(this.runDetailsRecipeTab).toBeVisible();
  }

  async expectRunSuccessVisible(): Promise<void> {
    await expect(this.runDetailsStatusPill).toContainText('Success');
  }

  async cancelCreateSourceModal(): Promise<void> {
    this.logger?.step('cancel create source');
    await this.cancelSourceButton.click();
  }

  async navigateBackToIngestion(): Promise<void> {
    this.logger?.step('navigate back to ingestion via breadcrumb');
    await this.manageDataSourcesBreadcrumb.click();
  }
}
