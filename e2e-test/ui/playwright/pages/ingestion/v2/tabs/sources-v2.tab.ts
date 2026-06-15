import { type Locator, type Page, expect } from '@playwright/test';
import {
  SourcesBaseTab,
  UpdateIngestionSourceOptions,
  type CreateIngestionSourceOptions,
} from '../../base/tabs/sources-base.tab';
import type { DataHubLogger } from '../../../../utils/logger';
import { SnowflakeSource } from '../../base/sources/SnowflakeSource';
import { CustomSource } from '@pages/ingestion/base/sources/CustomSource';

export class SourcesV2Tab extends SourcesBaseTab {
  readonly recipeBuilderNextButton: Locator;
  readonly recipeBuilderYamlButton: Locator;
  readonly scheduleNextButton: Locator;
  readonly sourceNameInput: Locator;
  readonly saveButton: Locator;
  readonly saveAndRunButton: Locator;
  readonly cliVersionInput: Locator;
  readonly configureScheduleHeading: Locator;
  readonly giveSourceNameHeading: Locator;
  readonly advancedButton: Locator;
  readonly editDataSourceHeading: Locator;

  // Sources
  readonly customSource: CustomSource;
  readonly snowflakeSource: SnowflakeSource;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    super(page, logger, logDir);

    this.recipeBuilderNextButton = page.getByTestId('recipe-builder-next-button');
    this.recipeBuilderYamlButton = page.getByTestId('recipe-builder-yaml-button');
    this.scheduleNextButton = page.getByTestId('ingestion-schedule-next-button');

    this.sourceNameInput = page.getByTestId('source-name-input');
    this.saveButton = page.getByTestId('ingestion-source-save-button');
    this.saveAndRunButton = page.getByTestId('ingestion-source-save-and-run-button');
    this.cliVersionInput = page.getByTestId('cli-version-input');

    this.configureScheduleHeading = page.getByTestId('configure-schedule-heading');
    this.giveSourceNameHeading = page.getByTestId('give-source-name-heading');
    this.advancedButton = page.getByTestId('advanced-settings-header');
    this.editDataSourceHeading = page.getByRole('dialog', { name: 'Edit Data Source' });

    this.customSource = new CustomSource(page, logger, logDir);
    this.snowflakeSource = new SnowflakeSource(page, logger, logDir);
  }

  async openCreateSourceModal(): Promise<void> {
    this.logger?.step('open create source modal');
    await this.createSourceButton.click();
    await this.sourceTypeSearchInput.waitFor({ state: 'visible' });
  }

  async clickNextButtonOnRecipeBuilderStep(): Promise<void> {
    this.logger?.step('click next button on recipe builder step');
    await this.recipeBuilderNextButton.scrollIntoViewIfNeeded();
    await this.recipeBuilderNextButton.click();
  }

  async clickNextButtonOnScheduleStep(): Promise<void> {
    this.logger?.step('click next button on schedule step');
    await this.scheduleNextButton.scrollIntoViewIfNeeded();
    await this.scheduleNextButton.click();
  }

  async setScheduleHour(hour: string): Promise<void> {
    this.logger?.step('set schedule hour', { hour });
    await this.cronHoursSelect.scrollIntoViewIfNeeded();
    const clearButton = this.getCronHoursClearButton();
    if (await clearButton.isVisible()) {
      await clearButton.click();
    }
    await this.cronHoursSelect.click();
    const option = this.getScheduleHourOption(hour);
    await option.click();
    await expect(this.cronHoursSelect).toContainText(hour);
  }

  async createIngestionSource(sourceName: string, options: CreateIngestionSourceOptions): Promise<string> {
    this.logger?.step('create ingestion source', { sourceName });
    const { sourceType, fillForm, schedule, shouldRun = false, cliVersion } = options;

    await this.openCreateSourceModal();

    await this.selectSourceType(sourceType);

    // Recipe builder step
    if (fillForm) {
      await fillForm(this.page);
    }
    await this.clickNextButtonOnRecipeBuilderStep();

    // Schedule step
    await expect(this.configureScheduleHeading).toBeVisible();
    if (schedule) {
      await this.setScheduleHour(schedule.hour);
    }
    await this.clickNextButtonOnScheduleStep();

    // Name and advanced settings step
    await expect(this.giveSourceNameHeading).toBeVisible();
    await this.setSourceName(sourceName);
    if (cliVersion) {
      await this.advancedButton.click();
      await this.cliVersionInput.fill(cliVersion);
    }

    // Save the source
    return shouldRun ? this.saveSource({ shouldRun: true }) : this.saveSource();
  }

  async updateIngestionSource(sourceUrn: string, options: UpdateIngestionSourceOptions): Promise<string> {
    this.logger?.step('update ingestion source', { sourceUrn });

    const { sourceName: newSourceName, displayName, verifyForm, fillForm } = options;

    if (displayName) {
      await this.searchIfNotVisible(displayName);
    }
    await this.openMoreOptions(displayName!);
    await this.clickDropdownItem('Edit');
    await expect(this.editDataSourceHeading).toBeVisible();

    if (verifyForm) {
      await verifyForm(this.page);
    }

    if (fillForm) {
      await fillForm(this.page);
    }

    await this.recipeBuilderNextButton.scrollIntoViewIfNeeded();
    await this.recipeBuilderNextButton.click();
    await expect(this.configureScheduleHeading).toBeVisible();

    if (options.schedule) {
      await this.setScheduleHour(options.schedule.hour);
    }

    await this.scheduleNextButton.click();
    if (newSourceName) {
      await this.setSourceName(newSourceName);
    }

    const responsePromise = this.graphql.waitForGraphQLResponse('getIngestionSource');
    const updatedUrn = await this.saveUpdatedSource();
    await responsePromise;
    return updatedUrn;
  }
}
