import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base-page';
import type { DataHubLogger } from '../utils/logger';

const KAFKA_INCIDENTS_DATASET_URL =
  '/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset-v2,PROD)/Incidents';

const BQ_INCIDENTS_DATASET_URL =
  '/dataset/urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)/Incidents';

export class IncidentsPage extends BasePage {
  readonly createIncidentBtn: Locator;
  readonly createIncidentBtnWithSiblings: Locator;
  readonly incidentNameInput: Locator;
  readonly descriptionEditor: Locator;
  readonly categorySelectTrigger: Locator;
  readonly categoryOptionsList: Locator;
  readonly prioritySelectTrigger: Locator;
  readonly priorityOptionsList: Locator;
  readonly stageSelectTrigger: Locator;
  readonly stageOptionsList: Locator;
  readonly statusSelectTrigger: Locator;
  readonly statusOptionsList: Locator;
  readonly assigneesSelectTrigger: Locator;
  readonly assigneesOptionsList: Locator;
  readonly formContainer: Locator;
  readonly saveButton: Locator;
  readonly editIncidentIcon: Locator;
  readonly filterBase: Locator;
  readonly drawerHeaderTitle: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.createIncidentBtn = page.locator('[data-testid="create-incident-btn-main"]');
    this.createIncidentBtnWithSiblings = page.locator(
      '[data-testid="create-incident-btn-main-with-siblings"]',
    );
    this.incidentNameInput = page.locator('[data-testid="incident-name-input"]');
    // remirror-editor is the rich text editor used for incident description
    this.descriptionEditor = page.locator('.remirror-editor');
    this.categorySelectTrigger = page.locator('[data-testid="category-select-input-type"]');
    this.categoryOptionsList = page.locator('[data-testid="category-options-list"]');
    this.prioritySelectTrigger = page.locator('[data-testid="priority-select-input-type"]');
    this.priorityOptionsList = page.locator('[data-testid="priority-options-list"]');
    this.stageSelectTrigger = page.locator('[data-testid="stage-select-input-type"]');
    this.stageOptionsList = page.locator('[data-testid="stage-options-list"]');
    this.statusSelectTrigger = page.locator('[data-testid="status-select-input-type"]');
    this.statusOptionsList = page.locator('[data-testid="status-options-list"]');
    this.assigneesSelectTrigger = page.locator('[data-testid="incident-assignees-select-input-type"]');
    this.assigneesOptionsList = page.locator('[data-testid="incident-assignees-options-list"]');
    this.formContainer = page.locator('[data-testid="incident-editor-form-container"]');
    this.saveButton = page.locator('[data-testid="incident-create-button"]');
    this.editIncidentIcon = page.locator('[data-testid="edit-incident-icon"]');
    this.filterBase = page.locator('[data-testid="filter-base"]');
    this.drawerHeaderTitle = page.locator('[data-testid="drawer-header-title"]');
  }

  async navigateToKafkaDatasetIncidents(): Promise<void> {
    await this.navigate(KAFKA_INCIDENTS_DATASET_URL);
    await this.page.waitForLoadState('networkidle');
  }

  async navigateToBigQueryDatasetIncidents(params = ''): Promise<void> {
    await this.navigate(
      `${BQ_INCIDENTS_DATASET_URL}?is_lineage_mode=false${params ? `&${params}` : ''}`,
    );
    await this.page.waitForLoadState('networkidle');
  }

  getIncidentRow(incidentTitle: string): Locator {
    return this.page.locator(`[data-testid="incident-row-${incidentTitle}"]`);
  }

  getIncidentNameBadge(incidentTitle: string): Locator {
    return this.page.locator(`[data-testid="${incidentTitle}"]`);
  }

  getIncidentGroup(priority: string): Locator {
    return this.page.locator(`[data-testid="incident-group-${priority}"]`);
  }

  /**
   * Expand a priority group if it is currently collapsed.
   * Mirrors the `expandGroupIfNeeded` helper function in the Cypress test.
   */
  async expandGroupIfNeeded(priority: string): Promise<void> {
    const group = this.getIncidentGroup(priority);
    const collapsedIcon = group.locator('[data-testid="group-header-collapsed-icon"]');

    const isVisible = await collapsedIcon.isVisible().catch(() => false);
    if (isVisible) {
      await collapsedIcon.click({ force: true });
    }
  }

  async clickCreateIncidentBtn(): Promise<void> {
    // Wait for loading state to clear before trying to click.
    // The button cycles through data-testid="create-incident-btn-loading" → "create-incident-btn-main".
    await this.page
      .locator('[data-testid^="create-incident-btn"]:not([data-testid="create-incident-btn-loading"])')
      .waitFor({ state: 'visible', timeout: 15000 });
    await this.createIncidentBtn.click();
  }

  async clickCreateIncidentBtnWithSiblings(): Promise<void> {
    await this.createIncidentBtnWithSiblings.waitFor({ state: 'visible', timeout: 10000 });
    await this.createIncidentBtnWithSiblings.click();
  }

  /** Select the first sibling option from the AntD dropdown that appears after clicking the siblings button. */
  async selectFirstSiblingFromDropdown(): Promise<void> {
    const firstItem = this.page.locator('.ant-dropdown-menu-item').first();
    await firstItem.waitFor({ state: 'visible', timeout: 10000 });
    await firstItem.click();
  }

  async fillIncidentName(name: string): Promise<void> {
    await this.incidentNameInput.fill(name);
  }

  /**
   * Fill the Remirror rich-text description editor.
   * When there are multiple editors on the page (e.g. siblings mode), pass `editorIndex`.
   */
  async fillDescription(description: string, editorIndex = 0): Promise<void> {
    // If the requested index doesn't exist, fall back to the first editor.
    // In sibling mode the Cypress suite expected a second editor (index 1) but
    // the current implementation only renders one description editor per form.
    const count = await this.descriptionEditor.count();
    const resolvedIndex = editorIndex < count ? editorIndex : 0;
    const editor = this.descriptionEditor.nth(resolvedIndex);
    await editor.waitFor({ state: 'visible' });
    await editor.scrollIntoViewIfNeeded();
    await editor.click({ force: true });
    await editor.fill(description);
    await expect(editor).toContainText(description);
  }

  async selectDropdownOption(trigger: Locator, optionsList: Locator, optionText: string): Promise<void> {
    await trigger.scrollIntoViewIfNeeded();
    await trigger.click();
    const option = optionsList.getByText(optionText, { exact: true });
    await option.waitFor({ state: 'visible', timeout: 10000 });
    await option.scrollIntoViewIfNeeded();
    await option.click({ force: true });
  }

  async selectCategory(category: string): Promise<void> {
    await this.selectDropdownOption(this.categorySelectTrigger, this.categoryOptionsList, category);
  }

  async selectPriority(priority: string): Promise<void> {
    await this.selectDropdownOption(this.prioritySelectTrigger, this.priorityOptionsList, priority);
  }

  async selectStage(stage: string): Promise<void> {
    await this.selectDropdownOption(this.stageSelectTrigger, this.stageOptionsList, stage);
  }

  async selectStatus(status: string): Promise<void> {
    // The Status select uses SimpleSelect which only renders its clickable trigger
    // (SelectBase) when the Container element is visible in the viewport, determined
    // by IntersectionObserver via the useIsVisible hook. The Status field is at the
    // bottom of the edit form and may be off-screen.
    //
    // Strategy:
    // 1. Locate the always-present form label "Status" within the form container
    //    and scroll it into view. This brings the Container div into the viewport.
    // 2. IntersectionObserver fires → isVisible = true → SelectBase is rendered.
    // 3. Wait for statusSelectTrigger (data-testid on SelectBase) to be attached.
    // 4. Click it and select the option.

    // Scroll the Status label into view so IntersectionObserver can fire
    const statusLabel = this.formContainer.getByText('Status', { exact: true });
    await statusLabel.waitFor({ state: 'visible', timeout: 15000 });
    await statusLabel.scrollIntoViewIfNeeded();

    // Wait for the SelectBase to appear in the DOM (rendered once the Container
    // enters the viewport and IntersectionObserver sets isVisible = true)
    await this.statusSelectTrigger.waitFor({ state: 'attached', timeout: 10000 });
    await this.statusSelectTrigger.scrollIntoViewIfNeeded();
    await this.statusSelectTrigger.click();
    const option = this.statusOptionsList.getByText(status, { exact: true });
    await option.waitFor({ state: 'visible', timeout: 10000 });
    await option.scrollIntoViewIfNeeded();
    await option.click({ force: true });
  }

  async selectFirstAssignee(): Promise<void> {
    await this.assigneesSelectTrigger.scrollIntoViewIfNeeded();
    await this.assigneesSelectTrigger.click();
    const firstLabel = this.assigneesOptionsList.locator('label').first();
    await firstLabel.waitFor({ state: 'visible', timeout: 10000 });
    await firstLabel.scrollIntoViewIfNeeded();
    await firstLabel.click({ force: true });
  }

  /** Click outside of the form dropdowns to dismiss them before submitting. */
  async dismissDropdowns(): Promise<void> {
    // Click on the Name input (top of form) to dismiss any open dropdown
    // without closing the whole form/drawer.
    await this.incidentNameInput.click();
  }

  async submitIncidentForm(): Promise<void> {
    await this.saveButton.scrollIntoViewIfNeeded();
    await this.saveButton.click();
    // Wait for the drawer/dialog to close after successful submission
    await this.saveButton.waitFor({ state: 'hidden', timeout: 15000 });
    await this.page.waitForLoadState('networkidle');
  }

  async filterByStatus(status: string): Promise<void> {
    await this.filterBase.click();
    await this.page.locator(`[data-testid="child-option-${status}"]`).click();
    // Close the filter dropdown
    await this.filterBase.click();
  }

  async clickIncidentRow(incidentTitle: string): Promise<void> {
    await this.getIncidentRow(incidentTitle).click();
  }

  async clickEditIcon(): Promise<void> {
    await this.editIncidentIcon.waitFor({ state: 'visible', timeout: 10000 });
    await this.editIncidentIcon.click();
  }

  async clearAndFillName(name: string): Promise<void> {
    await this.incidentNameInput.clear();
    await this.incidentNameInput.fill(name);
  }

  /**
   * Clear and refill the Remirror rich-text editor.
   * Remirror does not support fill() for clearing — use triple-click then type.
   */
  async clearAndFillDescription(description: string, editorIndex = 0): Promise<void> {
    const editor = this.descriptionEditor.nth(editorIndex);
    await editor.click();
    await editor.press('Control+a');
    await editor.press('Backspace');
    await editor.type(description);
    await expect(editor).toContainText(description);
  }

  async expectIncidentRowExists(incidentTitle: string, timeout = 15000): Promise<void> {
    await expect(this.getIncidentRow(incidentTitle)).toBeVisible({ timeout });
  }

  async expectIncidentNameBadgeVisible(incidentTitle: string): Promise<void> {
    await this.getIncidentNameBadge(incidentTitle).scrollIntoViewIfNeeded();
    await expect(this.getIncidentNameBadge(incidentTitle)).toBeVisible();
  }

  async expectDrawerTitleContains(text: string, timeout = 30000): Promise<void> {
    await expect(this.drawerHeaderTitle).toContainText(text, { timeout });
  }

  async expectIncidentGroupVisible(priority: string, timeout = 15000): Promise<void> {
    await expect(this.getIncidentGroup(priority)).toBeVisible({ timeout });
  }

  async expectIncidentRowStage(incidentTitle: string, stage: string): Promise<void> {
    const row = this.getIncidentRow(incidentTitle);
    await expect(row.locator('[data-testid="incident-stage"]')).toContainText(stage);
  }

  async expectIncidentRowCategory(incidentTitle: string, category: string): Promise<void> {
    const row = this.getIncidentRow(incidentTitle);
    await expect(row.locator('[data-testid="incident-category"]')).toContainText(category);
  }

  async expectResolveButtonContains(incidentTitle: string, text: string): Promise<void> {
    const row = this.getIncidentRow(incidentTitle);
    await expect(row.locator('[data-testid="incident-resolve-button-container"]')).toContainText(text);
  }

  async expectResolveButtonVisible(incidentTitle: string): Promise<void> {
    const row = this.getIncidentRow(incidentTitle);
    await expect(row.locator('[data-testid="incident-resolve-button-container"]')).toBeVisible();
  }
}
