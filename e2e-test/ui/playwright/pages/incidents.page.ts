import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

const KAFKA_INCIDENTS_DATASET_URL =
  '/dataset/urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset-v2,PROD)/Incidents';

const BQ_INCIDENTS_DATASET_URL =
  '/dataset/urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)/Incidents';

// Maps from human-readable label (used in test constants) to the option.value stored
// in the GQL enum — which is also the data-testid suffix rendered by SimpleSelect
// (`data-testid="option-${option.value}"`).  Using testid-based clicks avoids the
// fragile text-search that can accidentally land on the wrong option when the
// dropdown is mid-animation.
const STAGE_LABEL_TO_VALUE: Record<string, string> = {
  Triage: 'TRIAGE',
  Investigation: 'INVESTIGATION',
  'In progress': 'WORK_IN_PROGRESS',
  Fixed: 'FIXED',
  'No action required': 'NO_ACTION_REQUIRED',
};

const PRIORITY_LABEL_TO_VALUE: Record<string, string> = {
  Critical: 'CRITICAL',
  High: 'HIGH',
  Medium: 'MEDIUM',
  Low: 'LOW',
};

const CATEGORY_LABEL_TO_VALUE: Record<string, string> = {
  Operational: 'OPERATIONAL',
  'Data Schema': 'DATA_SCHEMA',
  Field: 'FIELD',
  Freshness: 'FRESHNESS',
  SQL: 'SQL',
  Volume: 'VOLUME',
  Custom: 'CUSTOM',
};

const STATUS_LABEL_TO_VALUE: Record<string, string> = {
  Active: 'ACTIVE',
  Resolved: 'RESOLVED',
};

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
    this.createIncidentBtnWithSiblings = page.locator('[data-testid="create-incident-btn-main-with-siblings"]');
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
    // Multiple GraphQL queries run in parallel after navigating to the Incidents tab.
    // networkidle waits for them all to settle so assertions don't race the data load.
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Navigate to the Kafka incidents page and wait until a specific incident row is visible
   * within a given priority group, AND has at least one linked asset.
   *
   * Two separate retry loops handle two distinct ES indexing delays:
   *   1. The incident entity itself must be in ES so it appears in the `getEntityIncidents` query.
   *   2. The `IncidentOn` relationship must be indexed so that `linkedAssets.relationships` is
   *      non-empty.  Without this, the edit form's Linked Assets field is empty, and the
   *      `isLinkedAssetMissing` guard in IncidentEditor keeps the submit button permanently disabled.
   *
   * Both delays are addressed by retrying the full navigate → expand → inspect cycle until
   * the row is visible AND its `incident-linked-assets` cell shows a non-zero count.
   */
  async navigateToKafkaDatasetIncidentsAndWaitForRow(
    incidentTitle: string,
    groupPriority: string,
    { maxRetries = 8, retryDelayMs = 4000 }: { maxRetries?: number; retryDelayMs?: number } = {},
  ): Promise<void> {
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      await this.navigate(KAFKA_INCIDENTS_DATASET_URL);
      await this.page.waitForLoadState('networkidle');
      await this.expandGroupIfNeeded(groupPriority);

      const row = this.getIncidentRow(incidentTitle);
      const isRowVisible = await row.isVisible().catch(() => false);
      if (!isRowVisible) {
        if (attempt < maxRetries) await this.page.waitForTimeout(retryDelayMs);
        continue;
      }

      // Also wait for the linked assets cell to show a positive count so that the
      // edit form's `resourceUrns` field is pre-populated and the submit button is enabled.
      const assetsCell = row.locator('[data-testid="incident-linked-assets"]');
      const assetsCellText = await assetsCell.textContent().catch(() => '0');
      const assetCount = parseInt(assetsCellText?.trim() || '0', 10);
      if (assetCount > 0) {
        return;
      }

      if (attempt < maxRetries) {
        // The incident is indexed but relationships are not yet — wait and retry.
        await this.page.waitForTimeout(retryDelayMs);
      }
    }
    // Final attempt: let the caller's assertion handle the failure with its own timeout.
  }

  async navigateToBigQueryDatasetIncidents(params = ''): Promise<void> {
    await this.navigate(`${BQ_INCIDENTS_DATASET_URL}?is_lineage_mode=false${params ? `&${params}` : ''}`);
    // Same as navigateToKafkaDatasetIncidents: GraphQL responses are in-flight after navigation.
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
   *
   * Waits for the group row to appear in the DOM before checking its expanded state.
   * This is necessary because incident data loads asynchronously after navigation —
   * the group may not be present at the moment this method is called (e.g. in test 3
   * where Apollo cache is empty and the query must fetch from GMS).  Without the wait,
   * `isVisible()` returns false before the group renders, the expansion click is never
   * made, and the group stays collapsed once data loads (the `processed` flag in
   * `useGetExpandedTableGroupsFromEntityUrnInUrl` prevents the auto-expand from re-firing).
   */
  async expandGroupIfNeeded(priority: string): Promise<void> {
    const group = this.getIncidentGroup(priority);
    // Wait for the group row to appear in the DOM before checking its state.
    await group.waitFor({ state: 'attached', timeout: 15000 });
    const collapsedIcon = group.locator('[data-testid="group-header-collapsed-icon"]');
    const isCollapsed = await collapsedIcon.isVisible().catch(() => false);
    if (isCollapsed) {
      await collapsedIcon.click({ force: true });
      // Wait for the collapsed icon to disappear so downstream assertions
      // don't race the expand animation.
      await collapsedIcon.waitFor({ state: 'hidden', timeout: 10000 });
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
    // The siblings button is only rendered once the create-incident loading state clears;
    // waitFor ensures the element is in the DOM before we try to click.
    await this.createIncidentBtnWithSiblings.waitFor({ state: 'visible', timeout: 10000 });
    await this.createIncidentBtnWithSiblings.click();
  }

  /** Select the first sibling option from the AntD dropdown that appears after clicking the siblings button. */
  async selectFirstSiblingFromDropdown(): Promise<void> {
    const firstItem = this.page.locator('.ant-dropdown-menu-item').first();
    // Ant Design renders dropdown menu items asynchronously after the trigger click;
    // the items are not in the DOM until the dropdown animation completes.
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

  /**
   * Click a dropdown trigger, then select an option by its `data-testid="option-${optionValue}"`.
   * Using the enum value as a testid key is more reliable than text matching because it avoids
   * races where the dropdown animation places the wrong element under the click point.
   */
  async selectDropdownOptionByValue(trigger: Locator, optionsList: Locator, optionValue: string): Promise<void> {
    await trigger.scrollIntoViewIfNeeded();
    await trigger.click();
    const option = optionsList.locator(`[data-testid="option-${optionValue}"]`);
    await option.waitFor({ state: 'visible', timeout: 10000 });
    await option.scrollIntoViewIfNeeded();
    await option.click({ force: true });
  }

  /** Fallback: select by visible text when no enum-value map is available. */
  async selectDropdownOption(trigger: Locator, optionsList: Locator, optionText: string): Promise<void> {
    await trigger.scrollIntoViewIfNeeded();
    await trigger.click();
    const option = optionsList.getByText(optionText, { exact: true });
    await option.waitFor({ state: 'visible', timeout: 10000 });
    await option.scrollIntoViewIfNeeded();
    await option.click({ force: true });
  }

  async selectCategory(category: string): Promise<void> {
    const value = CATEGORY_LABEL_TO_VALUE[category];
    if (value) {
      await this.selectDropdownOptionByValue(this.categorySelectTrigger, this.categoryOptionsList, value);
    } else {
      await this.selectDropdownOption(this.categorySelectTrigger, this.categoryOptionsList, category);
    }
  }

  async selectPriority(priority: string): Promise<void> {
    const value = PRIORITY_LABEL_TO_VALUE[priority];
    if (value) {
      await this.selectDropdownOptionByValue(this.prioritySelectTrigger, this.priorityOptionsList, value);
    } else {
      await this.selectDropdownOption(this.prioritySelectTrigger, this.priorityOptionsList, priority);
    }
  }

  async selectStage(stage: string): Promise<void> {
    const value = STAGE_LABEL_TO_VALUE[stage];
    // Retry up to 3 times: the stage dropdown can silently fail to register when
    // an in-flight React re-render (e.g. linked-asset loading) commits between the
    // trigger click and the option click, causing the dropdown to close prematurely.
    // IncidentStagePill renders <div title="${stage}"> inside the SelectBase; a
    // missing title means the selection did not take effect and we should retry.
    for (let attempt = 0; attempt < 3; attempt++) {
      if (value) {
        await this.selectDropdownOptionByValue(this.stageSelectTrigger, this.stageOptionsList, value);
      } else {
        await this.selectDropdownOption(this.stageSelectTrigger, this.stageOptionsList, stage);
      }
      const confirmed = await this.stageSelectTrigger
        .locator(`[title="${stage}"]`)
        .isVisible()
        .catch(() => false);
      if (confirmed) return;
      await this.page.waitForTimeout(300);
    }
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
    const statusValue = STATUS_LABEL_TO_VALUE[status];
    const option = statusValue
      ? this.statusOptionsList.locator(`[data-testid="option-${statusValue}"]`)
      : this.statusOptionsList.getByText(status, { exact: true });
    await option.waitFor({ state: 'visible', timeout: 10000 });
    await option.scrollIntoViewIfNeeded();
    await option.click({ force: true });
  }

  async selectFirstAssignee(): Promise<void> {
    await this.assigneesSelectTrigger.scrollIntoViewIfNeeded();
    await this.assigneesSelectTrigger.click();
    const firstLabel = this.assigneesOptionsList.locator('label').first();
    // Ant Design Select renders options asynchronously after the trigger click;
    // the option list is not in the DOM until the open animation completes.
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
    // The save button can remain disabled while linked assets are loading (isLoadingAssigneeOrAssets).
    // Wait for it to be enabled before clicking to avoid a premature click on a disabled button.
    await expect(this.saveButton).toBeEnabled({ timeout: 30000 });
    await this.saveButton.click();
    // Wait for the drawer/dialog to close after successful submission
    await this.saveButton.waitFor({ state: 'hidden', timeout: 15000 });
    // After submission the incident list re-fetches via GraphQL; networkidle ensures
    // the updated data has loaded before assertions run.
    await this.page.waitForLoadState('networkidle');
  }

  childOption(status: string): Locator {
    return this.page.locator(`[data-testid="child-option-${status}"]`);
  }

  async filterByStatus(status: string): Promise<void> {
    // The filter SelectBase is gated on IntersectionObserver (useIsVisible). Scroll
    // the element into the viewport so the observer fires and the SelectBase renders.
    await this.filterBase.scrollIntoViewIfNeeded();
    await this.filterBase.click();

    // The NestedSelect renders parent categories collapsed by default.
    // The status options live under the "state" parent — expand it first so that
    // the child option elements are rendered in the DOM before we try to click.
    const stateParent = this.page.locator('[data-testid="parent-option-state"]');
    await stateParent.waitFor({ state: 'visible', timeout: 10000 });
    const childOption = this.childOption(status);
    const childVisible = await childOption.isVisible().catch(() => false);
    if (!childVisible) {
      await stateParent.click();
    }

    await childOption.waitFor({ state: 'visible', timeout: 10000 });
    await childOption.click();
    // Close the filter dropdown by clicking the base again.
    await this.filterBase.click();
  }

  async clickIncidentRow(incidentTitle: string): Promise<void> {
    await this.getIncidentRow(incidentTitle).click();
  }

  async clickEditIcon(): Promise<void> {
    // The edit icon is conditionally rendered after the incident drawer opens and its
    // data loads; waitFor prevents a race between the drawer animation and the click.
    await this.editIncidentIcon.waitFor({ state: 'visible', timeout: 10000 });
    await this.editIncidentIcon.click();
  }

  async clearAndFillName(name: string): Promise<void> {
    await this.incidentNameInput.clear();
    await this.incidentNameInput.fill(name);
  }

  /**
   * Clear and refill the Remirror rich-text editor.
   * Remirror does not support fill() for clearing — select-all then pressSequentially.
   */
  async clearAndFillDescription(description: string, editorIndex = 0): Promise<void> {
    const editor = this.descriptionEditor.nth(editorIndex);
    await editor.click();
    await editor.press('Control+a');
    await editor.press('Backspace');
    await editor.pressSequentially(description);
    await expect(editor).toContainText(description);
  }

  async expectIncidentNameBadgeVisible(incidentTitle: string): Promise<void> {
    await this.getIncidentNameBadge(incidentTitle).scrollIntoViewIfNeeded();
    await expect(this.getIncidentNameBadge(incidentTitle)).toBeVisible();
  }

  async expectDrawerTitleContains(text: string, timeout = 30000): Promise<void> {
    await expect(this.drawerHeaderTitle).toContainText(text, { timeout });
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
