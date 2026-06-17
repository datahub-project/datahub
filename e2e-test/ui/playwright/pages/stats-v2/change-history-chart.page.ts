import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';

/**
 * Change History Chart Page Object
 *
 * Handles complex interactions specific to the Change History calendar:
 * - Day selection and popovers
 * - Operation type filtering
 * - User filtering via drawer
 * - Detailed change history display
 *
 * Usage:
 *   const changeHistory = new ChangeHistoryChart(page);
 *   await changeHistory.openChangeHistoryDrawer();
 *   await changeHistory.selectDay('15');
 *   await changeHistory.verifyOperationInPopover('COLUMN_ADDED', '5');
 */
export class ChangeHistoryChart extends BasePage {
  readonly drawer: Locator;
  readonly drawerTitle: Locator;
  readonly closeDrawerButton: Locator;

  // Filters
  readonly dateSwitch: Locator;
  readonly usersSelect: Locator;
  readonly typesSelect: Locator;

  // Calendar
  readonly calendar: Locator;
  readonly calendarDays: Locator;

  // Popover
  readonly popover: Locator;
  readonly popoverContent: Locator;

  // Timeline (inside drawer)
  readonly timeline: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.drawer = page.getByTestId('change-history-details');
    this.drawerTitle = this.drawer.getByRole('heading');
    this.closeDrawerButton = this.drawer.getByTestId('drawer-close-button');

    this.dateSwitch = page.getByTestId('date-switcher');
    this.usersSelect = page.getByTestId('users-select');
    this.typesSelect = page.getByTestId('types-select');

    // Calendar is rendered as days within the change-history-chart
    // The calendar container itself may not have explicit testid, so we reference the days
    this.calendar = page.getByTestId('change-history-chart');
    this.calendarDays = page.getByTestId(/^day-\d+$/);

    this.popover = page.getByRole('dialog');
    this.popoverContent = this.popover.getByTestId('day-popover');

    this.timeline = page.getByTestId('change-history-timeline');
  }

  // ── Drawer ────────────────────────────────────────────────────────────────

  /**
   * Verify drawer is open and visible
   */
  async verifyDrawerIsOpen(): Promise<void> {
    this.logger?.step('verifyDrawerIsOpen');
    await this.drawer.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Close the change history drawer
   */
  async closeDrawer(): Promise<void> {
    this.logger?.step('closeDrawer');
    await this.closeDrawerButton.click({ timeout: TIMEOUTS.SHORT });
    await this.drawer.waitFor({ state: 'hidden', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Force-close any open drawers from previous tests
   * Used in test setup/cleanup to ensure clean state
   */
  async forceCloseDrawer(): Promise<void> {
    this.logger?.step('forceCloseDrawer');
    try {
      // Try the drawer title container first
      const drawerTitleContainer = this.page.getByTestId('change-history-details-drawer-title-container');
      if ((await drawerTitleContainer.count()) > 0) {
        const closeButton = this.page.getByTestId('drawer-close-button');
        if ((await closeButton.count()) > 0) {
          await closeButton.click({ timeout: 500 }).catch(() => {});
        }
        await drawerTitleContainer.waitFor({ state: 'hidden', timeout: 500 }).catch(() => {});
      }

      // Also try the drawer element
      if ((await this.drawer.count()) > 0) {
        const closeButton = this.drawer.getByTestId('drawer-close-button');
        if ((await closeButton.count()) > 0) {
          await closeButton.click({ timeout: 500 }).catch(() => {});
        }
        await this.drawer.waitFor({ state: 'hidden', timeout: 500 }).catch(() => {});
      }

      // Force escape multiple times
      for (let i = 0; i < 3; i++) {
        await this.page.keyboard.press('Escape').catch(() => {});
      }
    } catch {
      // Ignore cleanup errors
    }
  }

  // ── Day Selection ─────────────────────────────────────────────────────────

  /**
   * Select a day in the calendar (opens popover)
   */
  async selectDay(dayNumber: string): Promise<void> {
    this.logger?.step('selectDay', { dayNumber });
    const day = this.page.getByTestId(`day-${dayNumber}`);
    await day.click({ timeout: TIMEOUTS.SHORT });
    await this.page.waitForTimeout(500); // Small delay for popover animation
  }

  /**
   * Verify a specific day has the expected number of changes
   */
  async verifyDayHasChanges(dayNumber: string, expectedChangeCount: number): Promise<void> {
    this.logger?.step('verifyDayHasChanges', { dayNumber, expectedChangeCount });
    const dayPopover = this.page.getByTestId(`day-popover-${dayNumber}`);
    const totalChanges = dayPopover.getByTestId('total-changes');
    await expect(totalChanges).toContainText(expectedChangeCount.toString());
  }

  // ── Popover Interactions ──────────────────────────────────────────────────

  /**
   * Verify operation type appears in popover with expected count
   */
  async verifyOperationInPopover(operationType: string, expectedCount: string): Promise<void> {
    this.logger?.step('verifyOperationInPopover', { operationType, expectedCount });
    const operation = this.popover.getByTestId(`operation-${operationType}`);
    await expect(operation).toContainText(expectedCount);
  }

  /**
   * Get total number of changes shown in popover
   */
  async getTotalChangesFromPopover(): Promise<string> {
    this.logger?.step('getTotalChangesFromPopover');
    const totalChanges = this.popover.getByTestId('total-changes');
    return totalChanges.textContent().then((content) => content?.match(/\d+/)?.[0] || '0');
  }

  /**
   * Verify "no changes this day" message appears
   */
  async verifyNoChangesForDay(): Promise<void> {
    this.logger?.step('verifyNoChangesForDay');
    const noChanges = this.popover.getByTestId('no-changes-this-day');
    await noChanges.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
  }

  // ── Filtering ─────────────────────────────────────────────────────────────

  /**
   * Select operation types to filter by (e.g., COLUMN_ADDED, COLUMN_DELETED)
   */
  async selectOperationTypes(operationTypes: string[]): Promise<void> {
    this.logger?.step('selectOperationTypes', { operationTypes });
    await this.typesSelect.click({ timeout: TIMEOUTS.SHORT });
    for (const opType of operationTypes) {
      await this.page.getByTestId(`option-${opType}`).click({ timeout: TIMEOUTS.SHORT });
    }
    await this.page.keyboard.press('Escape'); // Close select
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Select users to filter by
   */
  async selectUsers(userNames: string[]): Promise<void> {
    this.logger?.step('selectUsers', { userCount: userNames.length });
    await this.usersSelect.click({ timeout: TIMEOUTS.SHORT });
    for (const userName of userNames) {
      await this.page.getByTestId(`option-${userName}`).click({ timeout: TIMEOUTS.SHORT });
    }
    await this.page.keyboard.press('Escape'); // Close select
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Switch to a specific date using the date switcher
   */
  async switchToDate(dateString: string): Promise<void> {
    this.logger?.step('switchToDate', { dateString });
    await this.dateSwitch.click({ timeout: TIMEOUTS.SHORT });
    // DateSwitcher opens an input, type the date
    const input = this.drawer.getByRole('textbox');
    await input.clear();
    await input.fill(dateString);
    await this.page.keyboard.press('Enter');
    await this.page.waitForLoadState('networkidle');
  }

  // ── Summary Pills ─────────────────────────────────────────────────────────

  /**
   * Verify operation type summary pill shows correct count
   */
  async verifyOperationSummaryPill(operationType: string, expectedCount: string): Promise<void> {
    this.logger?.step('verifyOperationSummaryPill', { operationType, expectedCount });
    const pill = this.drawer.getByTestId(`summary-pill-${operationType}`);
    await expect(pill).toContainText(expectedCount);
  }

  /**
   * Get all visible operation type summary pills
   */
  async getOperationSummaryPills(): Promise<string[]> {
    this.logger?.step('getOperationSummaryPills');
    const pills = this.drawer.getByTestId(/^summary-pill-/);
    const count = await pills.count();
    const pillTexts: string[] = [];
    if (count === 0) {
      return pillTexts;
    }
    const allText = await this.drawer.textContent();
    if (allText) {
      pillTexts.push(allText.trim());
    }
    return pillTexts;
  }

  // ── Day Interaction ────────────────────────────────────────────────────────

  /**
   * Click on a specific day to open its popover/drawer (YYYY-MM-DD format)
   */
  async clickDay(dateString: string): Promise<void> {
    this.logger?.step('clickDay', { dateString });
    const day = this.getDayCell(dateString);
    await day.scrollIntoViewIfNeeded();
    await day.click({ timeout: TIMEOUTS.SHORT });
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);
  }

  /**
   * Verify popover shows specific operation type with expected count
   */
  async verifyPopoverOperationType(operationType: string, expectedCount: string): Promise<void> {
    this.logger?.step('verifyPopoverOperationType', { operationType, expectedCount });
    const operation = this.popover.getByTestId(`operation-${operationType}`);
    await expect(operation).toContainText(expectedCount);
  }

  /**
   * Verify popover shows "no operations" message for a day
   */
  async verifyPopoverNoOperations(): Promise<void> {
    this.logger?.step('verifyPopoverNoOperations');
    const noOpsMessage = this.popover.getByTestId('no-operations-message');
    await expect(noOpsMessage).toBeVisible();
  }

  /**
   * Verify popover shows "no data reported" message
   */
  async verifyPopoverNoDataReported(): Promise<void> {
    this.logger?.step('verifyPopoverNoDataReported');
    const noDataMessage = this.popover.getByTestId('no-data-message');
    await expect(noDataMessage).toBeVisible();
  }

  /**
   * Verify popover total changes text
   */
  async verifyPopoverTotalChanges(expectedText: string): Promise<void> {
    this.logger?.step('verifyPopoverTotalChanges', { expectedText });
    const totalChanges = this.popover.getByTestId('total-changes');
    await expect(totalChanges).toContainText(expectedText);
  }

  /**
   * Open types select dropdown and toggle multiple operation types
   */
  async toggleOperationTypes(operationTypes: string[]): Promise<void> {
    this.logger?.step('toggleOperationTypes', { operationTypes });
    await this.typesSelect.click({ timeout: TIMEOUTS.SHORT });
    for (const opType of operationTypes) {
      const option = this.page.getByTestId(`option-${opType}`);
      await option.click({ timeout: TIMEOUTS.SHORT });
    }
    await this.page.keyboard.press('Escape');
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Click on a summary pill to toggle operation type filter
   */
  async togglePill(operationType: string): Promise<void> {
    this.logger?.step('togglePill', { operationType });
    const pill = this.page.getByTestId(`summary-pill-${operationType}`);
    await pill.click({ timeout: TIMEOUTS.SHORT });
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Open day drawer by clicking on a day
   */
  async openDayDrawer(dayNumber: string): Promise<void> {
    this.logger?.step('openDayDrawer', { dayNumber });
    await this.clickDay(dayNumber);
    // Look for "View details" button or similar in popover
    const viewDetailsButton = this.popover.getByRole('button', { name: /view|details/i });
    if ((await viewDetailsButton.count()) > 0) {
      await viewDetailsButton.click({ timeout: TIMEOUTS.SHORT });
    }
    await this.drawer.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Close the day details drawer
   */
  async closeDayDrawer(): Promise<void> {
    this.logger?.step('closeDayDrawer');
    await this.closeDrawerButton.click({ timeout: TIMEOUTS.SHORT });
    await this.drawer.waitFor({ state: 'hidden', timeout: TIMEOUTS.MEDIUM });
  }

  // ── Day Cell Access ────────────────────────────────────────────────────────

  /**
   * Get a day cell by date string (YYYY-MM-DD format)
   */
  getDayCell(dateString: string): Locator {
    return this.page.getByTestId(`day-${dateString}`);
  }

  /**
   * Get popover for a specific day
   */
  getDayPopover(dateString: string): Locator {
    return this.page.getByTestId(`day-popover-${dateString}`);
  }

  // ── Operation Type Selectors ───────────────────────────────────────────────

  /**
   * Get operation element by type (CREATE, UPDATE, DELETE, etc.)
   */
  getOperationType(opType: string): Locator {
    return this.page.getByTestId(`operation-${opType}`);
  }

  /**
   * Get custom operation element
   */
  getCustomOperation(customType: string): Locator {
    return this.page.getByTestId(`operation-${customType}`);
  }

  /**
   * Get summary pill for operation type
   */
  getSummaryPill(opType: string): Locator {
    return this.page.getByTestId(`summary-pill-${opType}`);
  }

  // ── State Messages ──────────────────────────────────────────────────────────

  /**
   * Get "no changes this day" message
   */
  getNoChangesMessage(): Locator {
    return this.page.getByTestId('no-changes-this-day');
  }

  /**
   * Get "no data reported" message (outside data range)
   */
  getNoDataMessage(): Locator {
    return this.page.getByTestId('no-data-reported');
  }

  // ── Interaction Helpers ────────────────────────────────────────────────────

  /**
   * Hover over a day cell and wait for popover
   */
  async hoverDay(dateString: string, timeout: number = TIMEOUTS.LONG): Promise<void> {
    const dayCell = this.getDayCell(dateString);
    await dayCell.scrollIntoViewIfNeeded();
    await dayCell.hover({ timeout });
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
  }

  /**
   * Toggle operation type filter via dropdown
   */
  async toggleOperationType(opType: string, timeout: number = TIMEOUTS.SHORT): Promise<void> {
    const option = this.page.getByTestId(`option-${opType}`);
    if ((await option.count()) > 0) {
      await option.click({ timeout });
      await this.page.waitForTimeout(100);
    }
  }

  /**
   * Click summary pill to toggle filter
   */
  async toggleSummaryPill(opType: string, timeout: number = TIMEOUTS.SHORT): Promise<void> {
    const pill = this.getSummaryPill(opType);
    await pill.click({ timeout });
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Verify operation type is visible in popover with expected count
   */
  async verifyOperationVisible(opType: string, expectedCount: string): Promise<void> {
    const opElement = this.getOperationType(opType);
    const text = await opElement.textContent();
    if (!text?.includes(expectedCount)) {
      throw new Error(`${opType} should show count ${expectedCount} but got: "${text}"`);
    }
  }

  /**
   * Verify operation type is NOT visible
   */
  async verifyOperationNotVisible(opType: string): Promise<void> {
    const opElement = this.getOperationType(opType);
    if ((await opElement.count()) > 0) {
      throw new Error(`${opType} should not be visible but was found`);
    }
  }

  /**
   * Verify "no changes this day" message appears
   */
  async verifyNoChangesThisDay(): Promise<void> {
    const msg = this.getNoChangesMessage();
    if ((await msg.count()) === 0) {
      throw new Error('Should show "no changes this day" message');
    }
  }

  /**
   * Verify "no data reported" message appears
   */
  async verifyNoDataReported(): Promise<void> {
    const msg = this.getNoDataMessage();
    if ((await msg.count()) === 0) {
      throw new Error('Should show "no data reported" message (outside oldestOperationTime range)');
    }
  }
}
