import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS, KEYS, LOAD_STATES } from '../../utils/constants';

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
  readonly drawerTitleContainer: Locator;
  readonly closeDrawerButton: Locator;
  readonly noChangesMessage: Locator;
  readonly noDataMessage: Locator;

  // Filters
  readonly typesSelect: Locator;

  // Calendar
  readonly calendar: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.drawerTitleContainer = page.getByTestId('change-history-details-drawer-title-container');
    this.closeDrawerButton = page.getByTestId('drawer-close-button');
    this.noChangesMessage = page.getByTestId('no-changes-this-day');
    this.noDataMessage = page.getByTestId('no-data-reported');
    this.typesSelect = page.getByTestId('types-select');
    this.calendar = page.getByTestId('change-history-chart');
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

  /**
   * Get summary pill for operation type
   */
  getSummaryPill(opType: string): Locator {
    return this.page.getByTestId(`summary-pill-${opType}`);
  }

  // ── Dynamic Selectors (Parameterized) ─────────────────────────────────────

  /**
   * Get operation type dropdown option
   */
  private getOperationTypeOption(opType: string): Locator {
    return this.page.getByTestId(`option-${opType}`);
  }

  // ── State Messages ──────────────────────────────────────────────────────────

  /**
   * Get operation element in day popover
   */
  private getOperationInPopover(dayPopover: Locator, opTestId: string): Locator {
    return dayPopover.getByTestId(opTestId);
  }

  /**
   * Get no-changes message in day popover
   */
  private getNoChangesInPopover(dayPopover: Locator): Locator {
    return dayPopover.getByTestId('no-changes-this-day');
  }

  /**
   * Get "View Details" button in day popover
   */
  private getViewDetailsButton(dayPopover: Locator): Locator {
    return dayPopover.getByRole('button', { name: /view|details/i });
  }

  // ── Drawer ────────────────────────────────────────────────────────────────

  /**
   * Open the day drawer via the "View Details" button in the popover
   */
  async openDayDrawer(dateString: string, timeout: number = TIMEOUTS.LONG): Promise<void> {
    this.logger?.step('openDayDrawer', { dateString });
    // Hover day to show popover
    await this.hoverDay(dateString, timeout);
    // Get the popover for this day
    const dayPopover = this.getDayPopover(dateString);
    await expect(dayPopover).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    // Click "View Details" button to open drawer
    const viewDetailsBtn = this.getViewDetailsButton(dayPopover);
    await viewDetailsBtn.click({ timeout: TIMEOUTS.SHORT });
    // Wait for drawer title to be visible (indicates drawer is fully open)
    await this.drawerTitleContainer.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  /**
   * Verify the day drawer is visible
   */
  async ensureDayDrawerIsVisible(timeout: number = TIMEOUTS.MEDIUM): Promise<void> {
    this.logger?.step('ensureDayDrawerIsVisible');
    await expect(this.drawerTitleContainer).toBeVisible({ timeout });
  }

  /**
   * Close the day drawer via close button
   */
  async closeDayDrawer(timeout: number = TIMEOUTS.SHORT): Promise<void> {
    this.logger?.step('closeDayDrawer');
    await this.closeDrawerButton.click({ timeout });
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
  }

  /**
   * Force-close any open drawers from previous tests
   * Used in test setup/cleanup to ensure clean state
   */
  async forceCloseDrawer(): Promise<void> {
    this.logger?.step('forceCloseDrawer');
    await this.closeDrawerButton.click({ timeout: TIMEOUTS.BETWEEN_OPS }).catch(() => {});
    await this.page.keyboard.press(KEYS.ESCAPE);
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
    await this.getOperationTypeOption(opType).click({ timeout });
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
  }

  /**
   * Click summary pill to toggle filter
   */
  async toggleSummaryPill(opType: string, timeout: number = TIMEOUTS.SHORT): Promise<void> {
    const pill = this.getSummaryPill(opType);
    await pill.click({ timeout });
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  /**
   * Verify "no changes this day" message appears
   */
  async verifyNoChangesThisDay(): Promise<void> {
    await expect(this.noChangesMessage).toBeVisible();
  }

  /**
   * Verify "no data reported" message appears
   */
  async verifyNoDataReported(): Promise<void> {
    await expect(this.noDataMessage).toBeVisible();
  }

  // ── Popover Content Verification ────────────────────────────────────────

  /**
   * Verify operation appears in day popover with expected count
   */
  async verifyOperationCountInDayPopover(dayDateStr: string, opTestId: string, expectedCount: string): Promise<void> {
    const dayPopover = this.getDayPopover(dayDateStr);
    await expect(dayPopover).toBeVisible();
    const opElement = this.getOperationInPopover(dayPopover, opTestId);
    await expect(opElement).toContainText(expectedCount);
  }

  /**
   * Verify custom operation appears in day popover
   */
  async verifyCustomOperationInDayPopover(dayPopover: Locator, customOpTestId: string): Promise<void> {
    await expect(dayPopover).toBeVisible();
    const opElement = this.getOperationInPopover(dayPopover, customOpTestId);
    await expect(opElement).toBeVisible();
  }

  /**
   * Verify "no changes this day" message appears in day popover
   */
  async verifyNoChangesThisDayInDayPopover(dayPopover: Locator): Promise<void> {
    const msgElement = this.getNoChangesInPopover(dayPopover);
    await expect(msgElement).toBeVisible();
  }
}
