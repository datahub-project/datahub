import { Page, Locator } from '@playwright/test';
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
  readonly closeDrawerButton: Locator;
  readonly drawerTitleContainer: Locator;

  // Filters
  readonly dateSwitch: Locator;
  readonly usersSelect: Locator;
  readonly typesSelect: Locator;

  // Calendar
  readonly calendar: Locator;

  // Popover
  readonly popover: Locator;

  // Popover Messages
  readonly popoverNoOperations: Locator;
  readonly popoverTotalChanges: Locator;
  readonly popoverNoDataMessage: Locator;

  // Date Switcher Input
  readonly dateInput: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.drawer = page.getByTestId('change-history-details');
    this.closeDrawerButton = this.drawer.getByTestId('drawer-close-button');
    this.drawerTitleContainer = page.getByTestId('change-history-details-drawer-title-container');

    this.dateSwitch = page.getByTestId('date-switcher');
    this.usersSelect = page.getByTestId('users-select');
    this.typesSelect = page.getByTestId('types-select');

    // Calendar is rendered as days within the change-history-chart
    this.calendar = page.getByTestId('change-history-chart');

    this.popover = page.getByRole('dialog');

    // Popover Messages
    this.popoverNoOperations = this.popover.getByTestId('no-operations-message');
    this.popoverTotalChanges = this.popover.getByTestId('total-changes');
    this.popoverNoDataMessage = this.popover.getByTestId('no-data-message');

    // Date Switcher Input
    this.dateInput = this.drawer.getByRole('textbox');
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

  // ── Drawer ────────────────────────────────────────────────────────────────

  /**
   * Force-close any open drawers from previous tests
   * Used in test setup/cleanup to ensure clean state
   */
  async forceCloseDrawer(): Promise<void> {
    this.logger?.step('forceCloseDrawer');
    // Try closing via button
    if ((await this.closeDrawerButton.count()) > 0) {
      await this.closeDrawerButton.click({ timeout: 500 }).catch(() => {});
    }
    // Fallback: press Escape key
    await this.page.keyboard.press('Escape').catch(() => {});
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
    await this.getOperationTypeOption(opType)
      .click({ timeout })
      .catch(() => {});
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
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
   * Verify "no changes this day" message appears
   */
  async verifyNoChangesThisDay(): Promise<void> {
    if ((await this.getNoChangesMessage().count()) === 0) {
      throw new Error('Should show "no changes this day" message');
    }
  }

  /**
   * Verify "no data reported" message appears
   */
  async verifyNoDataReported(): Promise<void> {
    if ((await this.getNoDataMessage().count()) === 0) {
      throw new Error('Should show "no data reported" message (outside oldestOperationTime range)');
    }
  }
}
