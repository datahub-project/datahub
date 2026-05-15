import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';

export class BaseSettingsPage extends BasePage {
  protected readonly dropdownSearchInput: Locator;
  private readonly navSignOut: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.dropdownSearchInput = page.locator('[data-testid="dropdown-search-input"]');
    this.navSignOut = page.locator('[data-testid="nav-sidebar-sign-out"]');
  }

  /**
   * Clicks a dropdown trigger, searches for a value, and selects it.
   * Pass constructor-initialized locators for the base trigger and the dropdown list.
   *
   * Handles both single-select and multi-select SimpleSelect components:
   * - For multi-select, clicking the option text does not trigger selection; the
   *   checkbox inside the option row must be clicked instead.
   */
  async selectFromDropdown(base: Locator, dropdown: Locator, value: string): Promise<void> {
    await base.waitFor({ state: 'attached' });
    await base.click({ force: true });
    await this.dropdownSearchInput.waitFor({ state: 'visible' });
    await this.dropdownSearchInput.fill(value);
    const optionRow = dropdown.locator('[data-testid^="option-"]').filter({ hasText: value }).first();
    await optionRow.waitFor({ state: 'visible' });
    // Prefer clicking the checkbox (multiselect) when present; fall back to
    // clicking the entire row (single-select).
    const checkboxInput = optionRow.locator('input[type="checkbox"]');
    if ((await checkboxInput.count()) > 0) {
      // The OptionLabel click handler only fires for single-select. For multi-select,
      // selection is toggled via the CheckboxBase wrapper's React onClick handler.
      // We click toward the right edge of the option row where the checkbox visually
      // appears, ensuring the click lands on CheckboxBase (which has the handler).
      // Using { position } to click the right side of the row bypasses viewport issues
      // since the element might be near the viewport edge.
      const checkboxBase = optionRow.locator('[data-checked]');
      await checkboxBase.waitFor({ state: 'attached' });
      // Scroll the option into view before measuring its bounding box.
      await optionRow.scrollIntoViewIfNeeded();
      const box = await checkboxBase.boundingBox();
      if (box) {
        await this.page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
      } else {
        await checkboxBase.click({ force: true });
      }
    } else {
      await optionRow.scrollIntoViewIfNeeded();
      await optionRow.click({ force: true });
    }
  }

  /** Waits for a toast message to appear then disappear. */
  async waitForToast(text: string): Promise<void> {
    await expect(this.page.getByText(text)).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(text)).not.toBeVisible({ timeout: 15000 });
  }

  async logout(): Promise<void> {
    await this.navSignOut.click({ force: true });
    await expect(this.page.getByText('Username')).toBeVisible({ timeout: 10000 });
  }

  async skipIntroducePage(): Promise<void> {
    await this.page.evaluate(() => localStorage.setItem('skipAcrylIntroducePage', 'true'));
  }

  async skipOnboarding(): Promise<void> {
    await this.page.evaluate(() => {
      localStorage.setItem('skipOnboardingTour', 'true');
      localStorage.setItem('skipWelcomeModal', 'true');
    });
  }

  async registerViaInviteLink(inviteLink: string, email: string, name: string, password: string): Promise<void> {
    await this.page.goto(inviteLink);
    await this.skipIntroducePage();
    await this.page.locator('[data-testid="email"]').fill(email);
    await this.page.locator('[data-testid="name"]').fill(name);
    await this.page.locator('[data-testid="password"]').fill(password);
    await this.page.locator('[data-testid="confirmPassword"]').fill(password);
    await this.page.locator('[data-testid="sign-up"]').click();
    await expect(this.page.getByText('Accepted invite!')).not.toBeVisible({ timeout: 10000 });
  }
}
