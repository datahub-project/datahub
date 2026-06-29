import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import { WAIT_TIMEOUT, SHORT_TIMEOUT, ANIMATION_TIMEOUT } from '../../utils/constants';
import type { DataHubLogger } from '../../utils/logger';
import type { ScopedCleanup } from '../../utils/cleanup';

export interface PageOptions {
  logger?: DataHubLogger;
  logDir?: string;
  cleanup?: ScopedCleanup;
}

export class BaseSettingsPage extends BasePage {
  protected readonly dropdownSearchInput: Locator;
  private readonly navSignOut: Locator;
  private readonly emailInput: Locator;
  private readonly nameInput: Locator;
  private readonly passwordInput: Locator;
  private readonly confirmPasswordInput: Locator;
  private readonly signUpButton: Locator;
  private readonly modalDialog: Locator;
  protected readonly cleanup?: ScopedCleanup;

  constructor(page: Page, options?: PageOptions) {
    super(page, options?.logger, options?.logDir);
    this.cleanup = options?.cleanup;
    this.dropdownSearchInput = page.getByTestId('dropdown-search-input');
    this.navSignOut = page.getByTestId('nav-sidebar-sign-out');
    this.emailInput = page.getByTestId('email');
    this.nameInput = page.getByTestId('name');
    this.passwordInput = page.getByTestId('password');
    this.confirmPasswordInput = page.getByTestId('confirmPassword');
    this.signUpButton = page.getByTestId('sign-up');
    this.modalDialog = page.getByRole('dialog');
  }

  // ── Dropdown helper selectors ────────────────────────────────────────────
  private getDropdownOption(dropdown: Locator, value: string): Locator {
    return dropdown.filter({ hasText: value });
  }

  private getDropdownCheckbox(optionRow: Locator): Locator {
    return optionRow.getByRole('checkbox');
  }

  private getDropdownSearchInput(): Locator {
    return this.dropdownSearchInput.or(this.page.getByPlaceholder(/search/i));
  }

  async selectFromDropdown(base: Locator, dropdown: Locator, value: string): Promise<void> {
    await base.waitFor({ state: 'attached', timeout: SHORT_TIMEOUT });
    await base.click();
    await this.page.waitForLoadState('networkidle');

    // Use fallback selector if main dropdown-search-input is not found
    const searchInput = this.getDropdownSearchInput();
    const isSearchVisible = await searchInput.isVisible().catch(() => false);

    // Fill search input if visible
    if (isSearchVisible) {
      await searchInput.clear();
      await searchInput.fill(value);
      // Wait for filter results to appear
      await this.page.waitForTimeout(ANIMATION_TIMEOUT);
    }

    const optionRow = this.getDropdownOption(dropdown, value);
    await optionRow.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    const checkboxInput = this.getDropdownCheckbox(optionRow);
    if ((await checkboxInput.count()) > 0) {
      await optionRow.scrollIntoViewIfNeeded();
      // Click using bounding box to handle styled checkboxes that may not be directly visible
      const box = await checkboxInput.boundingBox();
      if (box) {
        await this.page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
      } else {
        await checkboxInput.click();
      }
    } else {
      await optionRow.scrollIntoViewIfNeeded();
      await optionRow.click();
    }
  }

  async waitForToast(text: string): Promise<void> {
    const toastMessage = this.page.getByText(text);
    await expect(toastMessage).toBeVisible();
    await expect(toastMessage).toBeHidden();
  }

  async logout(): Promise<void> {
    if ((await this.modalDialog.count()) > 0) {
      await this.page.keyboard.press('Escape');
      await this.page.waitForTimeout(ANIMATION_TIMEOUT);
    }
    await this.navSignOut.click();
    await this.page.waitForURL('**/login', { timeout: WAIT_TIMEOUT });
    await this.page.waitForLoadState('networkidle');
    await expect(this.page.getByText('Username')).toBeVisible({ timeout: WAIT_TIMEOUT });
    await expect(this.page.getByText('Password')).toBeVisible({ timeout: WAIT_TIMEOUT });
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
    await this.emailInput.fill(email);
    await this.nameInput.fill(name);
    await this.passwordInput.fill(password);
    await this.confirmPasswordInput.fill(password);
    await this.signUpButton.click();
    await expect(this.page.getByText('Accepted invite!')).toBeHidden();
  }
}
