import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import { WAIT_TIMEOUT, SHORT_TIMEOUT } from '../../utils/constants';
import type { DataHubLogger } from '../../utils/logger';

export class BaseSettingsPage extends BasePage {
  protected readonly dropdownSearchInput: Locator;
  private readonly navSignOut: Locator;
  private readonly emailInput: Locator;
  private readonly nameInput: Locator;
  private readonly passwordInput: Locator;
  private readonly confirmPasswordInput: Locator;
  private readonly signUpButton: Locator;
  private readonly modalDialog: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.dropdownSearchInput = page.getByTestId('dropdown-search-input');
    this.navSignOut = page.getByTestId('nav-sidebar-sign-out');
    this.emailInput = page.getByTestId('email');
    this.nameInput = page.getByTestId('name');
    this.passwordInput = page.getByTestId('password');
    this.confirmPasswordInput = page.getByTestId('confirmPassword');
    this.signUpButton = page.getByTestId('sign-up');
    this.modalDialog = page.getByRole('dialog');
  }

  async selectFromDropdown(base: Locator, dropdown: Locator, value: string): Promise<void> {
    await base.waitFor({ state: 'attached', timeout: SHORT_TIMEOUT });
    await base.click({ force: true });
    await this.dropdownSearchInput.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await this.dropdownSearchInput.fill(value);
    const optionRow = dropdown.locator('[data-testid^="option-"]').filter({ hasText: value });
    await optionRow.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    const checkboxInput = optionRow.locator('input[type="checkbox"]');
    if ((await checkboxInput.count()) > 0) {
      const checkboxBase = optionRow.locator('[data-checked]');
      await checkboxBase.waitFor({ state: 'attached', timeout: SHORT_TIMEOUT }).catch(() => {});
      await optionRow.scrollIntoViewIfNeeded();
      const box = await checkboxBase.boundingBox().catch(() => null);
      if (box) {
        await this.page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
      } else {
        await checkboxBase.click({ force: true }).catch(() => optionRow.click({ force: true }));
      }
    } else {
      await optionRow.scrollIntoViewIfNeeded();
      await optionRow.click({ force: true });
    }
  }

  async waitForToast(text: string): Promise<void> {
    await expect(this.page.getByText(text)).toBeVisible();
    await expect(this.page.getByText(text)).toBeHidden();
  }

  async logout(): Promise<void> {
    if ((await this.modalDialog.count()) > 0) {
      await this.page.keyboard.press('Escape');
      try {
        await expect(this.modalDialog).toBeHidden();
      } catch {
        // Dialog may still be visible, continue
      }
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
