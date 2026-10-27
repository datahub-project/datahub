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
    this.navSignOut = page.getByTestId('nav-sidebar-sign-out');
    this.emailInput = page.getByTestId('email');
    this.nameInput = page.getByTestId('name');
    this.passwordInput = page.getByTestId('password');
    this.confirmPasswordInput = page.getByTestId('confirmPassword');
    this.signUpButton = page.getByTestId('sign-up');
    this.modalDialog = page.getByRole('dialog');
  }

  async selectFromDropdown(base: Locator, dropdown: Locator, value: string): Promise<void> {
    await base.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await base.click();
    await dropdown.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });

    // The signed-in user is already in the unfiltered actor list. Searching replaces that
    // list with autocomplete results, and filtering the dropdown container matches every
    // checkbox inside it.
    const option = dropdown.getByTestId(`option-urn:li:corpuser:${value}`);
    const checkbox = option.getByTestId('checkbox-base');
    await checkbox.waitFor({ state: 'visible', timeout: WAIT_TIMEOUT });
    await checkbox.click();
  }

  async logout(): Promise<void> {
    if ((await this.modalDialog.count()) > 0) {
      await this.page.keyboard.press('Escape');
      // eslint-disable-next-line playwright/no-wait-for-timeout
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
