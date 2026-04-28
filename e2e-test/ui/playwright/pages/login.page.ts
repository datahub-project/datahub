import type { Page, Locator } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class LoginPage extends BasePage {
  readonly usernameInput: Locator;
  readonly passwordInput: Locator;
  readonly loginButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.usernameInput = page.locator('[data-testid="username"]');
    this.passwordInput = page.locator('[data-testid="password"]');
    this.loginButton = page.locator('[data-testid="sign-in"]');
  }

  async login(username: string, password: string): Promise<void> {
    this.logger?.step('login', { username });
    await this.usernameInput.fill(username);
    await this.passwordInput.fill(password);
    await this.loginButton.click();
    // Wait for the redirect away from /login — DataHub redirects to / on success.
    // Using waitForURL here is more reliable than waitForLoadState, which can
    // resolve against the current /login page before navigation begins.
    await this.page.waitForURL((url) => !url.pathname.includes('login'), {
      waitUntil: 'networkidle',
      timeout: 30000,
    });
  }

  async navigateToLogin(): Promise<void> {
    await this.navigate('/login');
    await this.waitForPageLoad();
  }
}
