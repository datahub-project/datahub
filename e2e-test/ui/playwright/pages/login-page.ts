import type { Page, Locator } from '@playwright/test';
import { BasePage } from './base-page';
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
    await this.waitForPageLoad();
  }

  async navigateToLogin(): Promise<void> {
    await this.navigate('/login');
  }
}
