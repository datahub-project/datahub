import { Page } from '@playwright/test';

export class NavigationHelper {
  constructor(private page: Page) {}

  async navigateTo(path: string): Promise<void> {
    await this.page.goto(path);
    await this.waitForNavigation();
  }

  async waitForNavigation(): Promise<void> {
    await this.page.waitForLoadState('domcontentloaded');
    await this.page.waitForLoadState('networkidle');
  }

  async getCurrentUrl(): Promise<string> {
    return this.page.url();
  }

  async goBack(): Promise<void> {
    await this.page.goBack();
    await this.waitForNavigation();
  }

  async reload(): Promise<void> {
    await this.page.reload();
    await this.waitForNavigation();
  }
}
