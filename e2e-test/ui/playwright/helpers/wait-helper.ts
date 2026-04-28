import { Page } from '@playwright/test';

export class WaitHelper {
  constructor(private page: Page) {}

  async waitForSelector(selector: string, timeout = 30000): Promise<void> {
    await this.page.waitForSelector(selector, { timeout });
  }

  async waitForText(text: string, timeout = 30000): Promise<void> {
    await this.page.waitForSelector(`text=${text}`, { timeout });
  }

  async waitForNetworkIdle(timeout = 30000): Promise<void> {
    await this.page.waitForLoadState('networkidle', { timeout });
  }

  async waitForResponse(urlPattern: string | RegExp, timeout = 30000): Promise<void> {
    await this.page.waitForResponse(urlPattern, { timeout });
  }
}
