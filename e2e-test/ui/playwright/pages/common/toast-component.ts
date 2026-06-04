import { type Locator, type Page, expect } from '@playwright/test';

/**
 * Ant Design global message toasts rendered via message.success/error/warning/info.
 */
export class ToastComponent {
  private readonly container: Locator;

  constructor(page: Page) {
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design global message container has no data-testid or ARIA role
    this.container = page.locator('.ant-message');
  }

  getToast(text: string | RegExp): Locator {
    return this.container.getByText(text);
  }

  async expectVisible(text: string | RegExp): Promise<void> {
    await expect(this.getToast(text)).toBeVisible();
  }

  async expectHidden(text: string | RegExp): Promise<void> {
    await expect(this.getToast(text)).toBeHidden();
  }

  async expectVisibleThenHidden(text: string | RegExp): Promise<void> {
    await expect(this.getToast(text)).toBeVisible();
    await expect(this.getToast(text)).toBeHidden();
  }
}
