import { type Locator, type Page, expect } from '@playwright/test';

/**
 * Toast notifications.
 *
 * The app is mid-migration from antd's global `message.*` API (rendered in `.ant-message`)
 * to the alchemy `toast.*` API (rendered in a portal with `data-testid="toast-notification-container"`).
 * Match either container so this helper works regardless of which API a given surface uses.
 */
export class ToastComponent {
  private readonly container: Locator;

  constructor(page: Page) {
    // eslint-disable-next-line playwright/no-raw-locators -- antd's global message container has no data-testid or ARIA role; the union covers both toast systems during migration.
    this.container = page.locator('.ant-message, [data-testid="toast-notification-container"]');
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
