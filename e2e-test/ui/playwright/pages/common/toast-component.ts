import { type Locator, type Page, expect } from '@playwright/test';

/**
 * Toast notifications.
 *
 * The app is mid-migration from antd's global `message.*` API (rendered in `.ant-message`)
 * to the alchemy `toast.*` API (rendered in a portal with `data-testid="toast-notification-container"`).
 * Some surfaces (e.g. subscriptions) still use antd's `notification.*` API, rendered in `.ant-notification`.
 * Match any of these containers so this helper works regardless of which API a given surface uses.
 */
export class ToastComponent {
  private readonly container: Locator;

  constructor(page: Page) {
    // eslint-disable-next-line playwright/no-raw-locators -- antd's global message/notification containers have no data-testid or ARIA role; the union covers all three toast systems during migration.
    this.container = page.locator('.ant-message, .ant-notification, [data-testid="toast-notification-container"]');
  }

  getToast(text: string | RegExp): Locator {
    return this.container.getByText(text);
  }

  async expectVisible(text: string | RegExp, options?: { timeout?: number }): Promise<void> {
    await expect(this.getToast(text)).toBeVisible(options);
  }

  async expectHidden(text: string | RegExp, options?: { timeout?: number }): Promise<void> {
    await expect(this.getToast(text)).toBeHidden(options);
  }

  // Toasts auto-dismiss, so the "hidden" leg rarely needs tuning; the "visible" leg
  // often does, since the toast only appears after an async/backend action completes.
  async expectVisibleThenHidden(
    text: string | RegExp,
    options?: { visibleTimeout?: number; hiddenTimeout?: number },
  ): Promise<void> {
    await this.expectVisible(text, { timeout: options?.visibleTimeout });
    await this.expectHidden(text, { timeout: options?.hiddenTimeout });
  }
}
