import { Page, Locator, expect } from '@playwright/test';

/**
 * datahub-web-react/src/alchemy-components/components/Modal/Modal.tsx
 */
export class ModalComponent {
  readonly modal: Locator;
  readonly title: Locator;

  constructor(page: Page) {
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design modal; no ARIA equivalent for :visible filter
    this.modal = page.locator('.ant-modal:visible'); // visible modal only
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design modal title; no semantic heading role available
    this.title = this.modal.locator('.ant-modal-title');
  }

  async waitForOpening(): Promise<void> {
    await expect(this.modal).toBeVisible();
  }

  async expectTitleContainText(text: string): Promise<void> {
    await expect(this.title).toContainText(text);
  }
}
