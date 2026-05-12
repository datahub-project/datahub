import { Page, Locator, expect } from '@playwright/test';

/**
 * datahub-web-react/src/alchemy-components/components/Modal/Modal.tsx
 */
export class ModalComponent {
  readonly modal: Locator;
  readonly title: Locator;

  constructor(page: Page) {
    this.modal = page.locator('.ant-modal:visible'); // visible modal only
    this.title = this.modal.locator('.ant-modal-title');
  }

  async waitForOpening(): Promise<void> {
    await expect(this.modal).toBeVisible();
  }

  async expectTitleContainText(text: string): Promise<void> {
    await expect(this.title).toContainText(text);
  }
}
