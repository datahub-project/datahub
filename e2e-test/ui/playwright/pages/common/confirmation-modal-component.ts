import { Page, Locator } from '@playwright/test';
import { ModalComponent } from './modal-component';

/**
 * datahub-web-react/src/app/sharedV2/modals/ConfirmationModal.tsx
 */
export class ConfirmationModalComponent extends ModalComponent {
  readonly closeButton: Locator;
  readonly confirmButton: Locator;
  readonly cancelButton: Locator;

  constructor(page: Page) {
    super(page);
    this.closeButton = this.modal.getByTestId('modal-close-icon');
    this.confirmButton = this.modal.getByTestId('modal-confirm-button');
    this.cancelButton = this.modal.getByTestId('modal-cancel-button');
  }

  async close(): Promise<void> {
    await this.closeButton.click();
  }

  async confirm(): Promise<void> {
    await this.confirmButton.click();
  }

  async cancel(): Promise<void> {
    await this.cancelButton.click();
  }
}
