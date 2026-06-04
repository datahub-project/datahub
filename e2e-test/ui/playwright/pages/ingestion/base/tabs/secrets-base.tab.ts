import { type Locator, type Page, expect } from '@playwright/test';
import type { DataHubLogger } from '../../../../utils/logger';
import { BaseTab } from './base.tab';
import { ModalComponent } from '@pages/common/modal-component';
import { ConfirmationModalComponent } from '@pages/common/confirmation-modal-component';
import { ToastComponent } from '@pages/common/toast-component';
import { GraphQLHelper } from '../../../../helpers/graphql-helper';

export class SecretsBaseTab extends BaseTab {
  readonly name: string;
  readonly path: string;
  readonly tabKey: string;

  readonly createSecretButton: Locator;
  readonly secretNameInput: Locator;
  readonly secretValueInput: Locator;
  readonly secretDescriptionInput: Locator;
  readonly secretModalCreateButton: Locator;

  protected readonly toast: ToastComponent;
  protected readonly graphql: GraphQLHelper;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    super(page, logger, logDir);

    this.name = 'Secrets';
    this.path = '/ingestion/secrets';
    this.tabKey = 'Secrets';

    this.createSecretButton = page.getByTestId('create-secret-button');
    this.secretNameInput = page.getByTestId('secret-modal-name-input').getByRole('textbox');
    this.secretValueInput = page.getByTestId('secret-modal-value-input').getByRole('textbox');
    this.secretDescriptionInput = page.getByTestId('secret-modal-description-input').getByRole('textbox');
    this.secretModalCreateButton = page.getByTestId('secret-modal-create-button');
    this.toast = new ToastComponent(page);
    this.graphql = new GraphQLHelper(page);
  }

  getSecretRow(urn: string): Locator {
    return this.page.getByTestId(`secret-row-${urn}`);
  }

  getDeleteSecretButton(urn: string): Locator {
    return this.getSecretRow(urn).getByTestId('delete-secret-action');
  }

  getSecretDescription(description: string): Locator {
    return this.page.getByText(description);
  }

  async createSecret(name: string, value: string, description?: string): Promise<string> {
    this.logger?.step('create secret', { name });
    await this.createSecretButton.click();
    const modal = new ModalComponent(this.page);
    await modal.waitForOpening();
    await this.secretNameInput.fill(name);
    await this.secretValueInput.fill(value);
    if (description) {
      await this.secretDescriptionInput.fill(description);
    }
    const mutationResponse = this.graphql.waitForGraphQLResponse('createSecret');
    await this.secretModalCreateButton.click();
    const resp = await mutationResponse;
    await this.toast.expectVisible('Successfully created Secret!');
    return (resp as { data?: { createSecret?: string } }).data?.createSecret ?? '';
  }

  async deleteSecret(urn: string): Promise<void> {
    this.logger?.step('delete secret', { urn });
    await this.getDeleteSecretButton(urn).click();
    const modal = new ConfirmationModalComponent(this.page);
    await modal.waitForOpening();
    await modal.confirm();
    await this.toast.expectVisible('Removed secret.');
  }

  async expectSecretVisible(urn: string): Promise<void> {
    await expect(this.getSecretRow(urn)).toBeVisible();
  }

  async expectSecretNotVisible(urn: string): Promise<void> {
    await expect(this.getSecretRow(urn)).toBeHidden();
  }

  async expectSecretDescriptionVisible(description: string): Promise<void> {
    await expect(this.getSecretDescription(description)).toBeVisible();
  }

  async expectSecretDescriptionNotVisible(description: string): Promise<void> {
    await expect(this.getSecretDescription(description)).toBeHidden();
  }
}
