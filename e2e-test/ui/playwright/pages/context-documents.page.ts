import type { Locator, Page } from '@playwright/test';
import type { DataHubLogger } from '../utils/logger';
import { BasePage } from './base.page';

export class ContextDocumentsPage extends BasePage {
  readonly importButton: Locator;
  readonly importModal: Locator;

  constructor(
    page: Page,
    logger?: DataHubLogger,
    logDir?: string,
  ) {
    super(page, logger, logDir);
    this.importButton = page.getByTestId('import-documents-button');
    this.importModal = page.getByTestId('import-documents-modal');
  }

  async navigateToContextDocuments(): Promise<void> {
    await this.page.goto('/context/documents');
    await this.page.getByTestId('context-documents-sidebar').waitFor({ state: 'visible', timeout: 30_000 });
  }

  async navigateToDocument(documentUrn: string): Promise<void> {
    await this.page.goto(`/document/${encodeURIComponent(documentUrn)}`);
    await this.page.getByTestId('context-documents-sidebar').waitFor({ state: 'visible', timeout: 30_000 });
  }

  async openImportModal(): Promise<void> {
    await this.importButton.click();
    await this.importModal.waitFor({ state: 'visible' });
  }

  async chooseFileUploadSource(): Promise<void> {
    await this.importModal.getByText('Upload Files', { exact: true }).click();
    await this.importModal.getByText(/Supported formats:/).waitFor({ state: 'visible' });
  }

  async uploadFiles(files: { name: string; mimeType: string; buffer: Buffer }[]): Promise<void> {
    await this.importModal.locator('input[type="file"]').setInputFiles(files);
  }

  async submitImport(): Promise<void> {
    await this.importModal.getByRole('button', { name: 'Import' }).click();
  }

  async expectImportComplete(): Promise<void> {
    await this.importModal.getByText('Import Complete').waitFor({ state: 'visible', timeout: 120_000 });
  }

  async dismissImportModal(): Promise<void> {
    await this.importModal.getByRole('button', { name: 'Done' }).click();
    await this.importModal.waitFor({ state: 'hidden' });
  }
}
