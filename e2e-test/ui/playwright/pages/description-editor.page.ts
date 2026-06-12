import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { TIMEOUTS, TABLE_LOAD_DELAY, LOAD_STATES } from '../utils/constants';
import { writeFile, deleteFileIfExists } from '../helpers/file-utils';

// Page object for entity description editor interactions
export default class DescriptionEditorPage extends BasePage {
  readonly editDescriptionButton: Locator;
  readonly descriptionEditorContainer: Locator;
  readonly remirrorEditor: Locator;
  readonly remirrorEditorEditable: Locator;
  readonly publishButton: Locator;
  readonly uploadFileButton: Locator;
  readonly fileUploadInput: Locator;
  readonly aboutSection: Locator;
  readonly editorFileNodesContainer: Locator;
  readonly aboutFileNodesContainer: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.editDescriptionButton = page.getByTestId('edit-description-button');
    this.descriptionEditorContainer = page.getByTestId('description-editor');
    this.remirrorEditor = page.getByTestId('description-editor').getByRole('textbox');
    this.remirrorEditorEditable = page.getByTestId('description-editor').getByRole('textbox', {
      includeHidden: false,
    });
    this.publishButton = page.getByTestId('publish-button');
    this.uploadFileButton = page.getByTestId('command-uploadFile-btn');
    this.fileUploadInput = page.getByTestId('file-upload-input');
    this.aboutSection = page.getByTestId('about-section');
    this.editorFileNodesContainer = this.remirrorEditor;
    this.aboutFileNodesContainer = this.aboutSection;
  }

  private getFileNodeTestId(fileName: string): string {
    return `file-node-${fileName}`;
  }

  async verifyFileNodeDoesNotExist(fileName: string): Promise<void> {
    const count = await this.editorFileNodesContainer.getByTestId(this.getFileNodeTestId(fileName)).count();
    if (count > 0) {
      throw new Error(`File node for "${fileName}" should not exist but was found`);
    }
  }

  async openEditor(): Promise<void> {
    await this.aboutSection.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    await this.editDescriptionButton.scrollIntoViewIfNeeded();
    await this.editDescriptionButton.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.editDescriptionButton.click({ timeout: TIMEOUTS.SHORT });
    await this.descriptionEditorContainer.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
  }

  // Simulate drag-and-drop using Playwright's buffer approach (avoids temp files)
  async dragAndDropFileWithBuffer(fileContent: string, fileName: string, mimeType: string): Promise<void> {
    const encoder = new TextEncoder();
    const bytes = encoder.encode(fileContent);

    await this.remirrorEditorEditable.evaluate(
      (element, { bytes, name, type: fileType }) => {
        const file = new File([new Uint8Array(bytes)], name, { type: fileType });
        const dataTransfer = new DataTransfer();
        dataTransfer.items.add(file);
        element.dispatchEvent(new DragEvent('dragenter', { bubbles: true, cancelable: true, dataTransfer }));
        element.dispatchEvent(new DragEvent('dragover', { bubbles: true, cancelable: true, dataTransfer }));
        element.dispatchEvent(new DragEvent('drop', { bubbles: true, cancelable: true, dataTransfer }));
      },
      { bytes: Array.from(bytes), name: fileName, type: mimeType },
    );

    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await this.waitForFileNodeToAppear(fileName);
  }

  // Upload via button with temp file (handles unsupported file type validation)
  async dropFileIntoEditor(fileContent: string, fileName: string, _mimeType: string): Promise<void> {
    const tempFilePath = writeFile(fileContent, fileName);

    try {
      await this.uploadFileButton.click();
      await this.fileUploadInput.waitFor({ state: 'attached', timeout: TIMEOUTS.MEDIUM });
      await this.fileUploadInput.setInputFiles(tempFilePath);
      await this.page.waitForTimeout(TABLE_LOAD_DELAY);
      // For invalid files, backend validation rejects silently - verify file wasn't added
      const fileNode = this.editorFileNodesContainer.getByTestId(this.getFileNodeTestId(fileName));
      await expect(fileNode).toBeHidden();
    } finally {
      deleteFileIfExists(tempFilePath);
    }
  }

  async uploadFileViaButton(fileName: string, mimeType: string, buffer: Buffer): Promise<void> {
    await this.uploadFileButton.click();
    await this.fileUploadInput.waitFor({ state: 'attached', timeout: TIMEOUTS.MEDIUM });
    await this.fileUploadInput.setInputFiles({ name: fileName, mimeType, buffer });
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await this.waitForFileNodeToAppear(fileName);
  }

  private async waitForFileNodeToAppear(fileName: string): Promise<void> {
    const fileNode = this.editorFileNodesContainer.getByTestId(this.getFileNodeTestId(fileName));
    // Extra-long timeout: upload → S3 → backend processing, especially in parallel tests
    await fileNode.waitFor({ state: 'visible', timeout: TIMEOUTS.EXTRA_LONG });
  }

  async verifyFileNodeInEditor(_fileId: string, fileName: string): Promise<void> {
    const fileNode = this.editorFileNodesContainer.getByTestId(this.getFileNodeTestId(fileName));
    await expect(fileNode).toBeVisible();

    const urlPattern = /\/openapi\/v1\/files\/product_assets\/urn:li:dataHubFile:.+/;
    const actualUrl = await fileNode.getAttribute('data-file-url');
    if (!actualUrl || !urlPattern.test(actualUrl)) {
      throw new Error(`File URL does not match expected pattern. Got: ${actualUrl}`);
    }
    await expect(fileNode).toContainText(fileName);
  }

  async verifyFileNodeInAboutSection(_fileId: string, fileName: string): Promise<void> {
    const fileNode = this.aboutFileNodesContainer.getByTestId(this.getFileNodeTestId(fileName));
    await expect(fileNode).toBeVisible();

    const urlPattern = /\/openapi\/v1\/files\/product_assets\/urn:li:dataHubFile:.+/;
    const actualUrl = await fileNode.getAttribute('data-file-url');
    if (!actualUrl || !urlPattern.test(actualUrl)) {
      throw new Error(`File URL does not match expected pattern. Got: ${actualUrl}`);
    }
    await expect(fileNode).toContainText(fileName);
  }

  async publishDescription(): Promise<void> {
    await this.publishButton.click();
    await expect(this.publishButton).toBeHidden();
  }

  async clearDescription(): Promise<void> {
    if (!(await this.descriptionEditorContainer.isVisible())) {
      await this.openEditor();
    }
    await this.remirrorEditorEditable.clear();
    await this.publishDescription();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async navigateToDomain(domainUrn: string): Promise<void> {
    const url = `/domain/${domainUrn}`;
    await this.page.goto(url, { waitUntil: LOAD_STATES.DOMCONTENTLOADED });
    await Promise.race([
      this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE).catch(() => null),
      this.editDescriptionButton.waitFor({ state: 'attached', timeout: TIMEOUTS.MEDIUM }).catch(() => null),
    ]);
  }

  async clearStaleFileNodes(): Promise<void> {
    if (!(await this.descriptionEditorContainer.isVisible())) {
      await this.openEditor();
    }
    await this.clearDescription();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await this.page.waitForTimeout(TABLE_LOAD_DELAY);
  }
}
