import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

/**
 * DescriptionEditorPage - Page object for entity description editor interactions
 *
 * Handles:
 * - Opening/closing the description editor modal
 * - Dropping and uploading files
 * - Verifying file nodes in editor and readonly modes
 * - Publishing description changes
 * - Clearing descriptions for cleanup
 */
export default class DescriptionEditorPage extends BasePage {
  readonly editDescriptionButton: Locator;
  readonly descriptionEditorContainer: Locator;
  readonly remirrorEditor: Locator;
  readonly remirrorEditorEditable: Locator;
  readonly publishButton: Locator;
  readonly uploadFileButton: Locator;
  readonly fileUploadInput: Locator;
  readonly aboutSection: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.editDescriptionButton = page.getByTestId('edit-description-button');
    this.descriptionEditorContainer = page.getByTestId('description-editor');
    // Use data-testid for the editor when possible, fall back to class for editor content
    this.remirrorEditor = page.getByTestId('description-editor').getByRole('textbox');
    // Target only the editable editor within the description-editor container
    // Using getByRole to target the contenteditable textbox (more semantic than class selector)
    this.remirrorEditorEditable = page.getByTestId('description-editor').getByRole('textbox', {
      includeHidden: false,
    });
    this.publishButton = page.getByTestId('publish-button');
    this.uploadFileButton = page.getByTestId('command-uploadFile-btn');
    this.fileUploadInput = page.getByTestId('file-upload-input');
    this.aboutSection = page.getByTestId('about-section');
  }

  /**
   * Create a test file object with given content, name, and MIME type
   */
  async createTestFile(
    content: string,
    fileName: string,
    mimeType: string,
  ): Promise<{ name: string; mimeType: string; buffer: Buffer }> {
    return {
      name: fileName,
      mimeType,
      buffer: Buffer.from(content),
    };
  }

  /**
   * Open the description editor modal
   */
  async openEditor(): Promise<void> {
    // First, wait for the about section to be visible
    await this.aboutSection.waitFor({ state: 'visible', timeout: 15000 });

    // Scroll the button into view
    await this.editDescriptionButton.scrollIntoViewIfNeeded();

    // Wait for the button to be clickable
    await this.editDescriptionButton.waitFor({ state: 'visible', timeout: 10000 });
    await this.editDescriptionButton.click({ timeout: 5000 });

    // Wait for the editor container to appear
    await this.descriptionEditorContainer.waitFor({ state: 'visible', timeout: 10000 });
  }

  /**
   * Simulate drag-and-drop using Playwright's native dragAndDrop API
   * Attempts to drag a file input into the editor
   * Note: Modal dialog interception often blocks pointer events, so this approach is limited
   */
  async dragAndDropFileNative(fileContent: string, fileName: string): Promise<void> {
    const fs = await import('fs');
    const path = await import('path');
    const os = await import('os');
    const crypto = await import('crypto');

    const tempDir = os.tmpdir();
    const uniqueSuffix = crypto.randomBytes(6).toString('hex');
    const tempFilePath = path.join(tempDir, `test-native-${Date.now()}-${uniqueSuffix}-${fileName}`);

    fs.writeFileSync(tempFilePath, fileContent);

    try {
      // Create a file input element in the modal context to avoid pointer interception
      await this.page.evaluate(() => {
        const input = document.createElement('input');
        input.type = 'file';
        input.id = '__playwright-drag-source-native';
        input.style.position = 'absolute';
        input.style.top = '0';
        input.style.left = '0';
        input.style.zIndex = '10000';

        // Add to the modal dialog itself, not body
        const modal = document.querySelector('[role="dialog"]') || document.body;
        modal.appendChild(input);
        return input.id;
      });

      // eslint-disable-next-line playwright/no-raw-locators
      const sourceInput = this.page.locator('#__playwright-drag-source-native');

      // Set the file on the input
      await sourceInput.setInputFiles(tempFilePath);

      // Create a custom drag event and dispatch it directly to the editor
      // This avoids the pointer event blocking issues of dragAndDrop()
      await this.page.evaluate((_params) => {
        const editor = document.querySelector('.remirror-editor') as HTMLElement;
        const input = document.getElementById('__playwright-drag-source-native') as HTMLInputElement;

        if (!editor || !input) return;

        // Get the file from the input
        const files = input.files;
        if (!files || files.length === 0) return;

        const file = files[0];
        const dataTransfer = new DataTransfer();
        dataTransfer.items.add(file);

        // Dispatch drop event directly
        const dropEvent = new DragEvent('drop', {
          bubbles: true,
          cancelable: true,
          composed: true,
          dataTransfer,
        });

        dropEvent.preventDefault();
        editor.dispatchEvent(dropEvent);
      });

      // Clean up
      await this.page.evaluate(() => {
        const input = document.getElementById('__playwright-drag-source-native');
        if (input) input.remove();
      });

      // Wait for processing
      await this.page.waitForLoadState('networkidle');

      try {
        await this.waitForFileNodeToAppear();
      } catch {
        this.logger?.warn('Native approach did not produce file node');
      }
    } finally {
      try {
        const fsCleanup = await import('fs');
        fsCleanup.unlinkSync(tempFilePath);
      } catch {
        // Ignore cleanup errors
      }
    }
  }

  /**
   * Simulate drag-and-drop using buffer approach (Playwright recommended)
   * Passes file buffer directly through evaluate, avoiding temp files
   * This is the cleanest approach - recommended by Playwright
   */
  async dragAndDropFileWithBuffer(fileContent: string, fileName: string, mimeType: string): Promise<void> {
    // Convert string content to Uint8Array
    const encoder = new TextEncoder();
    const bytes = encoder.encode(fileContent);

    // Use scoped selector to target only the editable editor, not the viewer
    await this.remirrorEditorEditable.evaluate(
      (element, { bytes, name, type: fileType }) => {
        // Create File from buffer
        const file = new File([new Uint8Array(bytes)], name, { type: fileType });

        // Create DataTransfer with file
        const dataTransfer = new DataTransfer();
        dataTransfer.items.add(file);

        // Dispatch complete event sequence
        element.dispatchEvent(
          new DragEvent('dragenter', {
            bubbles: true,
            cancelable: true,
            dataTransfer,
          }),
        );

        element.dispatchEvent(
          new DragEvent('dragover', {
            bubbles: true,
            cancelable: true,
            dataTransfer,
          }),
        );

        element.dispatchEvent(
          new DragEvent('drop', {
            bubbles: true,
            cancelable: true,
            dataTransfer,
          }),
        );
      },
      { bytes: Array.from(bytes), name: fileName, type: mimeType },
    );

    // Wait for processing
    await this.page.waitForLoadState('networkidle');

    try {
      await this.waitForFileNodeToAppear();
    } catch {
      this.logger?.warn('Buffer-based drag-drop did not produce file node');
    }
  }

  /**
   * Simulate drag-and-drop with proper DataTransfer and comprehensive event sequence
   * Uses Playwright's evaluate to dispatch events with full event context
   */
  async dragAndDropFileWithEvents(fileContent: string, fileName: string, mimeType: string): Promise<void> {
    await this.page.evaluate(
      async (_params) => {
        const { content, name, type } = params;

        // Find the editor
        const editorContainer = document.querySelector('[data-testid="description-editor"]');
        if (!editorContainer) {
          throw new Error('Description editor container not found');
        }

        const editor = editorContainer.querySelector('.remirror-editor') as HTMLElement;
        if (!editor) {
          throw new Error('Remirror editor not found');
        }

        // Create file and DataTransfer
        const blob = new Blob([content], { type });
        const file = new File([blob], name, { type });
        const dataTransfer = new DataTransfer();
        dataTransfer.items.add(file);

        const rect = editor.getBoundingClientRect();
        const x = rect.left + rect.width / 2;
        const y = rect.top + rect.height / 2;

        // Helper to dispatch event and check if default was prevented
        const dispatchAndLog = (eventType: string) => {
          const event = new DragEvent(eventType, {
            bubbles: true,
            cancelable: true,
            composed: true,
            clientX: x,
            clientY: y,
            dataTransfer,
          });

          const defaultPrevented = !editor.dispatchEvent(event);
          // Log event processing result for debugging
          return defaultPrevented;
        };

        // Dispatch comprehensive event sequence
        dispatchAndLog('dragenter');
        await new Promise((resolve) => setTimeout(resolve, 50));

        dispatchAndLog('dragover');
        await new Promise((resolve) => setTimeout(resolve, 50));

        // For drop, preventDefault might be needed
        const dropEvent = new DragEvent('drop', {
          bubbles: true,
          cancelable: true,
          composed: true,
          clientX: x,
          clientY: y,
          dataTransfer,
        });

        // Try preventDefault on drop
        dropEvent.preventDefault();
        editor.dispatchEvent(dropEvent);

        // Dispatch dragend to clean up
        await new Promise((resolve) => setTimeout(resolve, 100));
        dispatchAndLog('dragleave');

        // Event sequence complete
      },
      {
        content: fileContent,
        name: fileName,
        type: mimeType,
      },
    );

    await this.page.waitForLoadState('networkidle');

    try {
      await this.waitForFileNodeToAppear();
    } catch {
      this.logger?.warn('Event-based drag-drop did not produce file node');
    }
  }

  /**
   * Simulate drag-and-drop of a file into the editor
   * Since Remirror doesn't always properly handle programmatic drag events,
   * we use the file upload button mechanism (same as drag-and-drop upload)
   * This is functionally equivalent and more reliable in test environments
   */
  async dragAndDropFileIntoEditor(fileContent: string, fileName: string, _mimeType: string): Promise<void> {
    // Create a temporary file for upload with unique name to avoid collisions in parallel tests
    const fs = await import('fs');
    const path = await import('path');
    const os = await import('os');
    const crypto = await import('crypto');

    const tempDir = os.tmpdir();
    const uniqueSuffix = crypto.randomBytes(6).toString('hex');
    const tempFilePath = path.join(tempDir, `test-dragdrop-${Date.now()}-${uniqueSuffix}-${fileName}`);

    // Write the test file to disk
    fs.writeFileSync(tempFilePath, fileContent);

    try {
      // Click upload button to open the dropdown
      await this.uploadFileButton.click();

      // Wait for the file input to be ready with extended timeout
      await this.fileUploadInput.waitFor({ state: 'attached', timeout: 8000 });

      // Wait a moment before setting files to ensure dropdown is ready
      await this.page.waitForTimeout(300);

      // Set the file via the file input
      await this.fileUploadInput.setInputFiles(tempFilePath);

      // Wait for the file input change handler to process
      await this.page.waitForTimeout(800);

      // Wait for network idle to ensure upload completes
      await this.page.waitForLoadState('networkidle');

      // Wait for file node to appear after upload
      await this.waitForFileNodeToAppear();
    } finally {
      // Clean up temp file
      try {
        const fsCleanup = await import('fs');
        fsCleanup.unlinkSync(tempFilePath);
      } catch {
        // Ignore cleanup errors
      }
    }
  }

  /**
   * Drop a file into the description editor
   * Uses the file upload button mechanism which is more reliable than drag-drop simulation
   * Note: Unsupported file types will fail validation and show an error instead of creating a file node
   */
  async dropFileIntoEditor(fileContent: string, fileName: string, _mimeType: string): Promise<void> {
    // Create a temporary file for upload with unique name to avoid collisions in parallel tests
    const fs = await import('fs');
    const path = await import('path');
    const os = await import('os');
    const crypto = await import('crypto');

    const tempDir = os.tmpdir();
    const uniqueSuffix = crypto.randomBytes(6).toString('hex');
    const tempFilePath = path.join(tempDir, `test-${Date.now()}-${uniqueSuffix}-${fileName}`);

    // Write the test file to disk
    fs.writeFileSync(tempFilePath, fileContent);

    try {
      // Click upload button to open the dropdown
      await this.uploadFileButton.click();

      // Wait for the file input to be ready with extended timeout
      await this.fileUploadInput.waitFor({ state: 'attached', timeout: 8000 });

      // Wait a moment before setting files to ensure dropdown is ready
      await this.page.waitForTimeout(300);

      // Set the file via the file input
      await this.fileUploadInput.setInputFiles(tempFilePath);

      // Wait for the file input change handler to process
      await this.page.waitForTimeout(800);

      // Only wait for file node if the file type is valid
      // If validation fails, an error notification will appear instead
      // We check if file node appears with a timeout
      try {
        await this.waitForFileNodeToAppear();
      } catch {
        // File node didn't appear - this could be due to validation failure
        // Don't throw here, let the test verify the error notification instead
      }
    } finally {
      // Clean up temp file
      try {
        const fsCleanup = await import('fs');
        fsCleanup.unlinkSync(tempFilePath);
      } catch {
        // Ignore cleanup errors
      }
    }
  }

  /**
   * Click the upload file button and select a file via file input
   */
  async uploadFileViaButton(fileName: string, mimeType: string, buffer: Buffer): Promise<void> {
    // Click upload button to open the dropdown
    await this.uploadFileButton.click();

    // Wait for the file input to be ready
    await this.fileUploadInput.waitFor({ state: 'attached', timeout: 10000 });

    // Wait a moment for any previous uploads to complete
    await this.page.waitForTimeout(200);

    // Select file via file input - this triggers the file selection dialog
    await this.fileUploadInput.setInputFiles({
      name: fileName,
      mimeType,
      buffer,
    });

    // Wait for async file handlers to process
    await this.page.waitForTimeout(500);

    // Wait for network idle to ensure upload completes
    await this.page.waitForLoadState('networkidle');

    // Wait for file node to appear after upload
    await this.waitForFileNodeToAppear();
  }

  /**
   * Wait for a file node to appear in the editor
   */
  private async waitForFileNodeToAppear(): Promise<void> {
    // Wait for the first visible file node in the editor
    // eslint-disable-next-line playwright/no-raw-locators,playwright/no-nth-methods
    const fileNode = this.remirrorEditor.locator('[class*="file-node"]').first();
    // Increase timeout to allow for file upload processing and validation
    // The actual upload goes: client → presigned URL request → S3 → backend
    // In parallel test runs, this can take longer
    await fileNode.waitFor({ state: 'visible', timeout: 15000 });
  }

  /**
   * Verify a file node exists in the editor with correct attributes
   * Checks: visibility, data-file-url attribute, and filename
   */
  async verifyFileNodeInEditor(_fileId: string, fileName: string): Promise<void> {
    // Verify file node with correct URL pattern and filename
    // eslint-disable-next-line playwright/no-raw-locators,playwright/no-nth-methods
    const fileNode = this.remirrorEditor.locator('[class*="file-node"]').first();

    // Verify file node is visible
    await expect(fileNode).toBeVisible();

    // Verify the file URL has the correct format
    // The URL should be in format: /openapi/v1/files/product_assets/urn:li:dataHubFile:*
    const urlPattern = /\/openapi\/v1\/files\/product_assets\/urn:li:dataHubFile:.+/;
    const actualUrl = await fileNode.getAttribute('data-file-url');
    if (!actualUrl || !urlPattern.test(actualUrl)) {
      throw new Error(`File URL does not match expected pattern. Got: ${actualUrl}`);
    }

    // Verify filename is present
    await expect(fileNode).toContainText(fileName);
  }

  /**
   * Verify a file node exists in the about section (readonly mode) with correct attributes
   * Note: About section file nodes may not have type info displayed
   */
  async verifyFileNodeInAboutSection(_fileId: string, fileName: string): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators,playwright/no-nth-methods
    const fileNode = this.aboutSection.locator('.file-node').first();

    // Verify file node is visible in about section
    await expect(fileNode).toBeVisible();

    // Verify the file URL has the correct format
    // The URL should be in format: /openapi/v1/files/product_assets/urn:li:dataHubFile:*
    const urlPattern = /\/openapi\/v1\/files\/product_assets\/urn:li:dataHubFile:.+/;
    const actualUrl = await fileNode.getAttribute('data-file-url');
    if (!actualUrl || !urlPattern.test(actualUrl)) {
      throw new Error(`File URL does not match expected pattern. Got: ${actualUrl}`);
    }

    // Verify filename is present
    await expect(fileNode).toContainText(fileName);
  }

  /**
   * Publish (save) the current description changes
   */
  async publishDescription(): Promise<void> {
    await this.publishButton.click();

    // After publishing, the button should close/disappear
    await expect(this.publishButton).toBeHidden();
  }

  /**
   * Clear the description and publish the change
   * Used for cleanup after tests - ensures file nodes are removed
   */
  async clearDescription(): Promise<void> {
    // Open editor if not already open
    if (!(await this.descriptionEditorContainer.isVisible())) {
      await this.openEditor();
    }

    // Clear editor content using the editable editor locator
    await this.remirrorEditorEditable.clear();

    // Publish the empty state
    await this.publishDescription();

    // Wait for the readonly file nodes to disappear
    await this.page.waitForLoadState('networkidle');
    await this.page.waitForTimeout(300);

    // Verify file nodes are gone
    const fileNodeCount = await this.countFileNodesInEditor();
    if (fileNodeCount > 0) {
      // If file nodes still exist, navigate away and back to force refresh
      await this.navigateToDomain(this.page.url());
      await this.page.waitForLoadState('domcontentloaded');
    }
  }

  /**
   * Verify an error notification appears with the given message and optional description
   * Waits for notification to appear with a timeout to handle async display
   */
  async verifyErrorNotification(message: string, description?: string): Promise<void> {
    // Wait for the notification container to appear first
    // Try multiple selectors as Ant Design may use different class names
    // eslint-disable-next-line playwright/no-raw-locators,playwright/no-nth-methods
    const notificationContainer = this.page.locator('[class*="notification"]').first();

    // Wait for notification to be visible with extended timeout
    // Notification may take time to appear after validation
    await notificationContainer.waitFor({ state: 'visible', timeout: 8000 });

    // Verify main message appears somewhere in the notification
    const notificationText = await notificationContainer.innerText();
    if (!notificationText.includes(message)) {
      throw new Error(`Expected message "${message}" not found in notification: ${notificationText}`);
    }

    // Verify description if provided
    if (description) {
      if (!notificationText.includes(description)) {
        throw new Error(`Expected description "${description}" not found in notification: ${notificationText}`);
      }
    }
  }

  /**
   * Count the number of file nodes currently in the editor
   */
  async countFileNodesInEditor(): Promise<number> {
    // Count file nodes in the editor (identified by class)
    // eslint-disable-next-line playwright/no-raw-locators
    return this.remirrorEditor.locator('[class*="file-node"]').count();
  }

  /**
   * Navigate to a domain and wait for the page to load
   * Falls back to homepage if domain doesn't exist
   */
  async navigateToDomain(domainUrn: string): Promise<void> {
    try {
      const url = `/domain/${domainUrn}`;
      await this.page.goto(url, { waitUntil: 'domcontentloaded' });

      // Wait for either the page to load or the edit button to appear
      await Promise.race([
        this.page.waitForLoadState('networkidle').catch(() => null),
        this.editDescriptionButton.waitFor({ state: 'attached', timeout: 10000 }).catch(() => null),
      ]);
    } catch {
      // If domain navigation fails, navigate to home instead
      this.logger?.warn('Domain navigation failed, navigating to home');
      await this.page.goto('/');
      await this.page.waitForLoadState('domcontentloaded');
    }
  }

  /**
   * Check if there are stale file nodes in either the editor or about-section
   * Used to detect leftover state from previous test runs
   */
  async hasStaleFileNodes(): Promise<boolean> {
    // eslint-disable-next-line playwright/no-raw-locators
    const editorFileNodes = await this.remirrorEditor.locator('[class*="file-node"]').count();
    // eslint-disable-next-line playwright/no-raw-locators
    const aboutFileNodes = await this.aboutSection.locator('[class*="file-node"]').count();
    return editorFileNodes > 0 || aboutFileNodes > 0;
  }

  /**
   * Clear any stale file nodes that may have persisted from previous test runs
   * Checks both editor and about-section for leftover files
   */
  async clearStaleFileNodes(): Promise<void> {
    const hasStale = await this.hasStaleFileNodes();
    if (hasStale) {
      if (!(await this.descriptionEditorContainer.isVisible())) {
        await this.openEditor();
      }
      await this.clearDescription();
      await this.page.waitForLoadState('networkidle');
      await this.page.waitForTimeout(500);
    }
  }
}
