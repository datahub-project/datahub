/**
 * Upload Files feature tests
 *
 * Tests the description editor file upload functionality:
 *   1. File upload via drag-and-drop (buffer-based)
 *   2. File upload via button selection
 *   3. File persistence in read-only mode after publish
 *   4. File type validation and rejection
 *
 * Uses shared domain for isolation. Cleanup handled via clearStaleFileNodes.
 */

import { test } from '../../fixtures/base-test';
import { Route } from '@playwright/test';
import { generateRandomString } from '../../utils/random';
import DescriptionEditorPage from '../../pages/description-editor.page';
import type { ApiMocker } from '../../utils/api-mock';

test.describe('Upload Files', () => {
  const DOMAIN_URN = 'urn:li:domain:marketing';

  test.beforeEach(async ({ page, apiMock, logger, logDir }) => {
    // Enable feature flags for all tests
    await apiMock.setFeatureFlags({
      documentationFileUploadV1: true,
      assetSummaryPageV1: true,
    });

    // Clean up any stale readonly file nodes from previous test runs
    const editor = new DescriptionEditorPage(page, logger, logDir);
    await editor.navigateToDomain(DOMAIN_URN);
    await editor.clearStaleFileNodes();
  });

  const setupPresignedUrlMock = async (apiMock: ApiMocker, testId: string): Promise<void> => {
    const baseUrl = process.env.BASE_URL || 'http://localhost:9002';

    await apiMock.mockGraphQL('getPresignedUploadUrl', {
      getPresignedUploadUrl: {
        url: `${baseUrl}/presigned_url`,
        fileId: `urn:li:dataHubFile:test_${testId}`,
      },
    });

    await apiMock.mockRoute('**/presigned_url', async (route: Route) => {
      if (route.request().method() === 'PUT') {
        await route.fulfill({ status: 200, body: '' });
      } else {
        await route.fallback();
      }
    });
  };

  test('should allow to drop file', async ({ page, logger, logDir, apiMock }) => {
    const testId = generateRandomString();
    await setupPresignedUrlMock(apiMock, testId);

    const editor = new DescriptionEditorPage(page, logger, logDir);
    await editor.navigateToDomain(DOMAIN_URN);
    await editor.openEditor();

    const fileContent = 'content';
    const fileName = 'drop-file.txt';
    const mimeType = 'text/plain';

    await editor.dragAndDropFileWithBuffer(fileContent, fileName, mimeType);

    const fileId = `urn:li:dataHubFile:test_${testId}`;
    await editor.verifyFileNodeInEditor(fileId, fileName);
  });

  test('should allow to upload file by button', async ({ page, logger, logDir, apiMock }) => {
    const testId = generateRandomString();
    await setupPresignedUrlMock(apiMock, testId);

    const editor = new DescriptionEditorPage(page, logger, logDir);
    await editor.navigateToDomain(DOMAIN_URN);
    await editor.openEditor();

    const fileContent = 'content';
    const fileName = 'button-upload.txt';
    const mimeType = 'text/plain';
    const buffer = Buffer.from(fileContent);

    await editor.uploadFileViaButton(fileName, mimeType, buffer);

    const fileId = `urn:li:dataHubFile:test_${testId}`;
    await editor.verifyFileNodeInEditor(fileId, fileName);
  });

  test('should render file in readonly mode', async ({ page, logger, logDir, apiMock }) => {
    const testId = generateRandomString();
    await setupPresignedUrlMock(apiMock, testId);

    const editor = new DescriptionEditorPage(page, logger, logDir);
    await editor.navigateToDomain(DOMAIN_URN);
    await editor.openEditor();

    const fileContent = 'content';
    const fileName = 'readonly.txt';
    const mimeType = 'text/plain';

    await editor.dragAndDropFileWithBuffer(fileContent, fileName, mimeType);

    const fileId = `urn:li:dataHubFile:test_${testId}`;
    await editor.verifyFileNodeInEditor(fileId, fileName);

    // Publish to switch to read-only mode
    await editor.publishDescription();

    // Verify file persists in readonly display
    await editor.verifyFileNodeInAboutSection(fileId, fileName);

    // Cleanup: clear description for next test
    await editor.clearDescription();
  });

  test('should validate file type', async ({ page, logger, logDir, apiMock }) => {
    const testId = generateRandomString();
    await setupPresignedUrlMock(apiMock, testId);

    const editor = new DescriptionEditorPage(page, logger, logDir);
    await editor.navigateToDomain(DOMAIN_URN);
    await editor.openEditor();

    // Attempt to upload unsupported file type
    const unsupportedContent = 'Unsupported file';
    const unsupportedFileName = 'test.unknown';
    const unsupportedMimeType = 'unknown';

    await editor.dropFileIntoEditor(unsupportedContent, unsupportedFileName, unsupportedMimeType);

    // Verify unsupported file was not added to editor
    await editor.verifyFileNodeDoesNotExist(unsupportedFileName);
  });
});
