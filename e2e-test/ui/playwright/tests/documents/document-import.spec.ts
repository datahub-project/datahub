/**
 * Context document file import — Playwright coverage for the Upload Files flow.
 *
 * Replaces smoke-test/tests/knowledge/document_import_test.py (direct GraphQL).
 * GitHub imports are covered by the github-documents ingestion source separately.
 */

import { test, expect } from '../../fixtures/base-test';
import {
  createDocument,
  getDocumentParentUrn,
  getDocumentSourceType,
  waitForImportDocumentsFromFiles,
} from '../../helpers/document-import-helper';
import { ContextDocumentsPage } from '../../pages/context-documents.page';

test.describe('context document file import', () => {
  test.setTimeout(120_000);

  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({
      contextDocumentsEnabled: true,
      showNavBarRedesign: true,
      showHomePageRedesign: true,
    });
  });

  test('imports a text file via the import modal', async ({ page, cleanup, logger, logDir }) => {
    const fileName = `pw-import-${Date.now()}.txt`;
    const fileContent = '# Playwright Import\n\nSmoke test content for document import.';
    const contextDocuments = new ContextDocumentsPage(page, logger, logDir);

    await contextDocuments.navigateToContextDocuments();
    await contextDocuments.openImportModal();
    await contextDocuments.chooseFileUploadSource();
    await contextDocuments.uploadFiles([{ name: fileName, mimeType: 'text/plain', buffer: Buffer.from(fileContent) }]);

    const importResponse = waitForImportDocumentsFromFiles(page);
    await contextDocuments.submitImport();
    await contextDocuments.expectImportComplete();

    const result = await importResponse;
    expect(result.failedCount).toBe(0);
    expect(result.createdCount).toBeGreaterThanOrEqual(1);
    expect(result.documentUrns.length).toBeGreaterThanOrEqual(1);

    const documentUrn = result.documentUrns[0];
    cleanup.track(documentUrn);

    expect(await getDocumentSourceType(page, documentUrn)).toBe('NATIVE');

    await contextDocuments.dismissImportModal();
  });

  test('imports a file under the selected parent document', async ({ page, cleanup, logger, logDir }) => {
    const parentId = `pw-import-parent-${Date.now()}`;
    const parentUrn = await createDocument(page, parentId, `Parent ${parentId}`);
    cleanup.track(parentUrn);

    const fileName = `pw-child-${Date.now()}.md`;
    const fileContent = 'Child document content for hierarchy test.';
    const contextDocuments = new ContextDocumentsPage(page, logger, logDir);

    await contextDocuments.navigateToDocument(parentUrn);
    await contextDocuments.openImportModal();
    await contextDocuments.chooseFileUploadSource();
    await contextDocuments.uploadFiles([
      { name: fileName, mimeType: 'text/markdown', buffer: Buffer.from(fileContent) },
    ]);

    const importResponse = waitForImportDocumentsFromFiles(page);
    await contextDocuments.submitImport();
    await contextDocuments.expectImportComplete();

    const result = await importResponse;
    expect(result.createdCount).toBeGreaterThanOrEqual(1);

    const childUrn = result.documentUrns[0];
    cleanup.track(childUrn);
    expect(await getDocumentParentUrn(page, childUrn)).toBe(parentUrn);

    await contextDocuments.dismissImportModal();
  });

  test('re-importing the same file updates the existing document', async ({ page, cleanup, logger, logDir }) => {
    const fileName = `pw-idempotent-${Date.now()}.txt`;
    const fileContent = 'Idempotency test content';
    const contextDocuments = new ContextDocumentsPage(page, logger, logDir);

    const importOnce = async () => {
      await contextDocuments.navigateToContextDocuments();
      await contextDocuments.openImportModal();
      await contextDocuments.chooseFileUploadSource();
      await contextDocuments.uploadFiles([
        { name: fileName, mimeType: 'text/plain', buffer: Buffer.from(fileContent) },
      ]);
      const importResponse = waitForImportDocumentsFromFiles(page);
      await contextDocuments.submitImport();
      await contextDocuments.expectImportComplete();
      const result = await importResponse;
      await contextDocuments.dismissImportModal();
      return result;
    };

    const first = await importOnce();
    expect(first.createdCount).toBe(1);
    const urn1 = first.documentUrns[0];
    cleanup.track(urn1);

    const second = await importOnce();
    expect(second.updatedCount).toBe(1);
    expect(second.documentUrns[0]).toBe(urn1);
  });
});
