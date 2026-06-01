/**
 * External document profile tests — smoke coverage for the external document entity page.
 *
 * Asserts that:
 *   - The document title and summary tab load correctly
 *   - The "Last Synced" property is populated (validates the DEFAULT_RUN_ID fallback fix)
 *   - The inline content section renders with its info banner and markdown body
 *   - The inline content section has no edit controls (read-only)
 *
 * Prerequisites: `urn:li:document:playwright-external-doc-test` must exist —
 * seeded via `tests/documents/fixtures/data.json`.
 */

import { test, expect } from '../../fixtures/base-test';

test.use({ featureName: 'documents' });

const DOCUMENT_URN = 'urn:li:document:playwright-external-doc-test';
const DOCUMENT_TITLE = 'Playwright External Document';

test.describe('external document profile', () => {
  test.skip(true, 'Data seeding failure. AI-617');
  test.beforeEach(async ({ apiMock }) => {
    await apiMock.setFeatureFlags({
      contextDocumentsEnabled: true,
      showNavBarRedesign: true,
      showHomePageRedesign: true,
    });
  });

  test('loads summary tab with inline content and last synced property', async ({ page }) => {
    await page.goto(`/document/${encodeURIComponent(DOCUMENT_URN)}`);

    // Title confirms the entity loaded
    await expect(page.getByText(DOCUMENT_TITLE).first()).toBeVisible({ timeout: 15000 });

    // "Last Synced" property should be populated (validates the lastObserved fallback in DocumentMapper)
    await expect(page.getByText('Last Synced').first()).toBeVisible({ timeout: 10000 });

    // Inline content section renders
    const contentSection = page.getByTestId('external-document-inline-content-section');
    await expect(contentSection).toBeVisible({ timeout: 10000 });

    // Info banner explains the content is extracted and may be lossy
    const banner = page.getByTestId('external-document-inline-banner');
    await expect(banner).toBeVisible();
    await expect(banner).toContainText('Text extracted from');
    await expect(banner).toContainText('Notion');

    // Markdown body renders the document contents
    await expect(contentSection.getByText('Playwright smoke-test document')).toBeVisible();
  });
});
