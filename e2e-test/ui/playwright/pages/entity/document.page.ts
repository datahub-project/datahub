/**
 * DocumentPage — page object for the DataHub Document entity profile.
 *
 * Extends BaseEntityPage with document-specific navigation and CRUD operations.
 */

import { Page, expect } from '@playwright/test';
import { BaseEntityPage } from './base-entity.page';
import type { DataHubLogger } from '../../utils/logger';
import { DOCUMENT_SELECTORS } from '../../selectors/documents.selectors';
import { TIMEOUTS, WAITS } from '../../tests/entity-pages/constants';

export class DocumentPage extends BaseEntityPage {
  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
  }

  // ── Document URN utilities ────────────────────────────────────────────────

  extractDocumentUrnFromUrl(url: string): string {
    const match = url.match(/\/document\/([^/?]+)/);
    return match ? decodeURIComponent(match[1]) : '';
  }

  // ── Input utilities ───────────────────────────────────────────────────────

  private async debounceInput(): Promise<void> {
    await this.page.locator('body').click();
    await this.page.waitForTimeout(WAITS.INPUT_DEBOUNCE);
  }

  // ── Document operations ───────────────────────────────────────────────────

  async waitForPageReady(): Promise<void> {
    const titleInput = this.page.locator(DOCUMENT_SELECTORS.titleInput);
    await expect(titleInput).toBeVisible({ timeout: TIMEOUTS.LONG });
    const editorSection = this.page.locator(DOCUMENT_SELECTORS.editorSection);
    await expect(editorSection).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async setDocumentTitle(title: string): Promise<void> {
    const titleInput = this.page.locator(DOCUMENT_SELECTORS.titleInput);
    await expect(titleInput).toBeVisible({ timeout: TIMEOUTS.NORMAL });
    await expect(titleInput).toBeEnabled();
    await titleInput.clear();
    await titleInput.pressSequentially(title);
    await this.debounceInput();
  }

  async expectTitleInput(title: string): Promise<void> {
    await expect(this.page.locator(DOCUMENT_SELECTORS.titleInput)).toHaveValue(title);
  }

  async navigateToDocument(urn: string): Promise<void> {
    await this.navigate(`/document/${encodeURIComponent(urn)}`);
    await this.page.waitForLoadState('networkidle');
  }
}
