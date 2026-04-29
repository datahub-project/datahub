/**
 * EntityDocumentationPage — page object for entity Documentation tab interactions.
 *
 * Covers documentation editing, link CRUD, and validation workflows used by
 * edit_documentation.js and v2_edit_documentation.js.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class EntityDocumentationPage extends BasePage {
  readonly editDocumentationButton: Locator;
  readonly saveDescriptionButton: Locator;
  readonly addRelatedButton: Locator;
  readonly relatedList: Locator;
  readonly platformLinksContainer: Locator;
  readonly urlInput: Locator;
  readonly labelInput: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.editDocumentationButton = page.locator('[data-testid="edit-documentation-button"]');
    this.saveDescriptionButton = page.locator('[data-testid="description-editor-save-button"]');
    this.addRelatedButton = page.locator('[data-testid="add-related-button"]');
    this.relatedList = page.locator('[data-testid="related-list"]');
    this.platformLinksContainer = page.locator('[data-testid="platform-links-container"]');
    this.urlInput = page.locator('[data-testid="url-input"]');
    this.labelInput = page.locator('[data-testid="label-input"]');
  }

  async navigateToDatasetDocumentationTab(datasetUrn: string, datasetName: string): Promise<void> {
    await this.page.goto(`/dataset/${encodeURIComponent(datasetUrn)}/`);
    await expect(this.page.getByText(datasetName).first()).toBeVisible({ timeout: 30000 });
    await this.openEntityTab('Documentation');
  }

  async openEntityTab(tabName: string): Promise<void> {
    // ant-tabs tab items have an id ending with the tab name
    await this.page.locator(`div[id$="${tabName}"]:nth-child(1)`).click();
    await this.page.waitForLoadState('networkidle');
  }

  // ── Documentation editing ────────────────────────────────────────────────

  async editDocumentation(text: string): Promise<void> {
    await this.editDocumentationButton.click();
    // Wait for URL to confirm we're in edit mode (?editing=true), then scope to
    // [data-testid="description-editor"] which only exists inside DescriptionEditor
    // (the static read-only view uses data-testid="documentation-editor-content").
    await this.page.waitForURL(/editing=true/, { timeout: 15000 });
    const editor = this.page.locator('[data-testid="description-editor"] .ProseMirror');
    await editor.waitFor({ state: 'attached', timeout: 15000 });
    // force: true bypasses the viewport/visibility check; Remirror auto-focuses on mount
    // but the element may have zero height until the flex layout resolves.
    await editor.click({ force: true });
    await this.page.keyboard.press('Control+a');
    await this.page.keyboard.type(text);
    await this.saveDescriptionButton.click();
    await expect(this.page.getByText('Description Updated')).toBeVisible({ timeout: 15000 });
  }

  async clearDocumentation(): Promise<void> {
    await this.editDocumentationButton.click();
    await this.page.waitForURL(/editing=true/, { timeout: 15000 });
    const editor = this.page.locator('[data-testid="description-editor"] .ProseMirror');
    await editor.waitFor({ state: 'attached', timeout: 15000 });
    await editor.click({ force: true });
    await this.page.keyboard.press('Control+a');
    await this.page.keyboard.press('Delete');
    await this.saveDescriptionButton.click();
    await expect(this.page.getByText('Description Updated')).toBeVisible({ timeout: 15000 });
  }

  // ── Field documentation (v1 schema tab) ─────────────────────────────────

  async navigateToDatasetSchemaTab(datasetUrn: string): Promise<void> {
    await this.page.goto(`/dataset/${encodeURIComponent(datasetUrn)}/Schema`);
    await this.page.waitForLoadState('networkidle');
    // Guard: ensure the entity page loaded and is not a "Not Found" error page.
    // The schema table is rendered only when the entity exists; waiting for any
    // row with an id matching the column- convention confirms a real schema tab.
    await expect(this.page.locator('[id^="column-"]').first()).toBeVisible({ timeout: 30000 });
  }

  async editFieldDescription(fieldName: string, description: string): Promise<void> {
    // Use the row's id attribute (set by SchemaTable's onRow handler) so the click
    // reliably hits the row element rather than matching arbitrary text on the page.
    await this.page.locator(`#column-${fieldName}`).click();
    await this.page.locator('[data-testid="edit-field-description"]').click();
    await expect(this.page.getByText('Update description')).toBeVisible();
    // Wait for the editor to be in the DOM — it may have zero height initially so use
    // 'attached' rather than 'visible', and force-click to focus even if not laid out yet.
    const editor = this.page.locator('[data-testid="description-editor"] .ProseMirror');
    await editor.waitFor({ state: 'attached', timeout: 15000 });
    await editor.click({ force: true });
    await this.page.keyboard.press('Control+a');
    await this.page.keyboard.press('Delete');
    await this.page.waitForTimeout(500);
    await this.page.keyboard.type(description);
    await this.page.locator('[data-testid="description-modal-update-button"]').click();
    await expect(this.page.getByText('Updated!')).toBeVisible({ timeout: 15000 });
  }

  // ── Link management ──────────────────────────────────────────────────────

  async openAddLinkForm(): Promise<void> {
    await this.addRelatedButton.click();
    await this.page.waitForTimeout(500);
    await this.page.getByText('Add link').click();
    await this.page.waitForTimeout(500);
  }

  async fillLinkForm(url: string, label: string, showInPreview: boolean): Promise<void> {
    await this.urlInput.clear();
    await this.urlInput.fill(url);
    await this.labelInput.clear();
    await this.labelInput.fill(label);

    const checkboxInput = this.page.locator('[data-testid="show-in-asset-preview-checkbox"]').locator('input');
    const isChecked = await checkboxInput.isChecked();
    if (isChecked !== showInPreview) {
      await this.page.locator('[data-testid="show-in-asset-preview-checkbox"]').click();
    }
  }

  async submitLinkForm(): Promise<void> {
    await this.page.locator('[data-testid="link-form-modal-submit-button"]').click();
  }

  async addLink(url: string, label: string, showInPreview: boolean): Promise<void> {
    await this.openAddLinkForm();
    await this.fillLinkForm(url, label, showInPreview);
    await this.submitLinkForm();
    await expect(this.page.getByText('Link Added')).toBeVisible({ timeout: 15000 });
  }

  async updateLink(
    currentUrl: string,
    currentLabel: string,
    newUrl: string,
    newLabel: string,
    showInPreview: boolean,
  ): Promise<void> {
    await this.relatedList
      .locator('li.ant-list-item')
      .filter({ has: this.page.locator(`a[href='${currentUrl}']`) })
      .filter({ hasText: currentLabel })
      .locator('[data-testid="edit-link-button"]')
      .click();
    await this.fillLinkForm(newUrl, newLabel, showInPreview);
    await this.submitLinkForm();
    await expect(this.page.getByText('Link Updated')).toBeVisible({ timeout: 15000 });
  }

  async removeLinkByUrl(url: string): Promise<void> {
    await this.relatedList
      .locator('li.ant-list-item')
      .filter({ has: this.page.locator(`a[href="${url}"]`) })
      .locator('[data-testid="remove-link-button"]')
      .click();
    await expect(this.page.getByText('Link Removed')).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText('Link Removed')).not.toBeVisible({ timeout: 15000 });
  }

  // ── Link verification helpers ────────────────────────────────────────────

  async expectLinkInDocumentationTab(url: string): Promise<void> {
    await expect(this.relatedList.locator(`[href='${url}']`)).toHaveCount(1, { timeout: 15000 });
  }

  async expectLinkNotInDocumentationTab(url: string): Promise<void> {
    await expect(this.relatedList.locator(`[href='${url}']`)).toHaveCount(0, { timeout: 10000 });
  }

  async expectLinkInEntityHeader(url: string): Promise<void> {
    // Link may be in an overflow dropdown (hidden) when many show-in-preview links exist.
    // Use .first() because the link can appear twice in the DOM (visible + aria-hidden copy)
    // and strict-mode toBeAttached would fail on 2+ matches without it.
    await expect(this.platformLinksContainer.locator(`a[href='${url}']`).first()).toBeAttached({ timeout: 15000 });
  }

  async expectLinkNotInEntityHeader(url: string): Promise<void> {
    await expect(this.platformLinksContainer.locator(`a[href='${url}']`)).toHaveCount(0, { timeout: 10000 });
  }

  async expectLinkInSidebar(url: string): Promise<void> {
    await expect(
      this.page.locator('[data-testid="sidebar-section-content-Documentation"]').locator(`[href='${url}']`),
    ).toBeVisible({ timeout: 15000 });
  }

  async expectLinkNotInSidebar(url: string): Promise<void> {
    await expect(
      this.page.locator('[data-testid="sidebar-section-content-Documentation"]').getByText(url),
    ).not.toBeVisible({ timeout: 10000 });
  }
}
