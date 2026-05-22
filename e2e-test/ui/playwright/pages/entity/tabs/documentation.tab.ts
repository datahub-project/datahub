/**
 * DocumentationTab — manages the Documentation tab on entity pages.
 *
 * Extracted from EntityDocumentationPage. Tests that previously imported
 * EntityDocumentationPage should use `page.documentation` on DatasetPage instead.
 */

import { Page, Locator, expect } from '@playwright/test';
import type { Tab } from './tab.interface';

export class DocumentationTab implements Tab {
  private readonly page: Page;

  readonly editDocumentationButton: Locator;
  readonly saveDescriptionButton: Locator;
  readonly addRelatedButton: Locator;
  readonly relatedList: Locator;
  readonly platformLinksContainer: Locator;
  readonly urlInput: Locator;
  readonly labelInput: Locator;
  readonly proseMirrorEditor: Locator;
  readonly editFieldDescriptionButton: Locator;
  readonly fieldDescriptionEditor: Locator;
  readonly fieldDescriptionUpdateButton: Locator;
  readonly showInPreviewCheckbox: Locator;
  readonly linkFormSubmitButton: Locator;
  readonly sidebarDocumentationSection: Locator;

  constructor(page: Page) {
    this.page = page;
    this.editDocumentationButton = page.locator('[data-testid="edit-documentation-button"]');
    this.saveDescriptionButton = page.locator('[data-testid="description-editor-save-button"]');
    this.addRelatedButton = page.locator('[data-testid="add-related-button"]');
    this.relatedList = page.locator('[data-testid="related-list"]');
    this.platformLinksContainer = page.locator('[data-testid="platform-links-container"]');
    this.urlInput = page.locator('[data-testid="url-input"]');
    this.labelInput = page.locator('[data-testid="label-input"]');
    this.proseMirrorEditor = page.locator('.remirror-editor.ProseMirror[contenteditable="true"]');
    this.editFieldDescriptionButton = page.locator('[data-testid="edit-field-description"]');
    this.fieldDescriptionEditor = page.locator('[data-testid="description-editor"] .ProseMirror');
    this.fieldDescriptionUpdateButton = page.locator('[data-testid="description-modal-update-button"]');
    this.showInPreviewCheckbox = page.locator('[data-testid="show-in-asset-preview-checkbox"]');
    this.linkFormSubmitButton = page.locator('[data-testid="link-form-modal-submit-button"]');
    this.sidebarDocumentationSection = page.locator('[data-testid="sidebar-section-content-Documentation"]');
  }

  async open(): Promise<void> {
    await this.page.locator('[data-testid="Documentation-entity-tab-header"]').click();
    await this.page.waitForLoadState('networkidle');
  }

  async editDescription(text: string): Promise<void> {
    await this.editDocumentationButton.click();
    await this.page.waitForURL(/editing=true/, { timeout: 15000 });
    await this.saveDescriptionButton.waitFor({ state: 'attached', timeout: 30000 });
    await this.proseMirrorEditor.waitFor({ state: 'attached', timeout: 30000 });
    await this.proseMirrorEditor.click({ force: true });
    await this.page.keyboard.press('Control+a');
    await this.page.keyboard.type(text);
    await this.saveDescriptionButton.click();
    await expect(this.page.getByText('Description Updated')).toBeVisible({ timeout: 15000 });
  }

  async clearDescription(): Promise<void> {
    await this.editDocumentationButton.click();
    await this.page.waitForURL(/editing=true/, { timeout: 15000 });
    await this.saveDescriptionButton.waitFor({ state: 'attached', timeout: 30000 });
    await this.proseMirrorEditor.waitFor({ state: 'attached', timeout: 30000 });
    await this.proseMirrorEditor.click({ force: true });
    await this.page.keyboard.press('Control+a');
    await this.page.keyboard.press('Delete');
    await this.saveDescriptionButton.click();
    await expect(this.page.getByText('Description Updated')).toBeVisible({ timeout: 15000 });
  }

  async addLink(url: string, label: string, showInPreview: boolean): Promise<void> {
    await this.addRelatedButton.click();
    await this.page.waitForTimeout(500);
    await this.page.getByText('Add link').click();
    await this.page.waitForTimeout(500);
    await this.urlInput.clear();
    await this.urlInput.fill(url);
    await this.labelInput.clear();
    await this.labelInput.fill(label);
    const checkboxInput = this.showInPreviewCheckbox.locator('input');
    const isChecked = await checkboxInput.isChecked();
    if (isChecked !== showInPreview) {
      await this.showInPreviewCheckbox.click();
    }
    await this.linkFormSubmitButton.click();
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
    await this.urlInput.clear();
    await this.urlInput.fill(newUrl);
    await this.labelInput.clear();
    await this.labelInput.fill(newLabel);
    const checkboxInput = this.showInPreviewCheckbox.locator('input');
    const isChecked = await checkboxInput.isChecked();
    if (isChecked !== showInPreview) {
      await this.showInPreviewCheckbox.click();
    }
    await this.linkFormSubmitButton.click();
    await expect(this.page.getByText('Link Updated')).toBeVisible({ timeout: 15000 });
  }

  async openAddLinkForm(): Promise<void> {
    await this.addRelatedButton.click();
    await this.page.waitForTimeout(500);
    await this.page.getByText('Add link').click();
    await this.page.waitForTimeout(500);
  }

  async removeLink(url: string): Promise<void> {
    await this.relatedList
      .locator('li.ant-list-item')
      .filter({ has: this.page.locator(`a[href="${url}"]`) })
      .locator('[data-testid="remove-link-button"]')
      .click();
    await expect(this.page.getByText('Link Removed')).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText('Link Removed')).not.toBeVisible({ timeout: 15000 });
  }

  async expectLinkInTab(url: string): Promise<void> {
    await expect(this.relatedList.locator(`[href='${url}']`)).toHaveCount(1, { timeout: 15000 });
  }

  async expectLinkNotInTab(url: string): Promise<void> {
    await expect(this.relatedList.locator(`[href='${url}']`)).toHaveCount(0, { timeout: 10000 });
  }

  async expectLinkInHeader(url: string): Promise<void> {
    await expect(this.platformLinksContainer.locator(`a[href='${url}']`).first()).toBeAttached({ timeout: 15000 });
  }

  async expectLinkNotInHeader(url: string): Promise<void> {
    await expect(this.platformLinksContainer.locator(`a[href='${url}']`)).toHaveCount(0, { timeout: 10000 });
  }

  async expectLinkInSidebar(url: string): Promise<void> {
    await expect(this.sidebarDocumentationSection.locator(`[href='${url}']`)).toBeVisible({ timeout: 15000 });
  }

  async expectLinkNotInSidebar(url: string): Promise<void> {
    await expect(this.sidebarDocumentationSection.getByText(url)).not.toBeVisible({ timeout: 10000 });
  }

  /** Remove a link by its URL — alias matching EntityDocumentationPage.removeLinkByUrl(). */
  async removeLinkByUrl(url: string): Promise<void> {
    await this.removeLink(url);
  }

  /** Edit the main entity documentation — alias for editDescription(). */
  async editDocumentation(text: string): Promise<void> {
    await this.editDescription(text);
  }

  /** Clear the main entity documentation — alias for clearDescription(). */
  async clearDocumentation(): Promise<void> {
    await this.clearDescription();
  }

  async expectLinkInDocumentationTab(url: string): Promise<void> {
    await this.expectLinkInTab(url);
  }

  async expectLinkNotInDocumentationTab(url: string): Promise<void> {
    await this.expectLinkNotInTab(url);
  }

  async expectLinkInEntityHeader(url: string): Promise<void> {
    await this.expectLinkInHeader(url);
  }

  async expectLinkNotInEntityHeader(url: string): Promise<void> {
    await this.expectLinkNotInHeader(url);
  }

  async navigateToDatasetDocumentationTab(datasetUrn: string, datasetName: string): Promise<void> {
    await this.page.goto(`/dataset/${encodeURIComponent(datasetUrn)}/`);
    const { expect } = await import('@playwright/test');
    await expect(this.page.getByText(datasetName).first()).toBeVisible({ timeout: 30000 });
    await this.open();
  }

  async navigateToDatasetSchemaTab(datasetUrn: string): Promise<void> {
    await this.page.goto(`/dataset/${encodeURIComponent(datasetUrn)}/Schema`);
    await this.page.waitForLoadState('networkidle');
    const { expect } = await import('@playwright/test');
    await expect(this.page.locator('[id^="column-"]').first()).toBeVisible({ timeout: 30000 });
  }

  /** Edit a schema field's description via the schema field drawer. */
  async editFieldDescription(fieldName: string, description: string): Promise<void> {
    const row = this.page.locator(`#column-${fieldName}`);
    const isDrawerOpen = await this.editFieldDescriptionButton.isVisible();
    if (!isDrawerOpen) {
      await row.click();
      const becameVisible = await this.editFieldDescriptionButton
        .waitFor({ state: 'visible', timeout: 2000 })
        .then(() => true)
        .catch(() => false);
      if (!becameVisible) {
        await row.click();
      }
    }
    await this.editFieldDescriptionButton.waitFor({ state: 'visible', timeout: 15000 });
    await this.editFieldDescriptionButton.click();
    await expect(this.page.getByText('Update description')).toBeVisible({ timeout: 15000 });
    await this.fieldDescriptionEditor.waitFor({ state: 'attached', timeout: 15000 });
    await this.fieldDescriptionEditor.click({ force: true });
    await this.page.keyboard.press('Control+a');
    await this.page.keyboard.press('Delete');
    await this.page.keyboard.type(description);
    await this.fieldDescriptionUpdateButton.click();
    await expect(this.page.getByText('Updated!')).toBeVisible({ timeout: 15000 });
  }
}
