/**
 * EntityDocumentationPage — page object for entity Documentation tab interactions.
 *
 * Covers documentation editing, link CRUD, and validation workflows used by
 * edit_documentation.js and v2_edit_documentation.js.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { DELAYS, TIMEOUTS } from '../utils/constants';

export class EntityDocumentationPage extends BasePage {
  readonly editDocumentationButton: Locator;
  readonly saveDescriptionButton: Locator;
  readonly addRelatedButton: Locator;
  readonly relatedList: Locator;
  readonly platformLinksContainer: Locator;
  readonly urlInput: Locator;
  readonly labelInput: Locator;
  // The sole editable ProseMirror when the Documentation tab is in edit mode.
  // Read-only editors (source preview, sidebar) have contenteditable="false".
  readonly proseMirrorEditor: Locator;
  // Schema field description drawer
  readonly editFieldDescriptionButton: Locator;
  // Remirror textbox inside UpdateDescriptionModal (`data-testid="description-editor"`)
  readonly fieldDescriptionEditor: Locator;
  readonly fieldDescriptionUpdateButton: Locator;
  // Link form
  readonly showInPreviewCheckbox: Locator;
  readonly linkFormSubmitButton: Locator;
  // Sidebar documentation section
  readonly sidebarDocumentationSection: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.editDocumentationButton = page.getByTestId('edit-documentation-button');
    this.saveDescriptionButton = page.getByTestId('description-editor-save-button');
    this.addRelatedButton = page.getByTestId('add-related-button');
    this.relatedList = page.getByTestId('related-list');
    this.platformLinksContainer = page.getByTestId('platform-links-container');
    this.urlInput = page.getByTestId('url-input');
    this.labelInput = page.getByTestId('label-input');
    // eslint-disable-next-line playwright/no-raw-locators -- Remirror/ProseMirror CSS class + contenteditable attribute; no semantic equivalent
    this.proseMirrorEditor = page.locator('.remirror-editor.ProseMirror[contenteditable="true"]');
    this.editFieldDescriptionButton = page.getByTestId('edit-field-description');
    this.fieldDescriptionEditor = page.getByTestId('description-editor').getByRole('textbox');
    this.fieldDescriptionUpdateButton = page.getByTestId('description-modal-update-button');
    this.showInPreviewCheckbox = page.getByTestId('show-in-asset-preview-checkbox');
    this.linkFormSubmitButton = page.getByTestId('link-form-modal-submit-button');
    this.sidebarDocumentationSection = page.getByTestId('sidebar-section-content-Documentation');
  }

  async navigateToDatasetDocumentationTab(datasetUrn: string, datasetName: string): Promise<void> {
    await this.page.goto(`/dataset/${encodeURIComponent(datasetUrn)}/`);
    await expect(this.page.getByText(datasetName).first()).toBeVisible({ timeout: 30000 });
    await this.openEntityTab('Documentation');
  }

  async openEntityTab(tabName: string): Promise<void> {
    // ant-tabs tab items have an id ending with the tab name; no semantic selector for id-suffix matching
    // eslint-disable-next-line playwright/no-raw-locators -- AntD tab id suffix pattern; no getByRole equivalent with id constraint
    await this.page.locator(`div[id$="${tabName}"]:nth-child(1)`).click();
    await this.page.waitForLoadState('networkidle');
  }

  // ── Documentation editing ────────────────────────────────────────────────

  async editDocumentation(text: string): Promise<void> {
    await this.editDocumentationButton.click();
    // Wait for URL to confirm we're in edit mode (?editing=true).
    await this.page.waitForURL(/editing=true/, { timeout: 15000 });
    // The DescriptionEditor mounts the save/cancel toolbar immediately; wait for it
    // as a readiness signal before targeting the actual editor input.
    await this.saveDescriptionButton.waitFor({ state: 'attached', timeout: 30000 });
    // 30 s: DescriptionEditor renders null while entity data is loading, so the
    // editable ProseMirror can take longer than 15 s to mount in slow CI.
    await this.proseMirrorEditor.waitFor({ state: 'attached', timeout: 30000 });
    // force: true bypasses the viewport/visibility check; Remirror auto-focuses on mount
    // but the element may have zero height until the flex layout resolves.
    await this.proseMirrorEditor.click({ force: true });
    await this.page.keyboard.press('ControlOrMeta+a');
    await this.proseMirrorEditor.pressSequentially(text, { delay: DELAYS.SEQUENTIAL });
    // Wait for button to be enabled after change detection (same pattern as editFieldDescription)
    await expect(this.saveDescriptionButton).toBeEnabled({ timeout: TIMEOUTS.EXTRA_LONG });
    await this.saveDescriptionButton.click();
    await this.toast.expectVisible('Description Updated', { timeout: 15000 });
  }

  async clearDocumentation(): Promise<void> {
    await this.editDocumentationButton.click();
    await this.page.waitForURL(/editing=true/, { timeout: 15000 });
    await this.saveDescriptionButton.waitFor({ state: 'attached', timeout: 30000 });
    await this.proseMirrorEditor.waitFor({ state: 'attached', timeout: 30000 });
    await this.proseMirrorEditor.click({ force: true });
    await this.page.keyboard.press('ControlOrMeta+a');
    await this.page.keyboard.press('Delete');
    await this.saveDescriptionButton.click();
    await this.toast.expectVisible('Description Updated', { timeout: 15000 });
  }

  // ── Field documentation (v1 schema tab) ─────────────────────────────────

  async navigateToDatasetSchemaTab(datasetUrn: string): Promise<void> {
    await this.page.goto(`/dataset/${encodeURIComponent(datasetUrn)}/Schema`);
    await this.page.waitForLoadState('networkidle');
    // Guard: ensure the entity page loaded and is not a "Not Found" error page.
    // The schema table is rendered only when the entity exists; waiting for any
    // row with an id matching the column- convention confirms a real schema tab.
    // eslint-disable-next-line playwright/no-raw-locators -- dynamic id prefix [id^="column-"]; no getByTestId equivalent
    await expect(this.page.locator('[id^="column-"]').first()).toBeVisible({ timeout: 30000 });
  }

  async editFieldDescription(fieldName: string, description: string): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators -- React-generated CSS id (#column-{name}); no data-testid on schema rows
    const row = this.page.locator(`#column-${fieldName}`);
    const maxAttempts = 2;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        if (this.page.isClosed()) {
          throw new Error('Page was closed before editing field description');
        }

        // SchemaTable toggles the drawer open/closed on row click. Click only when the
        // edit affordance is not already visible, and recover from a toggle miss.
        const isDrawerOpen = await this.editFieldDescriptionButton.isVisible().catch(() => false);
        if (!isDrawerOpen) {
          await row.click();
          const becameVisible = await this.editFieldDescriptionButton
            .waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT })
            .then(() => true)
            .catch(() => false);
          if (!becameVisible) {
            await row.click();
          }
        }

        await this.editFieldDescriptionButton.waitFor({ state: 'visible', timeout: TIMEOUTS.EXTRA_LONG });
        await this.editFieldDescriptionButton.click();

        await expect(this.page.getByText('Update description')).toBeVisible({
          timeout: TIMEOUTS.EXTRA_LONG,
        });

        // Editor may have zero height on first paint — attach + force-click to focus.
        await this.fieldDescriptionEditor.waitFor({ state: 'attached', timeout: TIMEOUTS.EXTRA_LONG });
        await this.fieldDescriptionEditor.click({ force: true });
        await this.page.keyboard.press('ControlOrMeta+a');
        await this.page.keyboard.press('Delete');
        await this.fieldDescriptionEditor.pressSequentially(description, { delay: DELAYS.SEQUENTIAL });

        // Publish is disabled until Remirror onChange reports a value different from the
        // original description. Waiting for enabled avoids a 60s actionability timeout.
        await expect(this.fieldDescriptionUpdateButton).toBeEnabled({
          timeout: TIMEOUTS.EXTRA_LONG,
        });

        if (this.page.isClosed()) {
          throw new Error('Page was closed before clicking the description update button');
        }

        await this.fieldDescriptionUpdateButton.click();
        await this.toast.expectVisible('Updated!', { timeout: TIMEOUTS.EXTRA_LONG });
        return;
      } catch (err) {
        if (attempt === maxAttempts || this.page.isClosed()) {
          throw err;
        }
        // Dismiss a stuck modal before retrying the drawer → edit flow.
        await this.page.keyboard.press('Escape').catch(() => undefined);
        // eslint-disable-next-line playwright/no-wait-for-timeout
        await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS * attempt);
      }
    }
  }

  // ── Link management ──────────────────────────────────────────────────────

  async openAddLinkForm(): Promise<void> {
    await this.addRelatedButton.click();
    // eslint-disable-next-line playwright/no-wait-for-timeout
    await this.page.waitForTimeout(500);
    await this.page.getByText('Add link').click();
    // eslint-disable-next-line playwright/no-wait-for-timeout
    await this.page.waitForTimeout(500);
  }

  async fillLinkForm(url: string, label: string, showInPreview: boolean): Promise<void> {
    await this.urlInput.clear();
    await this.urlInput.fill(url);
    await this.labelInput.clear();
    await this.labelInput.fill(label);

    const checkboxInput = this.showInPreviewCheckbox.getByRole('checkbox');
    const isChecked = await checkboxInput.isChecked();
    if (isChecked !== showInPreview) {
      await this.showInPreviewCheckbox.click();
    }
  }

  async submitLinkForm(): Promise<void> {
    await this.linkFormSubmitButton.click();
  }

  async addLink(url: string, label: string, showInPreview: boolean): Promise<void> {
    await this.openAddLinkForm();
    await this.fillLinkForm(url, label, showInPreview);
    await this.submitLinkForm();
    // Confirm success by the link surfacing in the Resources list. We deliberately do
    // NOT assert the "Link Added" toast: toasts stack and linger (~3s), so when several
    // links are added in quick succession getByText('Link Added') resolves to multiple
    // nodes and fails Playwright strict mode.
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute selector; no semantic Playwright API for href filtering
    await expect(this.relatedList.locator(`a[href="${url}"]`)).toBeVisible({ timeout: 15000 });
  }

  async updateLink(
    currentUrl: string,
    currentLabel: string,
    newUrl: string,
    newLabel: string,
    showInPreview: boolean,
  ): Promise<void> {
    // Links render as ResourceLinkPill: the edit/remove affordances are buttons rendered
    // INSIDE the pill's <a href> (via the alchemy Pill rightIcons), so scope the edit
    // button under the anchor for this link.
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute selector; no semantic Playwright API for href filtering
    const pill = this.relatedList.locator(`a[href="${currentUrl}"]`).filter({ hasText: currentLabel });
    await pill.getByTestId('edit-link-button').click();
    await this.fillLinkForm(newUrl, newLabel, showInPreview);
    await this.submitLinkForm();
    // Confirm via the updated link surfacing in the list rather than the stacking toast.
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute selector; no semantic Playwright API for href filtering
    await expect(this.relatedList.locator(`a[href="${newUrl}"]`)).toBeVisible({ timeout: 15000 });
  }

  async removeLinkByUrl(url: string): Promise<void> {
    // Links render as ResourceLinkPill: the edit/remove affordances are buttons rendered
    // INSIDE the pill's <a href> (via the alchemy Pill rightIcons), so scope the remove
    // button under the anchor for this link.
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute selector; no semantic Playwright API for href filtering
    const pill = this.relatedList.locator(`a[href="${url}"]`);
    await pill.getByTestId('remove-link-button').click();
    // Confirm removal by the link disappearing from the list. We deliberately do NOT
    // assert the "Link Removed" toast: toasts stack and linger, so removing several links
    // in quick succession makes getByText('Link Removed') match multiple nodes (strict-
    // mode failure on both the visible and not-visible assertions).
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute selector; no semantic Playwright API for href filtering
    await expect(this.relatedList.locator(`a[href="${url}"]`)).toHaveCount(0, { timeout: 15000 });
  }

  // ── Link verification helpers ────────────────────────────────────────────

  async expectLinkInDocumentationTab(url: string): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute selector; no semantic Playwright API for href filtering
    await expect(this.relatedList.locator(`[href='${url}']`)).toHaveCount(1, { timeout: 15000 });
  }

  async expectLinkNotInDocumentationTab(url: string): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute selector; no semantic Playwright API for href filtering
    await expect(this.relatedList.locator(`[href='${url}']`)).toHaveCount(0, { timeout: 10000 });
  }

  async expectLinkInEntityHeader(url: string): Promise<void> {
    // Link may be in an overflow dropdown (hidden) when many show-in-preview links exist.
    // Use .first() because the link can appear twice in the DOM (visible + aria-hidden copy)
    // and strict-mode toBeAttached would fail on 2+ matches without it.
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute selector; no semantic Playwright API for href filtering
    await expect(this.platformLinksContainer.locator(`a[href='${url}']`).first()).toBeAttached({ timeout: 15000 });
  }

  async expectLinkNotInEntityHeader(url: string): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute selector; no semantic Playwright API for href filtering
    await expect(this.platformLinksContainer.locator(`a[href='${url}']`)).toHaveCount(0, { timeout: 10000 });
  }

  async expectLinkInSidebar(url: string): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators -- href attribute selector; no semantic Playwright API for href filtering
    await expect(this.sidebarDocumentationSection.locator(`[href='${url}']`)).toBeVisible({ timeout: 15000 });
  }

  async expectLinkNotInSidebar(url: string): Promise<void> {
    await expect(this.sidebarDocumentationSection.getByText(url)).not.toBeVisible({ timeout: 10000 });
  }
}
