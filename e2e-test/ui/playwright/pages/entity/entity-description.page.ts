/**
 * EntityDescriptionPage — editing an entity's documentation from the profile.
 *
 * A profile renders and edits its documentation on one of two surfaces, picked by entity type
 * and feature flags (see useEntityHasSummaryTab in datahub-web-react):
 *
 *   - 'summary':       the Summary tab's About section, edited in a modal
 *   - 'documentation': the Documentation tab, edited inline
 *
 * The sidebar Documentation pencil routes to whichever surface the profile has, so a caller states
 * the surface it expects and this page object asserts the pencil landed there.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BaseEntityPage } from './base-entity.page';
import type { DataHubLogger } from '../../utils/logger';
import { DELAYS, KEYS, TIMEOUTS } from '../../utils/constants';

export type DocumentationSurface = 'summary' | 'documentation';

export class EntityDescriptionPage extends BaseEntityPage {
  // Sidebar
  readonly sidebarEditDocumentationButton: Locator;
  readonly sidebarDocumentationContent: Locator;

  // The Summary modal and the Documentation tab mount their editor under the same test id; only
  // one is on the page at a time.
  readonly editorInput: Locator;

  // Summary tab About section and its edit modal
  readonly summaryAboutSection: Locator;
  readonly summaryEditDescriptionButton: Locator;
  readonly descriptionViewer: Locator;
  readonly publishButton: Locator;

  // Documentation tab inline editor
  readonly documentationEditButton: Locator;
  readonly addDocumentationButton: Locator;
  readonly descriptionSaveButton: Locator;
  readonly documentationTabContent: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.sidebarEditDocumentationButton = page.getByTestId('editDocumentation');
    this.sidebarDocumentationContent = page.getByTestId('sidebar-section-content-Documentation');

    this.editorInput = page.getByTestId('description-editor').getByRole('textbox');

    this.summaryAboutSection = page.getByTestId('about-section');
    this.summaryEditDescriptionButton = page.getByTestId('edit-description-button');
    this.descriptionViewer = page.getByTestId('description-viewer');
    this.publishButton = page.getByTestId('publish-button');

    this.documentationEditButton = page.getByTestId('edit-documentation-button');
    this.addDocumentationButton = page.getByTestId('add-documentation');
    this.descriptionSaveButton = page.getByTestId('description-editor-save-button');
    this.documentationTabContent = page.getByTestId('documentation-editor-content');
  }

  tabHeader(tabPath: string): Locator {
    return this.page.getByTestId(`${tabPath}-entity-tab-header`);
  }

  // ── Navigation ─────────────────────────────────────────────────────────────

  async openProfile(pathName: string, urn: string, name: string): Promise<void> {
    await this.navigate(`/${pathName}/${encodeURIComponent(urn)}/`);
    await expect(this.headerEntityName).toContainText(name, { timeout: TIMEOUTS.LONG });
    await expect(this.sidebarDocumentationContent).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  // ── Editing ────────────────────────────────────────────────────────────────

  /** Click the sidebar pencil and edit on the surface it is expected to open. */
  async editDescriptionFromSidebar(text: string, surface: DocumentationSurface): Promise<void> {
    this.logger?.step('edit description from sidebar', { surface });
    await this.sidebarEditDocumentationButton.click();
    if (surface === 'summary') {
      await expect(this.page).toHaveURL(/editingDescription=true/, { timeout: TIMEOUTS.LONG });
      await this.fillModalAndPublish(text);
      return;
    }
    await expect(this.page).toHaveURL(/editing=true/, { timeout: TIMEOUTS.LONG });
    await this.fillInlineEditorAndSave(text);
  }

  /** Open the tab that renders documentation and edit from its own edit control. */
  async editDescriptionFromTab(text: string, surface: DocumentationSurface): Promise<void> {
    this.logger?.step('edit description from tab', { surface });
    if (surface === 'summary') {
      await this.tabHeader('Summary').click();
      await expect(this.summaryAboutSection).toBeVisible({ timeout: TIMEOUTS.LONG });
      await this.summaryEditDescriptionButton.click();
      await this.fillModalAndPublish(text);
      return;
    }
    await this.tabHeader('Documentation').click();
    // The tab shows an edit button over existing text and an add button when empty.
    await this.documentationEditButton.or(this.addDocumentationButton).click();
    await expect(this.page).toHaveURL(/editing=true/, { timeout: TIMEOUTS.LONG });
    await this.fillInlineEditorAndSave(text);
  }

  private async fillModalAndPublish(text: string): Promise<void> {
    await expect(this.publishButton).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.editorInput).toBeVisible({ timeout: TIMEOUTS.LONG });
    await this.editorInput.clear();
    await this.editorInput.fill(text);
    await this.publishButton.click();
    await expect(this.publishButton).not.toBeAttached({ timeout: TIMEOUTS.LONG });
  }

  private async fillInlineEditorAndSave(text: string): Promise<void> {
    // The toolbar mounts first; the editable ProseMirror can lag while entity data loads.
    await expect(this.descriptionSaveButton).toBeAttached({ timeout: TIMEOUTS.EXTRA_LONG });
    await expect(this.editorInput).toBeAttached({ timeout: TIMEOUTS.EXTRA_LONG });
    // force: the editor can have zero height until the flex layout settles.
    await this.editorInput.click({ force: true });
    await this.page.keyboard.press(KEYS.CTRL_A);
    await this.editorInput.pressSequentially(text, { delay: DELAYS.SEQUENTIAL });
    await expect(this.descriptionSaveButton).toBeEnabled({ timeout: TIMEOUTS.EXTRA_LONG });
    await this.descriptionSaveButton.click();
    await this.toast.expectVisible('Description Updated', { timeout: TIMEOUTS.LONG });
  }

  // ── Assertions ─────────────────────────────────────────────────────────────

  async expectSidebarDescription(text: string): Promise<void> {
    await expect(this.sidebarDocumentationContent).toContainText(text, { timeout: TIMEOUTS.LONG });
  }

  /** The description as rendered on the surface it was edited on. */
  async expectSurfaceDescription(text: string, surface: DocumentationSurface): Promise<void> {
    const surfaceContent = surface === 'summary' ? this.descriptionViewer : this.documentationTabContent;
    await expect(surfaceContent).toContainText(text, { timeout: TIMEOUTS.LONG });
  }
}
