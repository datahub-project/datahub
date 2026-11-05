/**
 * EntityTagsPage — assigning and removing tags from the profile sidebar.
 *
 * The sidebar Tags section, the Add Tags modal and the tag pill are shared components, so one
 * page object serves every entity type that renders the section. Locators are test ids only.
 * The modal steps are exposed separately (open, search, confirm) so a spec can inspect the
 * modal, for example with the raw-identifier guard, while the options are on screen.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BaseEntityPage } from './base-entity.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';

export class EntityTagsPage extends BaseEntityPage {
  /** The whole sidebar Tags section: header (with the add button) plus content. */
  readonly tagsSection: Locator;
  readonly sidebarTagsContent: Locator;
  readonly addTagsButton: Locator;
  // Add Tags modal
  readonly tagSelectTrigger: Locator;
  readonly tagSearchInput: Locator;
  readonly addTagsConfirmButton: Locator;
  // Remove confirmation
  readonly removeConfirmButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    // eslint-disable-next-line playwright/no-raw-locators -- HTML id set by SidebarTagsSection (ENTITY_PROFILE_TAGS_ID); the add button sits in the section header, outside the content test id
    this.tagsSection = page.locator('#entity-profile-tags');
    this.sidebarTagsContent = this.tagsSection.getByTestId('sidebar-section-content-Tags');
    this.addTagsButton = this.tagsSection.getByTestId('add-tags-button');
    this.tagSelectTrigger = page.getByTestId('tag-term-modal-input');
    // The select renders its search input inside a dropdown popover, so the trigger must be
    // clicked before the input exists.
    this.tagSearchInput = page.getByTestId('dropdown-search-input');
    this.addTagsConfirmButton = page.getByTestId('add-tag-term-from-modal-btn');
    this.removeConfirmButton = page.getByTestId('modal-confirm-button');
  }

  /** A tag pill in the sidebar, keyed by the tag's display name. */
  tagPill(tagName: string): Locator {
    return this.sidebarTagsContent.getByTestId(`tag-${tagName}`);
  }

  /** An option in the Add Tags dropdown, keyed by the tag's display name. */
  tagOption(tagName: string): Locator {
    return this.page.getByTestId(`tag-term-option-${tagName}`);
  }

  async openProfile(pathName: string, urn: string, name: string): Promise<void> {
    await this.navigate(`/${pathName}/${encodeURIComponent(urn)}/`);
    await expect(this.headerEntityName).toContainText(name, { timeout: TIMEOUTS.LONG });
    await expect(this.sidebarTagsContent).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  // ── Add Tags modal, step by step ───────────────────────────────────────────

  async openAddTagsModal(): Promise<void> {
    this.logger?.step('open add tags modal');
    await expect(this.addTagsButton).toBeEnabled({ timeout: TIMEOUTS.MEDIUM });
    await this.addTagsButton.click();
    await expect(this.tagSelectTrigger).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /** Search the dropdown and wait for the option to render; nothing is selected yet. */
  async searchTag(tagName: string): Promise<void> {
    this.logger?.step('search tag', { tagName });
    await this.tagSelectTrigger.click();
    await this.tagSearchInput.fill(tagName);
    await expect(this.tagOption(tagName)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async selectTagAndConfirm(tagName: string): Promise<void> {
    this.logger?.step('select tag and confirm', { tagName });
    await this.tagOption(tagName).click();
    // The open dropdown popover overlaps the footer and intercepts a pointer click on the
    // confirm button, so dispatch the click directly; the selection is already in state.
    await expect(this.addTagsConfirmButton).toBeEnabled({ timeout: TIMEOUTS.MEDIUM });
    await this.addTagsConfirmButton.dispatchEvent('click');
    await this.toast.expectVisible('Added Tags!', { timeout: TIMEOUTS.LONG });
  }

  // ── Remove ─────────────────────────────────────────────────────────────────

  async removeTag(tagName: string): Promise<void> {
    this.logger?.step('remove tag', { tagName });
    await this.tagPill(tagName).getByTestId('remove-icon').click();
    await expect(this.removeConfirmButton).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await this.removeConfirmButton.click();
    await this.toast.expectVisible('Removed Tag!', { timeout: TIMEOUTS.LONG });
  }

  // ── Assertions ─────────────────────────────────────────────────────────────

  async expectTagAssigned(tagName: string): Promise<void> {
    await expect(this.tagPill(tagName)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectTagNotAssigned(tagName: string): Promise<void> {
    // A freshly loaded profile has no pills at all until the sidebar mounts, and a hidden
    // assertion passes on zero elements, so wait for the section before asserting the pill away.
    await expect(this.sidebarTagsContent).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.tagPill(tagName)).toBeHidden({ timeout: TIMEOUTS.LONG });
  }
}
