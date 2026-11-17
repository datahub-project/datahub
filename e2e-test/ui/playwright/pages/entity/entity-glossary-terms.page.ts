/**
 * EntityGlossaryTermsPage — assigning and removing glossary terms from the profile sidebar.
 *
 * The sidebar Terms section, the Add Terms modal and the term pill are shared components, so one
 * page object serves every entity type that renders the section. Locators are test ids only.
 * The modal steps are exposed separately (open, search, confirm) so a spec can inspect the
 * modal, for example with the raw-identifier guard, while the options are on screen.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BaseEntityPage } from './base-entity.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';

export class EntityGlossaryTermsPage extends BaseEntityPage {
  /** The whole sidebar Terms section: header (with the add button) plus content. */
  readonly termsSection: Locator;
  readonly sidebarTermsContent: Locator;
  readonly addTermsButton: Locator;
  // Add Terms modal
  readonly termSelectTrigger: Locator;
  readonly termSearchInput: Locator;
  readonly addTermsConfirmButton: Locator;
  // Remove confirmation
  readonly removeConfirmButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.termsSection = page.getByTestId('glossary-terms-section');
    this.sidebarTermsContent = this.termsSection.getByTestId('sidebar-section-content-Terms');
    this.addTermsButton = this.termsSection.getByTestId('add-terms-button');
    this.termSelectTrigger = page.getByTestId('tag-term-modal-input');
    // The select renders its search input inside a dropdown popover, so the trigger must be
    // clicked before the input exists.
    this.termSearchInput = page.getByTestId('dropdown-search-input');
    this.addTermsConfirmButton = page.getByTestId('add-tag-term-from-modal-btn');
    this.removeConfirmButton = page.getByTestId('modal-confirm-button');
  }

  /** A term pill in the sidebar, keyed by the term's display name. */
  termPill(termName: string): Locator {
    return this.sidebarTermsContent.getByTestId(`term-${termName}`);
  }

  /** An option in the Add Terms dropdown, keyed by the term's display name. */
  termOption(termName: string): Locator {
    return this.page.getByTestId(`tag-term-option-${termName}`);
  }

  async openProfile(pathName: string, urn: string, name: string): Promise<void> {
    await this.navigate(`/${pathName}/${encodeURIComponent(urn)}/`);
    await expect(this.headerEntityName).toContainText(name, { timeout: TIMEOUTS.LONG });
    await expect(this.sidebarTermsContent).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  // ── Add Terms modal, step by step ──────────────────────────────────────────

  async openAddTermsModal(): Promise<void> {
    this.logger?.step('open add terms modal');
    await expect(this.addTermsButton).toBeEnabled({ timeout: TIMEOUTS.MEDIUM });
    await this.addTermsButton.click();
    await expect(this.termSelectTrigger).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /** Search the dropdown and wait for the option to render; nothing is selected yet. */
  async searchTerm(termName: string): Promise<void> {
    this.logger?.step('search term', { termName });
    await this.termSelectTrigger.click();
    await this.termSearchInput.fill(termName);
    await expect(this.termOption(termName)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async selectTermAndConfirm(termName: string): Promise<void> {
    this.logger?.step('select term and confirm', { termName });
    await this.termOption(termName).click();
    // Clicking the trigger again closes the dropdown popover so the footer is reachable.
    await this.termSelectTrigger.click();
    await expect(this.addTermsConfirmButton).toBeEnabled({ timeout: TIMEOUTS.MEDIUM });
    await this.addTermsConfirmButton.click();
    await this.toast.expectVisible('Added Terms!', { timeout: TIMEOUTS.LONG });
  }

  // ── Remove ─────────────────────────────────────────────────────────────────

  async removeTerm(termName: string): Promise<void> {
    this.logger?.step('remove term', { termName });
    await this.termPill(termName).getByTestId('remove-icon').click();
    await expect(this.removeConfirmButton).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await this.removeConfirmButton.click();
    await this.toast.expectVisible('Removed Term!', { timeout: TIMEOUTS.LONG });
  }

  // ── Assertions ─────────────────────────────────────────────────────────────

  async expectTermAssigned(termName: string): Promise<void> {
    await expect(this.termPill(termName)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectTermNotAssigned(termName: string): Promise<void> {
    // A freshly loaded profile has no pills at all until the sidebar mounts, and a hidden
    // assertion passes on zero elements, so wait for the section before asserting the pill away.
    await expect(this.sidebarTermsContent).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.termPill(termName)).toBeHidden({ timeout: TIMEOUTS.LONG });
  }
}
