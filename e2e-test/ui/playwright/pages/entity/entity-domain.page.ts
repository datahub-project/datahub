/**
 * EntityDomainPage — setting and removing the domain from the profile sidebar.
 *
 * The sidebar Domain section, the Set Domain modal and the domain pill are shared components,
 * so one page object serves every entity type that renders the section. Locators are test ids
 * only, apart from the section wrapper (HTML id) and the select's search results (no test id in
 * the app). The modal steps are exposed separately (open, search, save) so a spec can inspect the
 * modal, for example with the raw-identifier guard, while the results are on screen.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BaseEntityPage } from './base-entity.page';
import type { DataHubLogger } from '../../utils/logger';
import { TIMEOUTS } from '../../utils/constants';

export class EntityDomainPage extends BaseEntityPage {
  /** The whole sidebar Domain section: header (with the set button) plus content. */
  readonly domainSection: Locator;
  readonly sidebarDomainContent: Locator;
  readonly setDomainButton: Locator;
  // Set Domain modal
  readonly setDomainModal: Locator;
  readonly domainSelect: Locator;
  readonly saveDomainButton: Locator;
  // Remove confirmation
  readonly removeConfirmButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    // eslint-disable-next-line playwright/no-raw-locators -- HTML id set by SidebarDomainSection (ENTITY_PROFILE_DOMAINS_ID); the set button sits in the section header, outside the content test id
    this.domainSection = page.locator('#entity-profile-domains');
    this.sidebarDomainContent = this.domainSection.getByTestId('sidebar-section-content-Domain');
    this.setDomainButton = this.domainSection.getByTestId('set-domain-button');
    // The modal has no stable test id of its own, so scope by the dialog that holds the domain select.
    this.setDomainModal = page.getByRole('dialog').filter({ has: page.getByTestId('set-domain-select') });
    this.domainSelect = page.getByTestId('set-domain-select');
    this.saveDomainButton = this.setDomainModal.getByTestId('submit-button');
    this.removeConfirmButton = page.getByTestId('modal-confirm-button');
  }

  /** The domain pill in the sidebar, keyed by the domain's display name. */
  domainPill(domainName: string): Locator {
    return this.sidebarDomainContent.getByTestId(`domain-${domainName}`);
  }

  /**
   * A search result in the Set Domain select's dropdown, by display name. The results carry no
   * test id (the `domain-option-*` ids belong to the collapsed domain navigator inside the same
   * modal, which is rendered at zero height while a search is typed), so this is the one locator
   * in the matrix page objects that depends on the select's own markup.
   */
  domainSearchResult(domainName: string): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- antd Select search results have no test id, and the only role=option elements are the select's zero-size screen-reader mirror whose text is the option value (a URN); the domain-option-* test ids are on the hidden navigator, not the results
    return this.page
      .locator('.ant-select-dropdown .ant-select-item-option')
      .filter({ has: this.page.getByText(domainName, { exact: true }) });
  }

  async openProfile(pathName: string, urn: string, name: string): Promise<void> {
    await this.navigate(`/${pathName}/${encodeURIComponent(urn)}/`);
    await expect(this.headerEntityName).toContainText(name, { timeout: TIMEOUTS.LONG });
    await expect(this.sidebarDomainContent).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  // ── Set Domain modal, step by step ─────────────────────────────────────────

  async openSetDomainModal(): Promise<void> {
    this.logger?.step('open set domain modal');
    await expect(this.setDomainButton).toBeEnabled({ timeout: TIMEOUTS.MEDIUM });
    await this.setDomainButton.click();
    await expect(this.domainSelect).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  /** Search the select and wait for the option to render; nothing is selected yet. */
  async searchDomain(domainName: string): Promise<void> {
    this.logger?.step('search domain', { domainName });
    await this.domainSelect.click();
    await this.domainSelect.pressSequentially(domainName);
    await expect(this.domainSearchResult(domainName)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async selectDomainAndSave(domainName: string): Promise<void> {
    this.logger?.step('select domain and save', { domainName });
    await this.selectDomainOption(domainName);
    await expect(this.saveDomainButton).toBeEnabled({ timeout: TIMEOUTS.MEDIUM });
    // A pick clears the search text, which re-shows the domain navigator over the modal footer, so
    // a pointer click on Save is intercepted; the selection is already in state, so dispatch the
    // click directly.
    await this.saveDomainButton.dispatchEvent('click');
    await this.toast.expectVisible('Updated Domain!', { timeout: TIMEOUTS.LONG });
  }

  /**
   * Pick the search result and confirm the pick took. The open select's search input intercepts
   * pointer events over the portal-rendered result, so the click is dispatched directly; the pick
   * is confirmed by the chosen name showing in the select. Retried a couple of times because the
   * result list re-renders as search results stream in.
   */
  private async selectDomainOption(domainName: string): Promise<void> {
    const result = this.domainSearchResult(domainName);
    for (let attempt = 1; attempt <= 3; attempt++) {
      await result.dispatchEvent('click');
      try {
        await expect(this.domainSelect).toContainText(domainName, { timeout: TIMEOUTS.SHORT });
        return;
      } catch (error) {
        if (attempt === 3) throw error;
        this.logger?.warn('domain search result click did not select; retrying', { domainName, attempt });
      }
    }
  }

  // ── Remove ─────────────────────────────────────────────────────────────────

  async removeDomain(domainName: string): Promise<void> {
    this.logger?.step('remove domain', { domainName });
    await this.domainPill(domainName).getByTestId('remove-icon').click();
    await expect(this.removeConfirmButton).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await this.removeConfirmButton.click();
    await this.toast.expectVisible('Removed Domain.', { timeout: TIMEOUTS.LONG });
  }

  // ── Assertions ─────────────────────────────────────────────────────────────

  async expectDomainSet(domainName: string): Promise<void> {
    await expect(this.domainPill(domainName)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async expectDomainNotSet(domainName: string): Promise<void> {
    // A freshly loaded profile has no pill at all until the sidebar mounts, and a hidden
    // assertion passes on zero elements, so wait for the section before asserting the pill away.
    await expect(this.sidebarDomainContent).toBeVisible({ timeout: TIMEOUTS.LONG });
    await expect(this.domainPill(domainName)).toBeHidden({ timeout: TIMEOUTS.LONG });
  }
}
