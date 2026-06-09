import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { TIMEOUTS, LOAD_STATES } from '../utils/constants';

export class SiblingsPage extends BasePage {
  readonly entityHeader: Locator;
  readonly tagTermInput: Locator;
  readonly tagTermModalTrigger: Locator;
  readonly addTagTermConfirmButton: Locator;
  readonly modalConfirmButton: Locator;

  private glossarySection: Locator;
  private addTermsButton: Locator;
  private searchResults: Locator;
  private mergedIndicator: Locator;
  private termAddedToast: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.entityHeader = page.getByTestId('entity-header-test-id');
    this.glossarySection = page.getByTestId('glossary-terms-section');
    this.addTermsButton = this.glossarySection.getByTestId('add-terms-button');
    this.tagTermInput = page.getByTestId('dropdown-search-input');
    this.tagTermModalTrigger = page.getByTestId('tag-term-modal-input');
    this.addTagTermConfirmButton = page.getByTestId('add-tag-term-from-modal-btn');
    this.modalConfirmButton = page.getByTestId('modal-confirm-button');
    this.searchResults = page.getByTestId('search-result-item');
    this.mergedIndicator = this.entityHeader.getByText('&');
    this.termAddedToast = page.getByText(/Added Terms/i);
  }

  // ── Private helper methods ────────────────────────────────────────────────

  private getPlatformLogo(platform: string): Locator {
    return this.entityHeader.getByTestId(`platform-icon-${platform.toLowerCase()}`);
  }

  private getSearchResultPlatformIcon(platform: string): Locator {
    return this.page.getByTestId(`platform-icon-${platform.toLowerCase()}`);
  }

  private getTermOption(termName: string): Locator {
    return this.page.getByTestId(`tag-term-option-${termName}`);
  }

  private getRemoveTermButton(termName: string): Locator {
    return this.page.getByTestId(`term-${termName}-pill`).getByTestId('remove-icon');
  }

  private getLineageNode(urn: string): Locator {
    return this.page.getByTestId(`lineage-node-${urn}`);
  }

  private getSiblingLink(urn: string): Locator {
    return this.page.getByTestId(`compact-entity-link-${urn}`);
  }

  // ── Navigation ────────────────────────────────────────────────────────────

  async navigateToDataset(urn: string): Promise<void> {
    await this.page.goto(`/dataset/${urn}/?is_lineage_mode=false`);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async navigateToSiblingByUrn(urn: string): Promise<void> {
    const siblingLink = this.getSiblingLink(urn);
    await siblingLink.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await siblingLink.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async navigateToSearchResults(query: string): Promise<void> {
    await this.page.goto(`/search?page=1&query=${query}`);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await expect(this.searchResults).not.toHaveCount(0, { timeout: TIMEOUTS.LONG });
  }

  async navigateToLineageGraph(entityType: string, urn: string): Promise<void> {
    await this.page.goto(`/${entityType}/${urn}/Lineage`);
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  // ── Verification methods ──────────────────────────────────────────────────

  async removeGlossaryTerm(termName: string): Promise<void> {
    await this.getRemoveTermButton(termName).click();
    await this.modalConfirmButton.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async verifyPlatformLogosVisible(platforms: string[]): Promise<void> {
    // Wait for entity header to be fully loaded with platform information
    await expect(this.entityHeader).toBeVisible({ timeout: TIMEOUTS.LONG });

    for (const platform of platforms) {
      const logo = this.getPlatformLogo(platform);
      await expect(logo).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    }
  }

  async verifyPlatformLogoNotVisible(platform: string): Promise<void> {
    const logo = this.getPlatformLogo(platform);
    await expect(logo).toBeHidden();
  }

  async verifyIndividualPlatformHeader(platform: string): Promise<void> {
    // Verify only one platform is shown (no merged indicator "&")
    await expect(this.mergedIndicator).toBeHidden();
    await expect(this.getPlatformLogo(platform)).toBeVisible();
  }

  async verifyGlossaryTermVisible(termName: string): Promise<void> {
    await expect(this.glossarySection.getByText(termName)).toBeVisible({ timeout: TIMEOUTS.LONG });
  }

  async verifySingleSearchResult(datasetName: string, platforms: string[]): Promise<void> {
    await expect(this.searchResults).toHaveCount(1);
    await expect(this.searchResults.getByText(datasetName)).toBeVisible();

    for (const platform of platforms) {
      await expect(this.getSearchResultPlatformIcon(platform)).toBeVisible();
    }
  }

  async verifyLineageNodesVisible(urns: string[]): Promise<void> {
    for (const urn of urns) {
      await expect(this.getLineageNode(urn)).toHaveCount(1);
    }
  }

  async verifyTermAddedToast(): Promise<void> {
    await expect(this.termAddedToast).toBeVisible();
  }

  // ── Interaction methods ───────────────────────────────────────────────────

  async addGlossaryTerm(termName: string): Promise<void> {
    await this.addTermsButton.click();
    await this.tagTermModalTrigger.click();
    await this.tagTermInput.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    await this.tagTermInput.click();
    await this.tagTermInput.fill(termName);
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);

    const termOption = this.getTermOption(termName);
    await termOption.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    await termOption.scrollIntoViewIfNeeded();
    await termOption.click({ force: true });

    await this.tagTermModalTrigger.click();
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);
    await this.addTagTermConfirmButton.click();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }
}
