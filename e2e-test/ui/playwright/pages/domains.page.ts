/**
 * DomainsPage — page object for /domains.
 *
 * Covers domain CRUD and entity assignment workflows used by v2_domains.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class DomainsPage extends BasePage {
  readonly newDomainButton: Locator;
  // Create-domain modal
  readonly domainNameInput: Locator;
  readonly domainIdInput: Locator;
  readonly createDomainConfirmButton: Locator;
  // Domain list search
  readonly domainSearchInput: Locator;
  // Add-entities-to-domain modal
  readonly batchAddButton: Locator;
  readonly modalSearchInput: Locator;
  readonly continueButton: Locator;
  // Remove domain from dataset (sidebar)
  readonly sidebarRemoveIcon: Locator;
  readonly modalConfirmButton: Locator;
  // Domain entity page
  readonly entityMenuDeleteButton: Locator;
  readonly entityTitle: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.newDomainButton = page.locator('[data-testid="domains-new-domain-button"]');
    this.domainNameInput = page.locator('[data-testid="create-domain-name"] input');
    this.domainIdInput = page.locator('[data-testid="create-domain-id"] input');
    this.createDomainConfirmButton = page.locator('[data-testid="create-domain-button"]');
    this.domainSearchInput = page.locator('[placeholder="Search domains..."]');
    this.batchAddButton = page.locator('[data-testid="domain-batch-add"]');
    // AntD modal content scopes the search so it doesn't match the page-level search bar
    this.modalSearchInput = page.locator('.ant-modal-content').locator('[data-testid="search-input"]');
    this.continueButton = page.locator('#continueButton');
    this.sidebarRemoveIcon = page.locator('.sidebar-domain-section [data-testid="remove-icon"]');
    this.modalConfirmButton = page.locator('[data-testid="modal-confirm-button"]');
    this.entityMenuDeleteButton = page.locator('[data-testid="entity-menu-delete-button"]');
    this.entityTitle = page.locator('[data-testid="entity-title"]');
  }

  async navigate(): Promise<void> {
    await this.page.goto('/domains');
    await this.page.waitForLoadState('networkidle', { timeout: 30000 });
    // Both layout variants (nestedDomainsEnabled on/off) show "Domains" text on the page
    await expect(this.page.getByText('Domains').first()).toBeVisible({ timeout: 15000 });
  }

  async createDomain(name: string, id: string | number): Promise<void> {
    await this.newDomainButton.click();
    await expect(this.page.getByText('Create New Domain')).toBeVisible();
    await this.domainNameInput.fill(name);
    await this.page.getByText('Advanced').click();
    await this.domainIdInput.fill(String(id));
    await this.createDomainConfirmButton.click();
    await expect(this.page.getByText(name).first()).toBeVisible({ timeout: 15000 });
    // Allow ES to index the new domain before subsequent tests search for it
    await this.page.waitForTimeout(5000);
  }

  async navigateToDomain(urn: string): Promise<void> {
    await this.page.goto(`/domain/${encodeURIComponent(urn)}`);
    await this.page.waitForLoadState('networkidle', { timeout: 30000 });
  }

  async navigateToContainer(urn: string): Promise<void> {
    await this.page.goto(`/container/${encodeURIComponent(urn)}`);
    await this.page.waitForLoadState('networkidle', { timeout: 30000 });
  }

  async clickDomain(name: string): Promise<void> {
    // On the flat-list layout, search to ensure the domain is visible before clicking
    if (await this.domainSearchInput.isVisible({ timeout: 2000 }).catch(() => false)) {
      await this.domainSearchInput.fill(name);
      await this.page.waitForTimeout(500);
    }
    // Use link role to skip hidden aria-live spans that also match the text
    await this.page.getByRole('link', { name }).first().click();
    await this.page.waitForLoadState('networkidle');
  }

  async addEntitiesToDomain(searchQuery: string, entityUrn: string): Promise<void> {
    await this.batchAddButton.click();
    // entityUrn is dynamic — checkbox testid includes the full URN at runtime
    const checkbox = this.page.locator(`[data-testid="checkbox-${entityUrn}"]`);
    // The modal's SearchBar searches by entity display name. DataHub indexes BigQuery tables
    // by their table name (last dotted segment), not the fully-qualified path, so strip to
    // the last segment for a reliable match.
    const searchTerm = searchQuery.split('.').pop() || searchQuery;
    // Retry the search+select loop: ES may need time to index the entity after seeding.
    // Must use pressSequentially (not fill) because the SearchBar triggers its GraphQL query
    // via the AntD AutoComplete onSearch callback, which only fires on real keystroke events.
    // fill() only updates the DOM value and fires the inner input onChange, which does NOT
    // propagate to the AutoComplete's onSearch, so the search query never executes.
    await expect(async () => {
      await this.modalSearchInput.click({ clickCount: 3 }); // select-all any existing text
      await this.modalSearchInput.pressSequentially(searchTerm);
      // Wait for the debounce (300ms) + search result render before checking the checkbox
      await this.page.waitForTimeout(800);
      await expect(checkbox).toBeVisible({ timeout: 10000 });
    }).toPass({ timeout: 90000, intervals: [3000] });
    await checkbox.click({ force: true });
    await this.continueButton.click();
    await expect(this.page.getByText('Added assets to Domain!')).toBeVisible({ timeout: 15000 });
  }

  async removeDomainFromDataset(datasetUrn: string, datasetName: string, _domainUrn: string): Promise<void> {
    await this.page.goto(`/dataset/${encodeURIComponent(datasetUrn)}/`);
    await expect(this.page.getByText(datasetName).first()).toBeVisible({ timeout: 30000 });
    // Click the close (remove) icon on the domain tag in the sidebar
    await this.sidebarRemoveIcon.click();
    // Confirm in the ConfirmationModal
    await this.modalConfirmButton.click();
    await this.page.waitForLoadState('networkidle');
  }

  async deleteDomain(domainUrn: string, _domainName: string): Promise<void> {
    // Navigate to the entity page and click the delete button directly — the V2 entity header
    // renders EntityMenuItems.DELETE as a standalone ActionMenuItem (DeleteEntityMenuAction),
    // not behind a dropdown, so no dropdown trigger is needed.
    await this.navigateToDomain(domainUrn);
    await this.entityMenuDeleteButton.click();
    await this.page.getByRole('button', { name: 'Yes' }).click();
    await this.page.waitForLoadState('networkidle', { timeout: 15000 });
    // Allow ES to de-index the deleted domain before asserting it's gone
    await this.page.waitForTimeout(5000);
  }

  async expectDomainVisible(name: string): Promise<void> {
    await expect(this.page.getByText(name).first()).toBeVisible({ timeout: 15000 });
  }

  async expectDomainNotVisible(name: string): Promise<void> {
    // Filter entityTitle by the domain name to avoid matching hidden aria-live spans.
    // 15-minute timeout accommodates ES de-indexing lag in slow environments.
    await expect(this.entityTitle.filter({ hasText: name })).not.toBeVisible({
      timeout: 15 * 60 * 1000,
    });
  }
}
