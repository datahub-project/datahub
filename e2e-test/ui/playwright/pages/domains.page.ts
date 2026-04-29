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

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.newDomainButton = page.locator('[data-testid="domains-new-domain-button"]');
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
    await this.page.locator('[data-testid="create-domain-name"] input').fill(name);
    await this.page.getByText('Advanced').click();
    await this.page.locator('[data-testid="create-domain-id"] input').fill(String(id));
    await this.page.locator('[data-testid="create-domain-button"]').click();
    await expect(this.page.getByText(name).first()).toBeVisible({ timeout: 15000 });
    // Allow ES to index the new domain before subsequent tests search for it
    await this.page.waitForTimeout(5000);
  }

  async navigateToDomain(urn: string): Promise<void> {
    await this.page.goto(`/domain/${encodeURIComponent(urn)}`);
    await this.page.waitForLoadState('networkidle', { timeout: 30000 });
  }

  async clickDomain(name: string): Promise<void> {
    // On the flat-list layout, search to ensure the domain is visible before clicking
    const searchBox = this.page.locator('[placeholder="Search domains..."]');
    if (await searchBox.isVisible({ timeout: 2000 }).catch(() => false)) {
      await searchBox.fill(name);
      await this.page.waitForTimeout(500);
    }
    // Use link role to skip hidden aria-live spans that also match the text
    await this.page.getByRole('link', { name }).first().click();
    await this.page.waitForLoadState('networkidle');
  }

  async addEntitiesToDomain(searchQuery: string, entityUrn: string): Promise<void> {
    await this.page.locator('[data-testid="domain-batch-add"]').click();
    await this.page.locator('.ant-modal-content').locator('[data-testid="search-input"]').click();
    await this.page.locator('.ant-modal-content').locator('[data-testid="search-input"]').fill(searchQuery);
    // Wait for search results to render (entity-title is the visible result card heading)
    await expect(this.page.locator('.ant-modal-content [data-testid="entity-title"]').first()).toBeVisible({
      timeout: 30000,
    });
    await this.page.locator(`[data-testid="checkbox-${entityUrn}"]`).click({ force: true, timeout: 30000 });
    await this.page.locator('#continueButton').click();
    await expect(this.page.getByText('Added assets to Domain!')).toBeVisible({ timeout: 15000 });
  }

  async removeDomainFromDataset(datasetUrn: string, datasetName: string, _domainUrn: string): Promise<void> {
    await this.page.goto(`/dataset/${encodeURIComponent(datasetUrn)}/`);
    await expect(this.page.getByText(datasetName).first()).toBeVisible({ timeout: 30000 });
    // Click the close (remove) icon on the domain tag in the sidebar
    await this.page.locator('.sidebar-domain-section [data-testid="remove-icon"]').click();
    // Confirm in the ConfirmationModal (button has data-testid="modal-confirm-button" and text "Yes")
    await this.page.locator('[data-testid="modal-confirm-button"]').click();
    await this.page.waitForLoadState('networkidle');
  }

  async deleteDomain(domainUrn: string, _domainName: string): Promise<void> {
    // Navigate to the entity page and click the delete button directly — the V2 entity header
    // renders EntityMenuItems.DELETE as a standalone ActionMenuItem (DeleteEntityMenuAction),
    // not behind a dropdown, so no dropdown trigger is needed.
    await this.navigateToDomain(domainUrn);
    await this.page.locator('[data-testid="entity-menu-delete-button"]').click();
    await this.page.getByRole('button', { name: 'Yes' }).click();
    await this.page.waitForLoadState('networkidle', { timeout: 15000 });
    // Allow ES to de-index the deleted domain before asserting it's gone
    await this.page.waitForTimeout(5000);
  }

  async expectDomainVisible(name: string): Promise<void> {
    await expect(this.page.getByText(name).first()).toBeVisible({ timeout: 15000 });
  }

  async expectDomainNotVisible(name: string): Promise<void> {
    // Use entity-title testid to avoid matching hidden aria-live spans.
    // 15-minute timeout accommodates ES de-indexing lag in slow environments.
    await expect(this.page.locator('[data-testid="entity-title"]', { hasText: name })).not.toBeVisible({
      timeout: 15 * 60 * 1000,
    });
  }
}
