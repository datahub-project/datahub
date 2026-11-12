/**
 * DomainsPage — page object for /domains.
 *
 * Covers domain CRUD and entity assignment workflows used by v2_domains.
 */

import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { GraphQLHelper } from '../../helpers/graphql-helper';
import { TIMEOUTS } from '../../utils/constants';

// How long a deleted domain can stay on an entity while its references are removed in the background.
const DOMAIN_REFERENCE_CLEANUP_TIMEOUT = 60000;

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

  private readonly graphql: GraphQLHelper;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.newDomainButton = page.getByTestId('domains-new-domain-button');
    this.domainNameInput = page.getByTestId('create-domain-name').getByRole('textbox');
    this.domainIdInput = page.getByTestId('create-domain-id').getByRole('textbox');
    this.createDomainConfirmButton = page.getByTestId('create-domain-button');
    this.domainSearchInput = page.getByPlaceholder('Search domains...');
    this.batchAddButton = page.getByTestId('domain-batch-add');
    // AntD modal content scopes the search so it doesn't match the page-level search bar
    this.modalSearchInput = page.getByRole('dialog').getByTestId('search-input');
    this.continueButton = page.getByTestId('search-select-modal-continue-button');
    // eslint-disable-next-line playwright/no-raw-locators -- sidebar-domain-section has no data-testid; compound CSS selector required
    this.sidebarRemoveIcon = page.locator('.sidebar-domain-section [data-testid="remove-icon"]');
    this.modalConfirmButton = page.getByTestId('modal-confirm-button');
    this.entityMenuDeleteButton = page.getByTestId('entity-menu-delete-button');
    this.entityTitle = page.getByTestId('entity-title');
    this.graphql = new GraphQLHelper(page);
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
    // Later tests open the domain by URN, which reads the primary store, so the create only has
    // to have succeeded; they do not depend on the domain being in the search index.
    const created = this.graphql.waitForGraphQLResponse('createDomain');
    await this.createDomainConfirmButton.click();
    expect((await created).errors).toBeUndefined();
    await expect(this.page.getByText(name).first()).toBeVisible({ timeout: 15000 });
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
    }
    // Use link role to skip hidden aria-live spans that also match the text
    await this.page.getByRole('link', { name }).first().click();
    await this.page.waitForLoadState('networkidle');
  }

  async addEntitiesToDomain(searchQuery: string, entityUrn: string): Promise<void> {
    await this.batchAddButton.click();
    // entityUrn is dynamic — checkbox testid includes the full URN at runtime
    const checkbox = this.page.getByTestId(`checkbox-${entityUrn}`);
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
    const deleted = this.graphql.waitForGraphQLResponse('deleteDomain', (variables) => variables.urn === domainUrn);
    await this.page.getByRole('button', { name: 'Yes' }).click();
    expect(await deleted).toMatchObject({ data: { deleteDomain: true } });
    await this.page.waitForLoadState('networkidle', { timeout: 15000 });
  }

  async expectDomainVisible(name: string): Promise<void> {
    await expect(this.page.getByText(name).first()).toBeVisible({ timeout: 15000 });
  }

  /**
   * Deleting a domain removes its references from entities asynchronously, after the delete
   * mutation returns, so reload the entity page until the domain is gone. `loadedText` must be
   * visible once the page has loaded, so the absence check cannot pass on an unrendered page.
   */
  async expectDomainRemovedFromEntity(domainName: string, loadedText: string): Promise<void> {
    await expect(async () => {
      await this.page.reload();
      await expect(this.page.getByText(loadedText).first()).toBeVisible({ timeout: TIMEOUTS.LONG });
      await expect(this.page.getByText(domainName)).toBeHidden();
    }).toPass({ timeout: DOMAIN_REFERENCE_CLEANUP_TIMEOUT });
  }

  async expectDomainNotVisible(name: string): Promise<void> {
    // Filter entityTitle by the domain name to avoid matching hidden aria-live spans.
    // 15-minute timeout accommodates ES de-indexing lag in slow environments.
    await expect(this.entityTitle.filter({ hasText: name })).not.toBeVisible({
      timeout: 15 * 60 * 1000,
    });
  }
}
