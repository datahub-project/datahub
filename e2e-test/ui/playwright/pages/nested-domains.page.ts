/**
 * NestedDomainsPage — page object for nested domains operations
 * Covers domain creation, hierarchy manipulation, documentation, ownership, and asset management
 */

import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

const SHORT_TIMEOUT = 5000;
const MEDIUM_TIMEOUT = SHORT_TIMEOUT * 3;
const LONG_TIMEOUT = SHORT_TIMEOUT * 6;

const CYPRESS_BAZ2_CHART_URN = 'urn:li:chart:(looker,cypress_baz2)';

export class NestedDomainsPage extends BasePage {
  readonly browseV2Container: Locator;
  readonly createDomainButton: Locator;
  readonly createDomainModal: Locator;
  readonly createDomainNameInput: Locator;
  readonly createDomainConfirmButton: Locator;
  readonly parentDomainSelector: Locator;
  readonly parentDomainCloseButton: Locator;
  readonly entityMenuMoveButton: Locator;
  readonly moveDomainModal: Locator;
  readonly moveDomainConfirmButton: Locator;
  readonly entityMenuDeleteButton: Locator;
  readonly deleteConfirmButton: Locator;
  readonly documentationTab: Locator;
  readonly addDocumentationButton: Locator;
  readonly editDocumentationButton: Locator;
  readonly documentationEditor: Locator;
  readonly saveDocumentationButton: Locator;
  readonly addLinkButton: Locator;
  readonly linkUrlInput: Locator;
  readonly linkLabelInput: Locator;
  readonly linkSubmitButton: Locator;
  readonly addOwnersButton: Locator;
  readonly ownersSearchInput: Locator;
  readonly addOwnerConfirmButton: Locator;
  readonly openDomainItem: Locator;
  readonly expandDomainButton: Locator;
  readonly assetsTab: Locator;
  readonly searchResultsEditButton: Locator;
  readonly toggleSidebarButton: Locator;
  readonly tagTermModalInput: Locator;
  readonly tagTermOption: Locator;
  readonly addTagButton: Locator;
  readonly pageTitle: Locator;
  readonly domainLinks: Locator;
  readonly editDocumentationButtonAlt: Locator;
  readonly documentationEditorAlt: Locator;
  readonly editIconButton: Locator;
  readonly editFieldTypography: Locator;
  readonly removeButtonsCircle: Locator;
  readonly yesConfirmButton: Locator;
  readonly summaryTab: Locator;
  readonly propertiesSection: Locator;
  readonly aboutSection: Locator;
  readonly templateSection: Locator;
  readonly assetsHeading: Locator;
  readonly domainsHeading: Locator;
  readonly dataProductsHeading: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.browseV2Container = page.locator('[id="browse-v2"]');
    this.createDomainButton = page.locator('[id="browse-v2"]').locator('button').nth(0);
    this.createDomainModal = page.getByText('Create New Domain');
    this.createDomainNameInput = page.locator('[data-testid="create-domain-name"]');
    this.createDomainConfirmButton = page.locator('[data-testid="create-domain-button"]');
    this.parentDomainSelector = page.locator('.ant-select-selection-item');
    this.parentDomainCloseButton = page.locator('[aria-label="close-circle"]');
    this.entityMenuMoveButton = page.locator('[data-testid="entity-menu-move-button"]');
    this.moveDomainModal = page.locator('[data-testid="move-domain-modal"]');
    this.moveDomainConfirmButton = page.locator('[data-testid="move-domain-modal-move-button"]');
    this.entityMenuDeleteButton = page.locator('[data-testid="entity-menu-delete-button"]');
    this.deleteConfirmButton = page.getByRole('button', { name: 'Yes' });
    this.documentationTab = page.locator('#rc-tabs-0-tab-Documentation');
    this.addDocumentationButton = page.locator('[data-testid="add-documentation"]');
    this.editDocumentationButton = page.locator('button:has-text("Edit")').nth(0);
    this.documentationEditor = page.locator('[role="textbox"]').nth(0);
    this.saveDocumentationButton = page.locator('[data-testid="description-editor-save-button"]');
    this.addLinkButton = page.locator('[data-testid="add-link-button"]').nth(0);
    this.linkUrlInput = page.locator('[data-testid="link-form-modal-url"]');
    this.linkLabelInput = page.locator('[data-testid="link-form-modal-label"]');
    this.linkSubmitButton = page.locator('[data-testid="link-form-modal-submit-button"]');
    this.addOwnersButton = page.locator('[data-testid="add-owners-button"]');
    this.ownersSearchInput = page.locator('[data-testid="edit-owners-modal-find-actors-input"]');
    this.addOwnerConfirmButton = page.locator('#addOwnerButton');
    this.openDomainItem = page.locator('[data-testid="open-domain-item"]');
    this.expandDomainButton = page.locator('[aria-label="right"]');
    this.assetsTab = page.locator('[data-node-key="Assets"]');
    this.searchResultsEditButton = page.locator('[data-testid="search-results-edit-button"]');
    this.toggleSidebarButton = page.locator('[data-testid="toggleSidebar"]');
    this.tagTermModalInput = page.locator('[data-testid="tag-term-modal-input"]');
    this.tagTermOption = page.locator('[data-testid="tag-term-option"]');
    this.addTagButton = page.locator('[data-testid="add-tag-term-from-modal-btn"]');
    this.pageTitle = page.getByTestId('page-title');
    this.domainLinks = page.locator('a[href*="/domain/"]');
    this.editDocumentationButtonAlt = page.locator('[data-testid="editDocumentation"]');
    this.documentationEditorAlt = page.locator('[role="textbox"]').first();
    this.editIconButton = page.locator('.anticon-edit').first();
    this.editFieldTypography = page.locator('.ant-typography-edit-content').first();
    this.removeButtonsCircle = page.locator('.ant-btn-circle');
    this.yesConfirmButton = page.getByRole('button', { name: 'Yes' });
    this.summaryTab = page.locator('[data-testid="entity-summary-tab"]');
    this.propertiesSection = page.locator('[data-testid="properties-section"]');
    this.aboutSection = page.locator('text="About"');
    this.templateSection = page.locator('[data-testid="template-section"]');
    this.assetsHeading = page.locator('text="Assets"');
    this.domainsHeading = page.locator('text="Domains"');
    this.dataProductsHeading = page.locator('text="Data Products"');
  }

  async navigateToDomainList(): Promise<void> {
    await this.page.goto('/domains');
    await this.page.waitForLoadState('networkidle');
  }

  async createDomain(domainName: string, isSubDomain: boolean = false): Promise<void> {
    await this.createDomainButton.click();

    if (isSubDomain) {
      const parentSelector = this.page.locator('.ant-select-selection-item');
      try {
        await parentSelector.isVisible({ timeout: SHORT_TIMEOUT });
        await parentSelector.hover();
        await this.parentDomainCloseButton.click();
      } catch {
        // Parent selector not visible, continue
      }
    }

    await this.createDomainNameInput.click();
    await this.createDomainNameInput.pressSequentially(domainName);

    await this.createDomainConfirmButton.click();
    await expect(this.page.getByText('Created domain!')).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async moveDomainToParent(parentName: string): Promise<void> {
    await this.entityMenuMoveButton.click();
    await expect(this.moveDomainModal).toBeVisible();

    const parentOption = this.moveDomainModal.getByRole('button').filter({ hasText: parentName });
    await parentOption.click();

    await this.moveDomainConfirmButton.click();
    await expect(this.page.getByText('Moved Domain!')).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async moveDomainToRoot(): Promise<void> {
    await this.entityMenuMoveButton.click();
    await expect(this.moveDomainModal).toBeVisible();

    await this.page.getByText('Move To').click();
    await this.moveDomainConfirmButton.click();
    await expect(this.page.getByText('Moved Domain!')).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async addDocumentation(description: string): Promise<void> {
    await this.documentationTab.click();

    try {
      await this.addDocumentationButton.click({ timeout: SHORT_TIMEOUT });
    } catch {
      await this.editDocumentationButton.click();
    }

    await this.documentationEditor.click();
    await this.documentationEditor.clear();
    await this.documentationEditor.pressSequentially(description);

    await this.saveDocumentationButton.click();
    await expect(this.page.getByText(description)).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async addLink(url: string, label: string): Promise<void> {
    await this.addLinkButton.click();
    await this.linkUrlInput.fill(url);
    await this.linkLabelInput.fill(label);
    await this.linkSubmitButton.click();
    await expect(this.page.getByText(label)).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async clearDocumentation(): Promise<void> {
    await this.editDocumentationButtonAlt.click();
    await this.documentationEditorAlt.click();
    await this.documentationEditorAlt.clear();
    await this.saveDocumentationButton.click();
    await expect(this.page.getByText('No documentation')).toBeVisible({ timeout: MEDIUM_TIMEOUT });

    // Remove all link buttons
    const count = await this.removeButtonsCircle.count();
    for (let i = 0; i < count; i++) {
      const btn = this.removeButtonsCircle.first();
      await btn.hover();
      await btn.click();
    }
  }

  async addOwner(displayName: string): Promise<void> {
    try {
      await this.toggleSidebarButton.click({ timeout: SHORT_TIMEOUT });
    } catch {
      // Sidebar button not visible, continue
    }

    await this.addOwnersButton.click();
    await this.ownersSearchInput.fill(displayName);
    await this.page.getByText(displayName).first().click();

    try {
      await this.addOwnerConfirmButton.click({ timeout: SHORT_TIMEOUT });
    } catch {
      // Confirm button not visible, continue
    }

    await expect(this.page.getByText(displayName)).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async deleteDomain(): Promise<void> {
    // Click delete button
    await this.entityMenuDeleteButton.click();
    await expect(this.page.getByText('Are you sure you want to remove this Domain?')).toBeVisible();

    // Confirm deletion
    await this.deleteConfirmButton.click();
    await this.page.waitForLoadState('networkidle');
  }

  async expandDomain(domainName: string): Promise<void> {
    const domainRow = this.page.getByText(domainName, { exact: true });
    await domainRow.locator('[aria-label="right"]').click();
  }

  async editDomainName(newName: string): Promise<void> {
    await this.editIconButton.click();
    await this.editFieldTypography.fill(newName);
    await this.editFieldTypography.press('Enter');
    await expect(this.page.getByText(newName)).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async addAssetToDomain(assetName: string, assetUrn: string): Promise<void> {
    await this.page.goto('/domain/urn:li:domain:marketing');
    await this.page.waitForLoadState('networkidle');

    await this.page.getByText('Add to Assets').click();
    await expect(this.page.getByText('Add assets to Domain')).toBeVisible();

    const searchInput = this.page.locator('[data-testid="search-input"]').last();
    await searchInput.fill(assetName);

    const checkbox = this.page.locator(`[data-testid="checkbox-${assetUrn}"]`);
    await expect(checkbox).toBeVisible({ timeout: MEDIUM_TIMEOUT });
    await checkbox.click();

    await this.page.locator('#continueButton').click();
    await expect(this.page.getByText('Added assets to Domain!')).toBeVisible({ timeout: LONG_TIMEOUT });
  }

  async addTagToAsset(tagName: string, assetUrn: string = CYPRESS_BAZ2_CHART_URN): Promise<void> {
    // Open bulk edit mode and verify empty selection
    await this.searchResultsEditButton.click({ timeout: MEDIUM_TIMEOUT });
    await expect(this.page.getByText('0 selected')).toBeVisible();

    // Search and select the asset
    const searchInput = this.page.locator('[data-testid="search-input"]').last();
    await searchInput.fill('Baz Chart 2');
    const checkbox = this.page.locator(`[data-testid="checkbox-${assetUrn}"]`);
    await checkbox.click();

    // Open tag modal and add tag
    await this.page.getByText('Tags').first().click();
    await this.page.getByText('Add tags').click();
    await this.tagTermModalInput.fill(tagName);
    await this.page.locator(`[data-testid="tag-term-option"]:has-text("${tagName}")`).click();

    // Submit tag change
    await this.page.getByText('Add Tags').click();
    await this.addTagButton.click();
    await expect(this.page.getByText(tagName)).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async removeTagFromAsset(tagName: string, assetUrn: string = CYPRESS_BAZ2_CHART_URN): Promise<void> {
    // Open bulk edit mode
    await this.searchResultsEditButton.click({ timeout: MEDIUM_TIMEOUT });

    // Select the asset
    const checkbox = this.page.locator(`[data-testid="checkbox-${assetUrn}"]`);
    await checkbox.click();

    // Open tag modal and remove tag
    await this.page.getByText('Tags').first().click();
    await this.page.getByText('Remove tags').click();
    await this.tagTermModalInput.fill(tagName);
    await this.page.locator(`[data-testid="tag-term-option"]:has-text("${tagName}")`).click();

    // Submit tag removal
    await this.page.getByText('Remove Tags').click();
    await this.addTagButton.click();
  }

  async unsetDomainFromAsset(): Promise<void> {
    const domainButton = this.page.locator('[class*="dropdown-trigger"], [role="button"]')
      .filter({ hasText: 'Domain' })
      .nth(0);
    try {
      await domainButton.click({ timeout: SHORT_TIMEOUT });
      await this.page.getByText('Unset Domain').click();
      await this.page.getByRole('button', { name: 'Yes' }).click();
      await this.page.waitForLoadState('networkidle');
    } catch {
      // Domain button not visible, continue
    }
  }

  getDomainOptionByName(domainName: string): Locator {
    return this.page.locator(`[data-testid="domain-option-${domainName}"]`);
  }

  getDomainOptionsAll(): Locator {
    return this.page.locator('[data-testid^="domain-option-"]');
  }

  getMarketingDomainLink(): Locator {
    return this.page.getByRole('link', { name: 'Marketing' });
  }
}
