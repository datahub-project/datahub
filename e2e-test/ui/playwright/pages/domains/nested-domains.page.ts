/**
 * NestedDomainsPage — page object for domain entity pages with advanced operations.
 * Used when working with an already-created domain entity at /domain/:urn
 *
 * Covers domain hierarchy, name editing, documentation, links, and ownership.
 * All selectors use data-testid attributes for reliability.
 *
 * Related: DomainsPage for /domains list operations (creation, deletion, entity assignment)
 */

import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { GraphQLHelper } from '../../helpers/graphql-helper';

const SHORT_TIMEOUT = 5000;
const MEDIUM_TIMEOUT = SHORT_TIMEOUT * 3;
const LONG_TIMEOUT = SHORT_TIMEOUT * 6;

const DELAY_TYPING = 10;
const DELAY_SEQUENTIAL = 50;
const DELAY_SMALL = 200;
const DELAY_MEDIUM = 300;
const DELAY_LARGE = 500;
const DELAY_XL = 1000;
const DELAY_2XL = 1500;
const DELAY_3XL = 2000;

const KEY_CTRL_A = 'Control+A';
const KEY_ENTER = 'Enter';

export class NestedDomainsPage extends BasePage {
  private readonly graphqlHelper: GraphQLHelper;

  readonly browseV2Container: Locator;
  readonly createDomainButton: Locator;
  readonly createDomainModal: Locator;
  readonly createDomainNameInput: Locator;
  readonly createDomainConfirmButton: Locator;
  readonly parentDomainCloseButton: Locator;
  readonly entityMenuMoveButton: Locator;
  readonly moveDomainModal: Locator;
  readonly moveDomainConfirmButton: Locator;
  readonly entityMenuDeleteButton: Locator;
  readonly deleteConfirmButton: Locator;
  readonly documentationTab: Locator;
  readonly editDocumentationButton: Locator;
  readonly documentationEditor: Locator;
  readonly publishButton: Locator;
  readonly descriptionViewer: Locator;
  readonly addRelatedButton: Locator;
  readonly addLinkMenuItem: Locator;
  readonly linkLabel: Locator;
  readonly sidebarCollapseTab: Locator;
  readonly addOwnersButton: Locator;
  readonly addOwnersSelect: Locator;
  readonly dropdownSearchInput: Locator;
  readonly addOwnerConfirmButton: Locator;
  readonly assetsTab: Locator;
  readonly pageTitle: Locator;
  readonly domainLinks: Locator;
  readonly summaryTab: Locator;
  readonly propertiesSection: Locator;
  readonly aboutSection: Locator;
  readonly templateSection: Locator;
  readonly assetsHeading: Locator;
  readonly domainsHeading: Locator;
  readonly dataProductsHeading: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.graphqlHelper = new GraphQLHelper(page);
    this.browseV2Container = page.locator('[id="browse-v2"]');
    this.createDomainButton = page.locator('[id="browse-v2"]').locator('button').nth(0);
    this.createDomainModal = page.getByText('Create New Domain');
    this.createDomainNameInput = page.getByTestId('create-domain-name');
    this.createDomainConfirmButton = page.getByTestId('create-domain-button');
    this.parentDomainCloseButton = page.locator('[aria-label="close-circle"]');
    this.entityMenuMoveButton = page.getByTestId('entity-menu-move-button');
    this.moveDomainModal = page.getByTestId('move-domain-modal');
    this.moveDomainConfirmButton = page.getByTestId('move-domain-modal-move-button');
    this.entityMenuDeleteButton = page.getByTestId('entity-menu-delete-button');
    this.deleteConfirmButton = page.getByRole('button', { name: 'Yes' });
    this.documentationTab = page.getByTestId('documentation-tab');
    this.editDocumentationButton = page.getByTestId('edit-description-button');
    this.documentationEditor = page.getByTestId('description-editor');
    this.publishButton = page.getByTestId('publish-button');
    this.descriptionViewer = page.getByTestId('description-viewer');
    this.addRelatedButton = page.getByTestId('add-related-button');
    this.addLinkMenuItem = page.getByTestId('menu-item-add-link');
    this.linkLabel = page.getByTestId('link-label');
    this.sidebarCollapseTab = page.locator('#entity-sidebar-tabs-tab-collapse');
    this.addOwnersButton = page.getByTestId('add-owners-button');
    this.addOwnersSelect = page.getByTestId('add-owners-select');
    this.dropdownSearchInput = page.getByTestId('dropdown-search-input');
    this.addOwnerConfirmButton = page.locator('#addOwnerButton');
    this.assetsTab = page.locator('[data-node-key="Assets"]');
    this.pageTitle = page.getByTestId('page-title');
    this.domainLinks = page.getByRole('link').filter({ hasText: /^[A-Z]/ });
    this.summaryTab = page.getByTestId('entity-summary-tab');
    this.propertiesSection = page.getByTestId('properties-section');
    this.aboutSection = page.getByTestId('about-section');
    this.templateSection = page.getByTestId('template-section');
    this.assetsHeading = page.getByTestId('assets-heading');
    this.domainsHeading = page.getByTestId('domains-heading');
    this.dataProductsHeading = page.getByTestId('data-products-heading');
  }

  async navigateToDomainList(): Promise<void> {
    await this.page.goto('/domains');
    await this.page.waitForLoadState('load');
  }

  async createDomain(domainName: string, isSubDomain: boolean = false): Promise<string> {
    await this.createDomainButton.click();
    await this.page.waitForLoadState('load');

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

    await this.createDomainNameInput.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await this.createDomainNameInput.click();
    await this.createDomainNameInput.pressSequentially(domainName, { delay: DELAY_SEQUENTIAL });

    // Intercept GraphQL response to get the actual domain URN from API
    const responsePromise = this.graphqlHelper.waitForGraphQLResponse('createDomain');
    await this.createDomainConfirmButton.click();

    // Wait for success message indicating domain was created
    await expect(this.page.getByText('Created domain!')).toBeVisible({ timeout: LONG_TIMEOUT });

    // Get URN from GraphQL response
    const response = await responsePromise;
    const domainUrn = (response.data as Record<string, string>).createDomain;

    if (!domainUrn) {
      throw new Error(`Failed to extract domain URN from GraphQL response: ${JSON.stringify(response)}`);
    }

    return domainUrn;
  }

  async moveDomainToParent(parentName: string): Promise<void> {
    await this.page.waitForLoadState('load');
    await this.page.waitForTimeout(DELAY_XL);

    await this.entityMenuMoveButton.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await this.entityMenuMoveButton.click({ force: true });
    await this.page.waitForTimeout(DELAY_2XL);

    const parentSelect = this.page.getByTestId('parent-domain-select');
    await parentSelect.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await this.page.waitForTimeout(DELAY_LARGE);

    await parentSelect.click({ force: true });
    await this.page.waitForTimeout(DELAY_2XL);

    const domainOption = this.moveDomainModal.getByTestId(`domain-option-${parentName}`);
    await domainOption.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await domainOption.click({ force: true });
    await this.page.waitForTimeout(DELAY_LARGE);

    await this.moveDomainConfirmButton.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await this.moveDomainConfirmButton.click({ force: true });
    await this.page.waitForTimeout(DELAY_LARGE);

    await expect(this.page.getByText('Moved Domain!')).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async addDocumentation(description: string): Promise<void> {
    await this.page.waitForLoadState('load');
    await this.page.waitForTimeout(DELAY_LARGE);

    await this.editDocumentationButton.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await this.editDocumentationButton.click({ force: true });
    await this.page.waitForTimeout(DELAY_XL);

    await this.documentationEditor.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await this.documentationEditor.focus();
    await this.page.waitForTimeout(DELAY_MEDIUM);

    await this.documentationEditor.press(KEY_CTRL_A);
    await this.page.waitForTimeout(DELAY_SMALL);
    await this.documentationEditor.pressSequentially(description, { delay: DELAY_TYPING });
    await this.page.waitForTimeout(DELAY_LARGE);

    await this.publishButton.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await this.publishButton.click({ force: true });
    await this.page.waitForTimeout(DELAY_LARGE);

    await expect(this.descriptionViewer.getByText(description)).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async addLink(url: string, label: string): Promise<void> {
    await this.page.waitForLoadState('load');
    await this.page.waitForTimeout(DELAY_LARGE);

    const addLinkBtn = this.addRelatedButton.nth(0);
    await addLinkBtn.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await addLinkBtn.click({ force: true });
    await this.page.waitForTimeout(DELAY_LARGE);

    await this.addLinkMenuItem.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await this.addLinkMenuItem.click({ force: true });
    await this.page.waitForTimeout(DELAY_XL);

    const modal = this.page.locator('[role="dialog"]').last();
    const urlInput = modal.locator('input').first();
    const labelInput = modal.locator('input').nth(1);

    await urlInput.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await urlInput.fill(url);
    await labelInput.fill(label);

    const submitBtn = modal
      .locator('button')
      .filter({ hasText: /Submit|Save|Add/ })
      .last();
    await submitBtn.click({ force: true });
    await this.page.waitForTimeout(DELAY_LARGE);

    await expect(this.linkLabel.getByText(label)).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async addOwner(displayName: string): Promise<void> {
    await this.sidebarCollapseTab.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await this.sidebarCollapseTab.click({ force: true });
    await this.page.waitForTimeout(DELAY_XL);

    await this.addOwnersButton.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await this.page.waitForTimeout(DELAY_MEDIUM);
    await this.addOwnersButton.click({ force: true });
    await this.page.waitForTimeout(DELAY_XL);

    await this.addOwnersSelect.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });
    await this.page.waitForTimeout(DELAY_MEDIUM);

    await this.addOwnersSelect.click({ force: true });
    await this.page.waitForTimeout(DELAY_LARGE);

    await this.dropdownSearchInput.waitFor({ state: 'visible', timeout: MEDIUM_TIMEOUT });

    await this.dropdownSearchInput.pressSequentially(displayName, { delay: DELAY_TYPING });
    await this.page.waitForTimeout(DELAY_LARGE);

    const ownerOption = this.page
      .getByTestId(/^option-/)
      .filter({ hasText: displayName })
      .first();
    await ownerOption.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await ownerOption.click({ force: true });
    await this.page.waitForTimeout(DELAY_LARGE);

    await this.addOwnerConfirmButton.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await this.addOwnerConfirmButton.click({ force: true });
    await this.page.waitForTimeout(DELAY_LARGE);

    const ownerElement = this.page.getByTestId(/^owner-urn:li:corpuser:/).first();
    await expect(ownerElement).toBeVisible({ timeout: MEDIUM_TIMEOUT });
  }

  async deleteDomain(): Promise<void> {
    try {
      await this.entityMenuDeleteButton.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
      await this.entityMenuDeleteButton.click();

      await expect(this.page.getByText('Are you sure you want to remove this Domain?')).toBeVisible({
        timeout: SHORT_TIMEOUT,
      });

      await this.deleteConfirmButton.click();
      await this.page.waitForLoadState('load');
    } catch (error) {
      await this.navigateToDomainList();
      throw error;
    }
  }

  async editDomainName(newName: string): Promise<void> {
    const editIcon = this.page.locator('.anticon-edit').first();
    await editIcon.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
    await editIcon.scrollIntoViewIfNeeded();
    await this.page.waitForTimeout(DELAY_MEDIUM);

    await editIcon.click({ force: true });
    await this.page.waitForTimeout(DELAY_XL);

    const editableInput = this.page.locator('input.ant-input').first();
    const editableDiv = this.page.locator('[contenteditable="true"]').first();

    let editField = editableInput;
    let found = await editField.count();

    if (found === 0) {
      editField = editableDiv;
      found = await editField.count();
    }

    if (found > 0) {
      await editField.waitFor({ state: 'visible', timeout: SHORT_TIMEOUT });
      await this.page.waitForTimeout(DELAY_MEDIUM);
      await editField.click({ force: true });

      await editField.press(KEY_CTRL_A);
      await editField.pressSequentially(newName, { delay: DELAY_SEQUENTIAL });
      await this.page.waitForTimeout(DELAY_MEDIUM);

      await editField.press(KEY_ENTER);
      await this.page.waitForTimeout(DELAY_3XL);

      await expect(this.page.locator('*').filter({ hasText: newName }).first()).toBeVisible({
        timeout: MEDIUM_TIMEOUT,
      });
    }
  }
}
