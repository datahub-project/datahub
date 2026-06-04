/**
 * DomainEntity — page object for single domain entity pages.
 * Used when working with an already-created domain entity at /domain/:urn
 *
 * Covers entity-level operations: name editing, documentation, links, ownership, and deletion.
 * Also covers domain hierarchy operations (moving to parent domains).
 * All selectors use data-testid attributes for reliability.
 *
 * Related: DomainsPage for /domains list operations (creation, search, batch assignment)
 */

import { Locator, Page, expect } from '@playwright/test';
import { BasePage } from '../base.page';
import type { DataHubLogger } from '../../utils/logger';
import { GraphQLHelper } from '../../helpers/graphql-helper';
import { TIMEOUTS, DELAYS, KEYS, LOAD_STATES } from '../../utils/constants';

export class DomainEntityPage extends BasePage {
  private readonly graphqlHelper: GraphQLHelper;

  readonly createDomainButton: Locator;
  readonly createDomainModal: Locator;
  readonly createDomainNameInput: Locator;
  readonly createDomainConfirmButton: Locator;
  readonly entityMenuMoveButton: Locator;
  readonly moveDomainModal: Locator;
  readonly moveDomainConfirmButton: Locator;
  readonly parentDomainSelect: Locator;
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
  readonly summaryTab: Locator;
  readonly propertiesSection: Locator;
  readonly aboutSection: Locator;
  readonly templateSection: Locator;
  readonly assetsHeading: Locator;
  readonly domainsHeading: Locator;
  readonly dataProductsHeading: Locator;
  readonly urlInput: Locator;
  readonly labelInput: Locator;
  readonly linkFormSubmitButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.graphqlHelper = new GraphQLHelper(page);
    this.createDomainButton = page.getByTestId('sidebar-create-domain-button');
    this.createDomainModal = page.getByText('Create New Domain');
    this.createDomainNameInput = page.getByTestId('create-domain-name');
    this.createDomainConfirmButton = page.getByTestId('create-domain-button');
    this.entityMenuMoveButton = page.getByTestId('entity-menu-move-button');
    this.moveDomainModal = page.getByTestId('move-domain-modal');
    this.moveDomainConfirmButton = page.getByTestId('move-domain-modal-move-button');
    this.parentDomainSelect = page.getByTestId('parent-domain-select');
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
    this.sidebarCollapseTab = page.getByTestId('entity-sidebar-collapse-tab');
    this.addOwnersButton = page.getByTestId('add-owners-button');
    this.addOwnersSelect = page.getByTestId('add-owners-select');
    this.dropdownSearchInput = page.getByTestId('dropdown-search-input');
    this.addOwnerConfirmButton = page.getByTestId('modal-add-owner-button');
    this.summaryTab = page.getByTestId('entity-summary-tab');
    this.propertiesSection = page.getByTestId('properties-section');
    this.aboutSection = page.getByTestId('about-section');
    this.templateSection = page.getByTestId('template-wrapper');
    this.assetsHeading = page.getByTestId('assets-module-title');
    this.domainsHeading = page.getByTestId('hierarchy-module-title');
    this.dataProductsHeading = page.getByTestId('data-products-module-title');
    this.urlInput = page.getByTestId('url-input');
    this.labelInput = page.getByTestId('label-input');
    this.linkFormSubmitButton = page.getByTestId('link-form-modal-submit-button');
  }

  // Private helper methods for dynamic selectors
  private getDomainOption(parentName: string): Locator {
    return this.moveDomainModal.getByTestId(`domain-option-${parentName}`);
  }

  private getOwnerOption(displayName: string): Locator {
    return this.page.getByTestId(/^option-/).filter({ hasText: displayName });
  }

  private getEditableContainer(): Locator {
    return this.page.getByTestId('entity-name-editable');
  }

  private getEditButton(container: Locator): Locator {
    return container.getByRole('button', { name: 'Edit', exact: true });
  }

  private getEditInput(): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- Ant Design textarea dynamically created without semantic role
    return this.page.locator('.ant-typography-edit-content textarea');
  }

  async createDomain(domainName: string): Promise<string> {
    await this.createDomainButton.click();
    await this.page.waitForLoadState(LOAD_STATES.LOAD);

    await this.createDomainNameInput.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
    await this.createDomainNameInput.click();
    await this.createDomainNameInput.pressSequentially(domainName, { delay: DELAYS.SEQUENTIAL });

    // Intercept GraphQL response to get the actual domain URN from API
    const responsePromise = this.graphqlHelper.waitForGraphQLResponse('createDomain');
    await this.createDomainConfirmButton.click();

    // Wait for success message indicating domain was created
    await expect(this.page.getByText('Created domain!')).toBeVisible({ timeout: TIMEOUTS.LONG });

    // Get URN from GraphQL response
    const response = await responsePromise;
    const domainUrn = (response.data as Record<string, string>).createDomain;

    if (!domainUrn) {
      throw new Error(`Failed to extract domain URN from GraphQL response: ${JSON.stringify(response)}`);
    }

    return domainUrn;
  }

  async moveDomainToParent(parentName: string): Promise<void> {
    await this.page.waitForLoadState(LOAD_STATES.LOAD);

    await this.entityMenuMoveButton.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.entityMenuMoveButton.click();

    await this.parentDomainSelect.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.parentDomainSelect.click();

    const domainOption = this.getDomainOption(parentName);
    await domainOption.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    await domainOption.click();

    await this.moveDomainConfirmButton.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
    await this.moveDomainConfirmButton.click();

    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await expect(this.page.getByText('Moved Domain!')).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async addDocumentation(description: string): Promise<void> {
    await this.page.waitForLoadState(LOAD_STATES.LOAD);

    await this.editDocumentationButton.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.editDocumentationButton.click();

    await this.documentationEditor.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.documentationEditor.focus();

    await this.documentationEditor.press(KEYS.CTRL_A);
    await this.documentationEditor.pressSequentially(description, { delay: DELAYS.TYPING });

    await this.publishButton.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
    await this.publishButton.click();

    await expect(this.descriptionViewer.getByText(description)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async addLink(url: string, label: string): Promise<void> {
    await this.page.waitForLoadState(LOAD_STATES.LOAD);
    // Ensure page is ready before clicking
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await this.addRelatedButton.click();

    await this.addLinkMenuItem.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.addLinkMenuItem.click();

    await this.urlInput.waitFor({ state: 'visible', timeout: TIMEOUTS.EXTRA_LONG });
    await this.urlInput.fill(url);
    await this.labelInput.fill(label);
    await this.linkFormSubmitButton.click();

    await expect(this.linkLabel.getByText(label)).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async addOwner(displayName: string): Promise<void> {
    // Sidebar is closed by default - open it first to access the add owners button
    await this.sidebarCollapseTab.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
    await this.sidebarCollapseTab.click();

    // Wait for sidebar to open and button to become available
    await this.addOwnersButton.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    await this.addOwnersButton.click();

    await this.addOwnersSelect.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await this.addOwnersSelect.click();

    // Wait for dropdown search input to appear (triggers on click, may have animation)
    await this.dropdownSearchInput.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });

    await this.dropdownSearchInput.pressSequentially(displayName, { delay: DELAYS.TYPING });

    const ownerOption = this.getOwnerOption(displayName);
    await ownerOption.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await ownerOption.click();

    await this.addOwnerConfirmButton.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
    await this.addOwnerConfirmButton.click();

    // Verify owner was added - just check that the confirm button is gone (modal closed successfully)
    await expect(this.addOwnerConfirmButton).not.toBeVisible({ timeout: TIMEOUTS.SHORT });
  }

  async deleteDomain(): Promise<void> {
    await this.entityMenuDeleteButton.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
    await this.entityMenuDeleteButton.click();

    await expect(this.page.getByText('Are you sure you want to remove this Domain?')).toBeVisible({
      timeout: TIMEOUTS.SHORT,
    });

    await this.deleteConfirmButton.click();
    await this.page.waitForLoadState(LOAD_STATES.LOAD);
  }

  async editDomainName(newName: string): Promise<void> {
    // Ensure page is fully loaded before manipulating DOM
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    const editableContainer = this.getEditableContainer();
    await editableContainer.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });

    const editButton = this.getEditButton(editableContainer);
    await editButton.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
    await editButton.click();

    const editInput = this.getEditInput();
    await editInput.waitFor({ state: 'visible', timeout: TIMEOUTS.LONG });
    await editInput.fill(newName);
    await editInput.press(KEYS.ENTER);

    // Wait for mutation to complete and verify the new name is visible
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await expect(editableContainer).toContainText(newName, { timeout: TIMEOUTS.MEDIUM });
  }
}
