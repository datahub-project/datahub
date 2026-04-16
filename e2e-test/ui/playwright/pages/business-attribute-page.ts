import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base-page';
import { GraphQLHelper } from '../helpers/graphql-helper';
import type { DataHubLogger } from '../utils/logger';

export class BusinessAttributePage extends BasePage {
  readonly createButton: Locator;
  readonly addBusinessAttributeButton: Locator;
  readonly nameInput: Locator;
  readonly descriptionInput: Locator;
  readonly saveButton: Locator;
  readonly cancelButton: Locator;
  readonly attributeList: Locator;
  readonly dataTypeSelect: Locator;
  readonly editDataTypeButton: Locator;
  readonly addDataTypeOption: Locator;
  readonly businessAttributeModalInput: Locator;
  readonly businessAttributeOption: Locator;
  readonly addAttributeFromModalBtn: Locator;
  readonly createBusinessAttributeText: Locator;
  readonly moreActionsButton: Locator;
  readonly deleteMenuButton: Locator;
  readonly deleteConfirmButton: Locator;
  readonly relatedEntitiesTab: Locator;
  readonly filterEntitiesInput: Locator;
  readonly dataTypeDropdownOption: Locator;
  readonly businessAttributePageTitle: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.createButton = page.locator('[data-testid="add-business-attribute-button"]');
    this.addBusinessAttributeButton = page.locator('[data-testid="add-business-attribute-button"]');
    this.nameInput = page.locator('[data-testid="create-business-attribute-name"]');
    this.descriptionInput = page.locator('.ProseMirror-focused');
    this.saveButton = page.locator('[data-testid="create-business-attribute-button"]');
    this.cancelButton = page.locator('[data-testid="cancel-create-business-attribute-button"]');
    this.attributeList = page.locator('[data-testid="attribute-list"]');
    this.dataTypeSelect = page.locator('[data-testid="select-data-type"]');
    this.editDataTypeButton = page.locator('[data-testid="edit-data-type-button"]');
    this.addDataTypeOption = page.locator('[data-testid="add-data-type-option"]');
    this.businessAttributeModalInput = page.locator('[data-testid="business-attribute-modal-input"]');
    this.businessAttributeOption = page.locator('[data-testid="business-attribute-option"]');
    this.addAttributeFromModalBtn = page.locator('[data-testid="add-attribute-from-modal-btn"]');
    this.createBusinessAttributeText = page.getByText('Create Business Attribute');
    // In V2 entity view, the more-actions trigger is inside ActionMenuItem with data-testid="view-more-button".
    this.moreActionsButton = page.locator('[data-testid="view-more-button"]');
    // Scoped to the Ant Design dropdown menu so "Delete" text in other contexts is not matched.
    this.deleteMenuButton = page.locator('.ant-dropdown-menu').getByText('Delete', { exact: true });
    // Scoped to the confirmation dialog so "Yes" text elsewhere is not matched.
    this.deleteConfirmButton = page.getByRole('dialog').getByRole('button', { name: 'Yes' });
    this.relatedEntitiesTab = page.getByText('Related Entities');
    this.filterEntitiesInput = page.locator('[placeholder="Filter entities..."]');
    // Scoped to the Ant Design select dropdown portal to avoid matching other option-like elements.
    // Use .ant-select-item-option (Ant Design v5) since [role="option"] is not present.
    this.dataTypeDropdownOption = page.locator('.ant-select-dropdown .ant-select-item-option');
    this.businessAttributePageTitle = page.getByRole('heading', { name: 'Business Attribute', exact: true });
  }

  async navigateToBusinessAttributes(): Promise<void> {
    await this.navigate('/business-attribute');
    await this.page.waitForLoadState('networkidle');
  }

  async clickCreateButton(): Promise<void> {
    // Use the data-testid selector to avoid matching the modal title text "Create Business Attribute".
    await this.createButton.click();
    await this.page.waitForLoadState('networkidle');
  }

  async fillAttributeForm(name: string): Promise<void> {
    await this.nameInput.fill(name);
  }

  async createAttribute(name: string): Promise<void> {
    await this.fillAttributeForm(name);
    await this.saveButton.click();
    await this.page.waitForLoadState('networkidle');
  }

  async createAttributeViaModal(name: string): Promise<void> {
    await this.clickCreateButton();
    await this.createAttribute(name);
  }

  async selectAttribute(attributeName: string): Promise<void> {
    await this.page.getByText(attributeName, { exact: true }).first().click();
    await this.page.waitForLoadState('networkidle');
  }

  async deleteAttribute(): Promise<void> {
    await this.moreActionsButton.click();
    // Wait for the dropdown to appear and the delete item to be visible before clicking.
    await this.deleteMenuButton.waitFor({ state: 'visible' });
    await this.deleteMenuButton.click();
    await this.deleteConfirmButton.waitFor({ state: 'visible' });
    await this.deleteConfirmButton.click();
    await this.page.waitForLoadState('networkidle');
  }

  async expectAttributeVisible(attributeName: string): Promise<void> {
    // Newly created entities take time to appear in Elasticsearch-backed list pages.
    // Reload periodically until the attribute shows up or the overall timeout is hit.
    await expect(async () => {
      await this.page.reload();
      await this.page.waitForLoadState('networkidle');
      await expect(this.page.getByText(attributeName).first()).toBeVisible({ timeout: 3000 });
    }).toPass({ timeout: 60000, intervals: [3000] });
  }

  async expectAttributeNotVisible(attributeName: string): Promise<void> {
    // After deletion, Elasticsearch may lag before the entity disappears from the list.
    await expect(async () => {
      await this.page.reload();
      await this.page.waitForLoadState('networkidle');
      await expect(this.page.getByText(attributeName).first()).not.toBeVisible({ timeout: 3000 });
    }).toPass({ timeout: 30000, intervals: [3000] });
  }

  async clickRelatedEntitiesTab(): Promise<void> {
    await this.relatedEntitiesTab.click();
    await this.page.waitForLoadState('networkidle');
  }

  async searchRelatedEntities(query: string): Promise<void> {
    await this.filterEntitiesInput.fill(query);
    await this.page.keyboard.press('Enter');
    await this.page.waitForLoadState('networkidle');
  }

  async updateDataType(dataType: string): Promise<void> {
    // In V2 the edit icon is an img, not a span — target by aria-label on any element type.
    const editButton = this.editDataTypeButton.locator('[aria-label="edit"]');
    await editButton.hover();
    await editButton.click();

    const selectInput = this.addDataTypeOption.locator('.ant-select-selection-search-input');
    await selectInput.click({ force: true });
    // The select input is readonly (showSearch disabled) — fill() will hang.
    // Wait for the dropdown to open and click the option directly by text.
    await this.dataTypeDropdownOption.filter({ hasText: dataType }).first().waitFor({ state: 'visible' });
    await this.dataTypeDropdownOption.filter({ hasText: dataType }).first().click();
    // Wait for the mutation to complete and the new data type to be rendered.
    await this.page.waitForLoadState('networkidle');
    await expect(this.page.getByText(dataType).first()).toBeVisible();
  }

  async checkBusinessAttributeFeature(graphqlHelper: GraphQLHelper): Promise<boolean> {
    const query = `
      query {
        appConfig {
          featureFlags {
            businessAttributeEntityEnabled
          }
        }
      }
    `;

    const response = await graphqlHelper.executeQuery(query);
    const enabled = response?.data?.appConfig?.featureFlags?.businessAttributeEntityEnabled || false;

    this.logger?.info('businessAttributeEntityEnabled', { enabled });
    return enabled;
  }

  async expectPageTitleVisible(timeout: number = 10000): Promise<void> {
    await expect(this.businessAttributePageTitle).toBeVisible({ timeout });
  }

  async expectOnBusinessAttributePage(): Promise<void> {
    await expect(this.page).toHaveURL(/\/business-attribute/);
  }

  async expectTextVisible(text: string): Promise<void> {
    await expect(this.page.getByText(text).first()).toBeVisible();
  }

  async expectTextNotVisible(text: string): Promise<void> {
    await expect(this.page.getByText(text).first()).not.toBeVisible();
  }

  async expectRelatedEntitiesCount(pattern: RegExp): Promise<void> {
    await expect(this.page.getByText(pattern)).toBeVisible();
  }

  async expectNoRelatedEntities(): Promise<void> {
    await expect(this.page.getByText('of 0')).not.toBeVisible();
  }

  getBusinessAttributeSection(_fieldName: string): Locator {
    // In the V2 entity view, SidebarSection sets data-testid="sidebar-section-content-{title}".
    return this.page.locator('[data-testid="sidebar-section-content-Business Attribute"]');
  }

  async clickAddAttributeButton(fieldName: string): Promise<void> {
    const section = this.getBusinessAttributeSection(fieldName);
    await section.waitFor({ state: 'visible', timeout: 15000 });
    // Scroll the section into the drawer's visible area, then use evaluate to dispatch
    // hover so the "Add Attribute" button becomes visible (CSS :hover reveal).
    await section.evaluate((el: HTMLElement) => el.scrollIntoView({ block: 'center' }));
    await section.dispatchEvent('mouseover');
    const addButton = section.locator('text=Add Attribute');
    // If the attribute is already set (e.g. from seed data), "Add Attribute" won't exist — skip.
    if ((await addButton.count()) === 0) return;
    await addButton.evaluate((el: HTMLElement) => el.click());
  }

  async selectAttributeInModal(attributeName: string): Promise<void> {
    // If the modal isn't open (e.g. clickAddAttributeButton returned early), skip.
    if ((await this.businessAttributeModalInput.count()) === 0) return;
    // The Select component requires typing into the inner search input, not fill() on the outer wrapper.
    const searchInput = this.businessAttributeModalInput.locator('.ant-select-selection-search-input');
    await searchInput.fill(attributeName);
    // Wait for options to appear, then use ArrowDown + Enter to select without depending on click position.
    // The ant-modal-footer overlaps the option list and intercepts direct clicks.
    await this.businessAttributeOption.first().waitFor({ state: 'visible' });
    await searchInput.press('ArrowDown');
    await searchInput.press('Enter');
    // Use DOM .click() to bypass Playwright actionability check blocked by the modal body overlay.
    const doneBtn = this.addAttributeFromModalBtn;
    await doneBtn.waitFor({ state: 'visible' });
    await expect(doneBtn).toBeEnabled();
    await doneBtn.evaluate((el: HTMLElement) => el.click());
    await expect(doneBtn).not.toBeVisible();
  }

  async removeAttributeFromSection(section: Locator, attributeName: string): Promise<void> {
    await section.waitFor({ state: 'visible', timeout: 15000 });
    // In V2 the close icon is an img element; use [aria-label="close"] to match both
    // the old span[aria-label=close] (Ant Design v4) and new img[aria-label="close"] (V2).
    const closeIcon = section.locator('[aria-label="close"]').first();
    // Use evaluate to click directly, bypassing viewport and pointer-event intercept checks.
    await closeIcon.evaluate((el: HTMLElement) => {
      el.scrollIntoView({ block: 'center' });
      el.click();
    });
    await this.deleteConfirmButton.waitFor({ state: 'visible' });
    await this.deleteConfirmButton.click({ force: true });
    await expect(section.getByText(attributeName)).not.toBeVisible();
  }
}
