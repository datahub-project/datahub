import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './BasePage';
import { GraphQLHelper } from '../helpers/GraphQLHelper';

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

  constructor(page: Page) {
    super(page);
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
    this.moreActionsButton = page.locator('[data-icon="more"]');
    this.deleteMenuButton = page.getByText('Delete', { exact: true });
    this.deleteConfirmButton = page.getByText('Yes');
    this.relatedEntitiesTab = page.getByText('Related Entities');
    this.filterEntitiesInput = page.locator('[placeholder="Filter entities..."]');
    this.dataTypeDropdownOption = page.locator('.ant-select-item-option-content');
    this.businessAttributePageTitle = page.getByText('Business Attribute');
  }

  async navigateToBusinessAttributes(): Promise<void> {
    await this.navigate('/business-attribute');
    await this.page.waitForLoadState('networkidle');
  }

  async clickCreateButton(): Promise<void> {
    await this.createBusinessAttributeText.click();
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
    await this.page.getByText(attributeName, { exact: true }).click();
    await this.page.waitForLoadState('networkidle');
  }

  async deleteAttribute(): Promise<void> {
    await this.moreActionsButton.click();
    const deleteButton = this.page.locator('[data-testid="entity-menu-delete-button"]');
    await expect(deleteButton).toBeEnabled();
    await this.deleteMenuButton.click();
    await this.deleteConfirmButton.click();
    await this.page.waitForLoadState('networkidle');
  }

  async expectAttributeVisible(attributeName: string): Promise<void> {
    await expect(this.page.getByText(attributeName)).toBeVisible();
  }

  async expectAttributeNotVisible(attributeName: string): Promise<void> {
    await expect(this.page.getByText(attributeName)).not.toBeVisible();
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
    const editButton = this.editDataTypeButton.locator('span[aria-label=edit]');
    await editButton.hover();
    await editButton.click();

    await this.addDataTypeOption.locator('.ant-select-selection-search-input').click({ force: true });
    await this.dataTypeDropdownOption.filter({ hasText: dataType }).click();

    await expect(this.page.getByText(dataType)).toBeVisible();
  }

  async selectAttributeInModal(attributeName: string): Promise<void> {
    await this.businessAttributeModalInput.fill(attributeName);
    await this.businessAttributeOption.click();
    await this.addAttributeFromModalBtn.click();
    await expect(this.addAttributeFromModalBtn).not.toBeVisible();
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

    console.log('Business Attribute Feature Enabled:', enabled);
    return enabled;
  }

  async expectPageTitleVisible(timeout: number = 10000): Promise<void> {
    await expect(this.businessAttributePageTitle).toBeVisible({ timeout });
  }

  async expectOnBusinessAttributePage(): Promise<void> {
    await expect(this.page).toHaveURL(/\/business-attribute/);
  }

  async expectTextVisible(text: string): Promise<void> {
    await expect(this.page.getByText(text)).toBeVisible();
  }

  async expectTextNotVisible(text: string): Promise<void> {
    await expect(this.page.getByText(text)).not.toBeVisible();
  }

  async expectRelatedEntitiesCount(pattern: RegExp): Promise<void> {
    await expect(this.page.getByText(pattern)).toBeVisible();
  }

  async expectNoRelatedEntities(): Promise<void> {
    await expect(this.page.getByText('of 0')).not.toBeVisible();
  }

  getBusinessAttributeSection(fieldName: string): Locator {
    return this.page.locator(`[data-testid="schema-field-${fieldName}-businessAttribute"]`);
  }

  async clickAddAttributeButton(fieldName: string): Promise<void> {
    const section = this.getBusinessAttributeSection(fieldName);
    await section.locator('text=Add Attribute').click();
  }

  async removeAttributeFromSection(section: Locator, attributeName: string): Promise<void> {
    const closeIcon = section.locator('span[aria-label=close]');
    await closeIcon.hover({ force: true });
    await closeIcon.click({ force: true });
    await this.deleteConfirmButton.click({ force: true });
    await expect(section.getByText(attributeName)).not.toBeVisible();
  }
}
