import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base-page';

export class DatasetPage extends BasePage {
  readonly datasetName: Locator;
  readonly schemaTab: Locator;
  readonly lineageTab: Locator;
  readonly propertiesTab: Locator;

  constructor(page: Page) {
    super(page);
    this.datasetName = page.locator('[data-testid="dataset-name"]');
    this.schemaTab = page.locator('[data-testid="schema-tab"]');
    this.lineageTab = page.locator('[data-testid="lineage-tab"]');
    this.propertiesTab = page.locator('[data-testid="properties-tab"]');
  }

  async navigateToDataset(urn: string): Promise<void> {
    await this.navigate(`/dataset/${encodeURIComponent(urn)}`);
    await this.page.waitForLoadState('networkidle');
    await this.page.waitForTimeout(5000);
  }

  async viewSchema(): Promise<void> {
    await this.schemaTab.click();
    await this.waitForPageLoad();
  }

  async viewLineage(): Promise<void> {
    await this.lineageTab.click();
    await this.waitForPageLoad();
  }

  async getDatasetName(): Promise<string> {
    return await this.datasetName.textContent() || '';
  }

  async closeAnyOpenModal(): Promise<void> {
    const closeButton = this.page.locator('button[aria-label="Close"]');
    if (await closeButton.isVisible()) {
      await closeButton.click();
    }
  }

  async clickSchemaField(fieldName: string): Promise<void> {
    await this.page.getByText(fieldName).click();
    await this.page.waitForTimeout(1000);
  }

  async addBusinessAttributeToField(fieldName: string, attributeName: string): Promise<void> {
    await this.clickSchemaField(fieldName);

    const businessAttributeSection = this.page.locator(`[data-testid="schema-field-${fieldName}-businessAttribute"]`);
    await businessAttributeSection.locator('text=Add Attribute').click();

    const modalInput = this.page.locator('[data-testid="business-attribute-modal-input"]');
    await modalInput.fill(attributeName);

    await this.page.locator('[data-testid="business-attribute-option"]').click();
    await this.page.locator('[data-testid="add-attribute-from-modal-btn"]').click();
    await expect(this.page.locator('[data-testid="add-attribute-from-modal-btn"]')).not.toBeVisible();

    await expect(businessAttributeSection.getByText(attributeName)).toBeVisible();
  }

  async removeBusinessAttributeFromField(fieldName: string, attributeName: string): Promise<void> {
    const businessAttributeSection = this.page.locator(`[data-testid="schema-field-${fieldName}-businessAttribute"]`);

    const closeIcon = businessAttributeSection.locator('span[aria-label=close]');
    await closeIcon.hover({ force: true });
    await closeIcon.click({ force: true });

    await this.page.getByText('Yes').click({ force: true });

    await expect(businessAttributeSection.getByText(attributeName)).not.toBeVisible();
  }

  async expectBusinessAttributeOnField(fieldName: string, attributeName: string): Promise<void> {
    const businessAttributeSection = this.page.locator(`[data-testid="schema-field-${fieldName}-businessAttribute"]`);
    await expect(businessAttributeSection.getByText(attributeName)).toBeVisible();
  }

  async expectBusinessAttributeNotOnField(fieldName: string, attributeName: string): Promise<void> {
    const businessAttributeSection = this.page.locator(`[data-testid="schema-field-${fieldName}-businessAttribute"]`);
    await expect(businessAttributeSection.getByText(attributeName)).not.toBeVisible();
  }

  async expectTagOnFieldFromAttribute(fieldName: string, tagName: string): Promise<void> {
    await this.clickSchemaField(fieldName);
    await expect(this.page.getByText(tagName)).toBeVisible();
  }

  async expectTermOnFieldFromAttribute(fieldName: string, termName: string): Promise<void> {
    await this.clickSchemaField(fieldName);
    await expect(this.page.getByText(termName)).toBeVisible();
  }
}
