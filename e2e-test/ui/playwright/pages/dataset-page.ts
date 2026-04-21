import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base-page';
import type { DataHubLogger } from '../utils/logger';

export class DatasetPage extends BasePage {
  readonly datasetName: Locator;
  readonly schemaTab: Locator;
  readonly lineageTab: Locator;
  readonly propertiesTab: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.datasetName = page.locator('[data-testid="dataset-name"]');
    this.schemaTab = page.locator('[data-testid="schema-tab"]');
    this.lineageTab = page.locator('[data-testid="lineage-tab"]');
    this.propertiesTab = page.locator('[data-testid="properties-tab"]');
  }

  async navigateToDataset(urn: string): Promise<void> {
    await this.navigate(`/dataset/${encodeURIComponent(urn)}`);
    await this.page.waitForLoadState('networkidle');
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
    // Use the row's id attribute (set by SchemaTable's onRow handler) so the click
    // reliably hits the row element and triggers the SchemaFieldDrawer to open.
    await this.page.locator(`#column-${fieldName}`).click();
  }

  async addBusinessAttributeToField(fieldName: string, attributeName: string): Promise<void> {
    await this.clickSchemaField(fieldName);

    // The V2 schema field drawer renders the business attribute section inside SidebarSection.
    // SidebarSection sets data-testid="sidebar-section-content-{title}" on its content container.
    const businessAttributeSection = this.getBusinessAttributeSection(fieldName);
    await businessAttributeSection.waitFor({ state: 'visible', timeout: 15000 });
    await businessAttributeSection.hover();
    await businessAttributeSection.locator('text=Add Attribute').click();

    // The Select component requires typing into the inner search input, not fill() on the outer wrapper.
    const modalSearchInput = this.page.locator('[data-testid="business-attribute-modal-input"]')
      .locator('.ant-select-selection-search-input');
    await modalSearchInput.fill(attributeName);

    // Wait for options to appear, then use ArrowDown + Enter to select without depending on click position.
    await this.page.locator('[data-testid="business-attribute-option"]').first().waitFor({ state: 'visible' });
    await modalSearchInput.press('ArrowDown');
    await modalSearchInput.press('Enter');

    // Once an option is selected the Done button becomes enabled.
    // Use evaluate+click() to trigger the React onClick via the native DOM click method,
    // bypassing the Playwright actionability check that fails due to the modal body overlay.
    const doneBtn = this.page.locator('[data-testid="add-attribute-from-modal-btn"]');
    await doneBtn.waitFor({ state: 'visible' });
    await expect(doneBtn).toBeEnabled();
    await doneBtn.evaluate((el: HTMLElement) => el.click());
    await expect(doneBtn).not.toBeVisible();

    await expect(businessAttributeSection.getByText(attributeName)).toBeVisible();
  }

  getBusinessAttributeSection(_fieldName: string): Locator {
    // In the V2 entity view, the schema field drawer renders a SidebarSection with
    // data-testid="sidebar-section-content-Business Attribute" for the business attribute content.
    return this.page.locator('[data-testid="sidebar-section-content-Business Attribute"]');
  }

  async removeBusinessAttributeFromField(fieldName: string, attributeName: string): Promise<void> {
    const businessAttributeSection = this.getBusinessAttributeSection(fieldName);

    const closeIcon = businessAttributeSection.locator('span[aria-label=close]');
    await closeIcon.hover({ force: true });
    await closeIcon.click({ force: true });

    // Scope the confirmation button to the dialog to avoid matching "Yes" text elsewhere.
    await this.page.getByRole('dialog').getByRole('button', { name: 'Yes' }).click({ force: true });

    await expect(businessAttributeSection.getByText(attributeName)).not.toBeVisible();
  }

  async expectBusinessAttributeOnField(fieldName: string, attributeName: string): Promise<void> {
    const businessAttributeSection = this.getBusinessAttributeSection(fieldName);
    await expect(businessAttributeSection.getByText(attributeName)).toBeVisible();
  }

  async expectBusinessAttributeNotOnField(fieldName: string, attributeName: string): Promise<void> {
    const businessAttributeSection = this.getBusinessAttributeSection(fieldName);
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
