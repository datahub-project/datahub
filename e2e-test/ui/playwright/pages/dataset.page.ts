import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { ConfirmationModalComponent } from './common/confirmation-modal-component';
import { ModalComponent } from './common/modal-component';

export class DatasetPage extends BasePage {
  readonly modalComponent: ModalComponent;
  readonly confirmationComponent: ConfirmationModalComponent;

  readonly datasetName: Locator;
  readonly schemaTab: Locator;
  readonly lineageTab: Locator;
  readonly propertiesTab: Locator;

  // ── Glossary term sidebar ──────────────────────────────────────────────────
  readonly sidebarGlossarySection: Locator;
  readonly addTermsButton: Locator;
  readonly tagTermModalInput: Locator;
  readonly tagTermOption: Locator;
  readonly addTagTermConfirmButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.modalComponent = new ModalComponent(page);
    this.confirmationComponent = new ConfirmationModalComponent(page);

    this.datasetName = page.locator('[data-testid="dataset-name"]');
    this.schemaTab = page.locator('[data-testid="schema-tab"]');
    this.lineageTab = page.locator('[data-testid="lineage-tab"]');
    this.propertiesTab = page.locator('[data-testid="properties-tab"]');

    this.sidebarGlossarySection = page.locator('#entity-profile-glossary-terms');
    this.addTermsButton = this.sidebarGlossarySection.locator('[data-testid="add-terms-button"]');
    // AntD Select renders a div wrapper; target the inner search input for typing.
    this.tagTermModalInput = page.locator('[data-testid="tag-term-modal-input"] input');
    this.tagTermOption = page.locator('[data-testid="tag-term-option"]').first();
    this.addTagTermConfirmButton = page.locator('[data-testid="add-tag-term-from-modal-btn"]');
  }

  async navigateToDataset(urn: string): Promise<void> {
    await this.navigate(`/dataset/${encodeURIComponent(urn)}`);
    await this.page.waitForLoadState('networkidle');
  }

  getGlossaryTermLocator(termName: string): Locator {
    return this.page.locator(`[data-testid="term-${termName}"]`);
  }

  getGlossaryTermRemoveButtonLocator(glossaryTermLocator: Locator): Locator {
    return glossaryTermLocator.locator('[data-testid="remove-icon"]');
  }

  async addGlossaryTerm(termName: string): Promise<void> {
    this.logger?.step('addGlossaryTerm', { termName });
    await this.addTermsButton.click();
    await expect(this.tagTermModalInput).toBeVisible();
    // AntD Select triggers search via keyboard events; pressSequentially (not fill) is required.
    await this.tagTermModalInput.pressSequentially(termName);
    await this.tagTermOption.click();
    // Click the header to close the terms select dropdown if it stayed open.
    await this.modalComponent.title.click();
    await this.addTagTermConfirmButton.click();
    await expect(this.addTagTermConfirmButton).toBeHidden();

    await this.page.waitForLoadState('networkidle');
  }

  async removeGlossaryTerm(termName: string): Promise<void> {
    this.logger?.step('removeGlossaryTerm', { termName });

    // Click remove button on glossary terms pill
    const glossaryTerm = this.getGlossaryTermLocator(termName);
    await glossaryTerm.scrollIntoViewIfNeeded();
    await glossaryTerm.hover();
    const termRemoveButton = this.getGlossaryTermRemoveButtonLocator(glossaryTerm);
    await expect(termRemoveButton).toBeVisible();
    await termRemoveButton.click();

    // Confirm deletion
    await this.confirmationComponent.waitForOpening();
    await this.confirmationComponent.expectTitleContainText(termName);
    await this.confirmationComponent.confirm();

    await this.page.waitForLoadState('networkidle');
  }

  async expectGlossaryTermVisible(termName: string): Promise<void> {
    const glossaryTerm = this.getGlossaryTermLocator(termName);
    await expect(glossaryTerm).toBeVisible();
  }

  async expectGlossaryTermNotVisible(termName: string): Promise<void> {
    const glossaryTerm = this.getGlossaryTermLocator(termName);
    await expect(glossaryTerm).toBeHidden();
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
    return (await this.datasetName.textContent()) || '';
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
    // The ant-drawer-body intercepts pointer events so .hover() fails.
    // Scroll the section into view and dispatch a mouseover event to reveal the "Add Attribute"
    // button (CSS :hover), then click it via the native DOM click to bypass overlay checks.
    await businessAttributeSection.evaluate((el: HTMLElement) => el.scrollIntoView({ block: 'center' }));
    await businessAttributeSection.dispatchEvent('mouseover');
    const addButton = businessAttributeSection.locator('text=Add Attribute');
    // If the attribute is already set (e.g. from seed data), "Add Attribute" won't be present — skip.
    if ((await addButton.count()) === 0) return;
    await addButton.evaluate((el: HTMLElement) => el.click());

    // The Select component requires typing into the inner search input, not fill() on the outer wrapper.
    const modalSearchInput = this.page
      .locator('[data-testid="business-attribute-modal-input"]')
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
    await expect(doneBtn).toBeHidden();

    await expect(businessAttributeSection.getByText(attributeName)).toBeVisible();
  }

  getBusinessAttributeSection(_fieldName: string): Locator {
    // In the V2 entity view, the schema field drawer renders a SidebarSection with
    // data-testid="sidebar-section-content-Business Attribute" for the business attribute content.
    return this.page.locator('[data-testid="sidebar-section-content-Business Attribute"]');
  }

  async removeBusinessAttributeFromField(fieldName: string, attributeName: string): Promise<void> {
    const businessAttributeSection = this.getBusinessAttributeSection(fieldName);

    // In V2 the close icon may be an img element; use [aria-label="close"] to match both
    // the old span[aria-label=close] (Ant Design v4) and new img[aria-label="close"] (V2).
    const closeIcon = businessAttributeSection.locator('[aria-label="close"]').first();
    // Use evaluate to click directly, bypassing viewport and pointer-event intercept checks.
    await closeIcon.evaluate((el: HTMLElement) => {
      el.scrollIntoView({ block: 'center' });
      el.click();
    });

    // Scope the confirmation button to the dialog to avoid matching "Yes" text elsewhere.
    await this.page.getByRole('dialog').getByRole('button', { name: 'Yes' }).waitFor({ state: 'visible' });
    await this.page.getByRole('dialog').getByRole('button', { name: 'Yes' }).click({ force: true });
    await this.page.waitForLoadState('networkidle');

    await expect(businessAttributeSection.getByText(attributeName)).not.toBeVisible({ timeout: 15000 });
  }

  async expectBusinessAttributeOnField(fieldName: string, attributeName: string): Promise<void> {
    const businessAttributeSection = this.getBusinessAttributeSection(fieldName);
    await expect(businessAttributeSection.getByText(attributeName)).toBeVisible();
  }

  async expectBusinessAttributeNotOnField(fieldName: string, attributeName: string): Promise<void> {
    const businessAttributeSection = this.getBusinessAttributeSection(fieldName);
    await expect(businessAttributeSection.getByText(attributeName)).toBeHidden();
  }

  async expectTagOnFieldFromAttribute(fieldName: string, tagName: string): Promise<void> {
    await this.clickSchemaField(fieldName);
    await expect(this.page.getByText(tagName)).toBeVisible();
  }

  async expectTermOnFieldFromAttribute(fieldName: string, termName: string): Promise<void> {
    await this.clickSchemaField(fieldName);
    await expect(this.page.getByText(termName)).toBeVisible();
  }

  // ── Ownership helpers (V1 UI) ─────────────────────────────────────────────

  async addOwner(owner: string, ownerType: string): Promise<void> {
    await this.page.locator('[data-testid="add-owners-button"]').first().click({ force: true });
    await expect(this.page.locator('[data-testid="add-owners-select"]')).toBeVisible({ timeout: 10000 });
    await this.page.locator('[data-testid="add-owners-select-base"]').click({ force: true });
    await this.page.locator('[data-testid="dropdown-search-input"]').fill(owner);
    await this.page.locator('[data-testid="add-owners-select-dropdown"]').getByText(owner).click({ force: true });
    await expect(this.page.getByText(owner)).toBeVisible();
    await this.page.getByRole('dialog').getByText('Technical Owner').click();
    await this.page.getByRole('listbox').locator('..').getByText(ownerType).click();
    await expect(this.page.getByRole('dialog').getByText(ownerType)).toBeVisible();
    await this.page.getByText('Done').click();
    await expect(this.page.getByText('Owners Added')).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(ownerType)).toBeVisible();
    await expect(this.page.getByText(owner)).toBeVisible();
  }

  // elementId is a dynamic selector (e.g. href targeting a specific user/group URN)
  // so it is passed in from the test rather than hardcoded here.
  async removeOwner(owner: string, elementId: string): Promise<void> {
    await this.page.locator(elementId).locator('xpath=following-sibling::*[1]').click();
    await this.page.getByText('Yes').click();
    await expect(this.page.getByText('Owner Removed')).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(owner)).not.toBeVisible({ timeout: 10000 });
  }
}
