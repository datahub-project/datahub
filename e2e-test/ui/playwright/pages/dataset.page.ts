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
  readonly tagTermOption: Locator;
  readonly addTagTermConfirmButton: Locator;

  // ── Sidebar Tags section ──────────────────────────────────────────────────
  readonly tagsSectionContainer: Locator;
  readonly addTagsButton: Locator;
  readonly tagTermModalInput: Locator;
  readonly tagTermInput: Locator;
  readonly addTagFromModalButton: Locator;
  readonly tagUnassignConfirmButton: Locator;
  readonly tagAddedToast: Locator;
  readonly tagRemovedToast: Locator;

  // ── Close ─────────────────────────────────────────────────────────────────
  readonly modalCloseButton: Locator;

  // ── Ownership ────────────────────────────────────────────────────────────
  readonly addOwnersButton: Locator;
  readonly addOwnersSelect: Locator;
  readonly addOwnersSelectBase: Locator;
  readonly ownerDropdownSearchInput: Locator;
  readonly addOwnersSelectDropdown: Locator;
  readonly ownersDoneButton: Locator;
  readonly ownersAddedToast: Locator;
  readonly ownerRemovedToast: Locator;
  readonly ownerRemoveConfirmButton: Locator;

  // ── Business Attribute ────────────────────────────────────────────────────
  readonly businessAttributeSection: Locator;
  readonly businessAttributeModalInput: Locator;
  readonly businessAttributeOption: Locator;
  readonly addAttributeFromModalBtn: Locator;

  // ── Schema Field Drawer ────────────────────────────────────────────────────
  readonly fieldDrawer: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.modalComponent = new ModalComponent(page);
    this.confirmationComponent = new ConfirmationModalComponent(page);

    this.datasetName = page.getByTestId('dataset-name');
    this.schemaTab = page.getByTestId('schema-tab');
    this.lineageTab = page.getByTestId('lineage-tab');
    this.propertiesTab = page.getByTestId('Properties-entity-tab-header');

    // eslint-disable-next-line playwright/no-raw-locators -- HTML id set by EntityProfileCard; no data-testid equivalent
    this.sidebarGlossarySection = page.locator('#entity-profile-glossary-terms');
    this.addTermsButton = this.sidebarGlossarySection.getByTestId('add-terms-button');
    this.tagTermOption = page.getByTestId('tag-term-option').first();
    this.addTagTermConfirmButton = page.getByTestId('add-tag-term-from-modal-btn');

    // eslint-disable-next-line playwright/no-raw-locators -- HTML id set by EntityProfileCard; no data-testid equivalent
    this.tagsSectionContainer = page.locator('#entity-profile-tags');
    this.addTagsButton = this.tagsSectionContainer.getByTestId('add-tags-button');
    this.tagTermModalInput = page.getByTestId('tag-term-modal-input');
    // AntD Select renders its inner input with role="combobox", not "textbox".
    this.tagTermInput = this.tagTermModalInput.getByRole('combobox');
    this.addTagFromModalButton = page.getByTestId('add-tag-term-from-modal-btn');
    this.tagUnassignConfirmButton = page.getByTestId('modal-confirm-button');
    this.tagAddedToast = page.getByText('Added Tags!');
    this.tagRemovedToast = page.getByText('Removed Tag!');

    this.modalCloseButton = page.getByRole('button', { name: 'Close' });

    this.addOwnersButton = page.getByTestId('add-owners-button').first();
    this.addOwnersSelect = page.getByTestId('add-owners-select');
    this.addOwnersSelectBase = page.getByTestId('add-owners-select-base');
    this.ownerDropdownSearchInput = page.getByTestId('dropdown-search-input');
    this.addOwnersSelectDropdown = page.getByTestId('add-owners-select-dropdown');
    this.ownersDoneButton = page.getByRole('dialog').getByText('Done');
    this.ownersAddedToast = page.getByText('Owners Added');
    this.ownerRemovedToast = page.getByText('Owner Removed');
    this.ownerRemoveConfirmButton = page.getByRole('dialog').getByText('Yes');

    this.businessAttributeSection = page.getByTestId('sidebar-section-content-Business Attribute');
    this.businessAttributeModalInput = page.getByTestId('business-attribute-modal-input');
    this.businessAttributeOption = page.getByTestId('business-attribute-option');
    this.addAttributeFromModalBtn = page.getByTestId('add-attribute-from-modal-btn');

    this.fieldDrawer = page.getByTestId('schema-field-drawer-content');
  }

  // ── Dynamic locators ──────────────────────────────────────────────────────

  getTagChip(tagName: string): Locator {
    return this.page.getByTestId(`tag-${tagName}`);
  }

  getTagOption(tagName: string): Locator {
    // eslint-disable-next-line playwright/no-raw-locators -- AntD Select option uses name attribute; no semantic equivalent
    return this.page.locator(`[name="${tagName}"]`);
  }

  getTagRemoveIcon(tagName: string): Locator {
    return this.getTagChip(tagName).getByTestId('remove-icon');
  }

  // ── Navigation ────────────────────────────────────────────────────────────

  async navigateToDataset(urn: string): Promise<void> {
    await this.navigate(`/dataset/${encodeURIComponent(urn)}`);
    await this.waitForPageLoad();
  }

  getGlossaryTermLocator(termName: string): Locator {
    return this.page.getByTestId(`term-${termName}`);
  }

  getGlossaryTermRemoveButtonLocator(glossaryTermLocator: Locator): Locator {
    return glossaryTermLocator.getByTestId('remove-icon');
  }

  async addGlossaryTerm(termName: string): Promise<void> {
    this.logger?.step('addGlossaryTerm', { termName });
    await this.addTermsButton.click();
    await expect(this.tagTermModalInput).toBeVisible();
    // AntD Select triggers search via keyboard events; pressSequentially (not fill) is required.
    await this.tagTermInput.pressSequentially(termName);
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

  async viewProperties(): Promise<void> {
    await this.propertiesTab.click();
    await this.waitForPageLoad();
  }

  async getDatasetName(): Promise<string> {
    return (await this.datasetName.textContent()) || '';
  }

  async closeAnyOpenModal(): Promise<void> {
    if (await this.modalCloseButton.isVisible()) {
      await this.modalCloseButton.click();
    }
  }

  async clickSchemaFieldByName(fieldName: string): Promise<void> {
    this.logger?.step('clickSchemaFieldByName', { fieldName });
    await this.page.getByText(fieldName, { exact: true }).click();
  }

  async waitForFieldDrawer(timeout: number = 15000): Promise<void> {
    this.logger?.step('waitForFieldDrawer');
    await this.fieldDrawer.waitFor({ state: 'visible', timeout });
  }

  async clickSchemaField(fieldName: string): Promise<void> {
    // Use the row's id attribute (set by SchemaTable's onRow handler) so the click
    // reliably hits the row element and triggers the SchemaFieldDrawer to open.
    // eslint-disable-next-line playwright/no-raw-locators -- SchemaTable sets id="column-<name>" on rows; no data-testid equivalent
    await this.page.locator(`#column-${fieldName}`).click();
  }

  async addBusinessAttributeToField(fieldName: string, attributeName: string): Promise<void> {
    await this.clickSchemaField(fieldName);

    // The V2 schema field drawer renders the business attribute section inside SidebarSection.
    // SidebarSection sets data-testid="sidebar-section-content-{title}" on its content container.
    await this.businessAttributeSection.waitFor({ state: 'visible', timeout: 15000 });
    // The ant-drawer-body intercepts pointer events so .hover() fails.
    // Scroll the section into view and dispatch a mouseover event to reveal the "Add Attribute"
    // button (CSS :hover), then click it via the native DOM click to bypass overlay checks.
    await this.businessAttributeSection.evaluate((el: HTMLElement) => el.scrollIntoView({ block: 'center' }));
    await this.businessAttributeSection.dispatchEvent('mouseover');
    const addButton = this.businessAttributeSection.getByText('Add Attribute');
    // If the attribute is already set (e.g. from seed data), "Add Attribute" won't be present — skip.
    if ((await addButton.count()) === 0) return;
    await addButton.evaluate((el: HTMLElement) => el.click());

    // The Select component requires typing into the inner search input, not fill() on the outer wrapper.
    // eslint-disable-next-line playwright/no-raw-locators -- AntD Select internal search input; no semantic equivalent
    const modalSearchInput = this.businessAttributeModalInput.locator('.ant-select-selection-search-input');
    await modalSearchInput.fill(attributeName);

    // Wait for options to appear, then use ArrowDown + Enter to select without depending on click position.
    await this.businessAttributeOption.first().waitFor({ state: 'visible' });
    await modalSearchInput.press('ArrowDown');
    await modalSearchInput.press('Enter');

    // Once an option is selected the Done button becomes enabled.
    // Use evaluate+click() to trigger the React onClick via the native DOM click method,
    // bypassing the Playwright actionability check that fails due to the modal body overlay.
    await this.addAttributeFromModalBtn.waitFor({ state: 'visible' });
    await expect(this.addAttributeFromModalBtn).toBeEnabled();
    await this.addAttributeFromModalBtn.evaluate((el: HTMLElement) => el.click());
    await expect(this.addAttributeFromModalBtn).toBeHidden();

    await expect(this.businessAttributeSection.getByText(attributeName)).toBeVisible();
  }

  async removeBusinessAttributeFromField(fieldName: string, attributeName: string): Promise<void> {
    // In V2 the close icon may be an img element; use [aria-label="close"] to match both
    // the old span[aria-label=close] (Ant Design v4) and new img[aria-label="close"] (V2).
    const closeIcon = this.businessAttributeSection.getByLabel('close').first();
    // Use evaluate to click directly, bypassing viewport and pointer-event intercept checks.
    await closeIcon.evaluate((el: HTMLElement) => {
      el.scrollIntoView({ block: 'center' });
      el.click();
    });

    // Scope the confirmation button to the dialog to avoid matching "Yes" text elsewhere.
    await this.page.getByRole('dialog').getByRole('button', { name: 'Yes' }).waitFor({ state: 'visible' });
    await this.page.getByRole('dialog').getByRole('button', { name: 'Yes' }).click({ force: true });
    await this.page.waitForLoadState('networkidle');

    await expect(this.businessAttributeSection.getByText(attributeName)).not.toBeVisible({ timeout: 15000 });
  }

  async expectBusinessAttributeOnField(_fieldName: string, attributeName: string): Promise<void> {
    await expect(this.businessAttributeSection.getByText(attributeName)).toBeVisible();
  }

  async expectBusinessAttributeNotOnField(_fieldName: string, attributeName: string): Promise<void> {
    await expect(this.businessAttributeSection.getByText(attributeName)).toBeHidden();
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
    await this.addOwnersButton.click({ force: true });
    await expect(this.addOwnersSelect).toBeVisible({ timeout: 10000 });
    await this.addOwnersSelectBase.click({ force: true });
    await this.ownerDropdownSearchInput.fill(owner);
    await this.addOwnersSelectDropdown.getByText(owner).click({ force: true });
    await expect(this.page.getByText(owner)).toBeVisible();
    await this.page.getByRole('dialog').getByText('Technical Owner').click();
    // eslint-disable-next-line playwright/no-raw-locators -- XPath parent traversal (..) has no semantic Playwright equivalent
    await this.page.getByRole('listbox').locator('..').getByText(ownerType).click();
    await expect(this.page.getByRole('dialog').getByText(ownerType)).toBeVisible();
    await this.ownersDoneButton.click();
    await expect(this.ownersAddedToast).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(ownerType)).toBeVisible();
    await expect(this.page.getByText(owner)).toBeVisible();
  }

  // elementId is a dynamic selector (e.g. href targeting a specific user/group URN)
  // so it is passed in from the test rather than hardcoded here.
  async removeOwner(owner: string, elementId: string): Promise<void> {
    // eslint-disable-next-line playwright/no-raw-locators -- dynamic href/id selector passed from test; no semantic equivalent
    await this.page.locator(elementId).locator('xpath=following-sibling::*[1]').click();
    await this.ownerRemoveConfirmButton.click();
    await expect(this.ownerRemovedToast).toBeVisible({ timeout: 15000 });
    await expect(this.page.getByText(owner)).not.toBeVisible({ timeout: 10000 });
  }

  // ── Tags ──────────────────────────────────────────────────────────────────

  async assignTag(tagName: string): Promise<void> {
    this.logger?.step('assignTag', { tagName });
    await expect(this.addTagsButton).toBeVisible();
    await expect(this.addTagsButton).toBeEnabled();
    await this.addTagsButton.click();

    await expect(this.tagTermModalInput).toBeVisible();
    await this.tagTermInput.focus();
    await this.tagTermInput.fill(tagName);

    const tagOption = this.getTagOption(tagName);
    await tagOption.waitFor({ state: 'visible' });
    await tagOption.click();

    await this.page.keyboard.press('Escape');

    await expect(this.addTagFromModalButton).toBeEnabled();
    await this.addTagFromModalButton.click();

    await expect(this.tagAddedToast).toBeVisible();
  }

  async unassignTag(tagName: string): Promise<void> {
    this.logger?.step('unassignTag', { tagName });
    await expect(this.getTagChip(tagName)).toBeVisible();
    await this.getTagRemoveIcon(tagName).click();
    await expect(this.tagUnassignConfirmButton).toBeVisible();
    await this.tagUnassignConfirmButton.click();
    await expect(this.tagRemovedToast).toBeVisible();
  }

  async expectTagAssigned(tagName: string): Promise<void> {
    this.logger?.step('expectTagAssigned', { tagName });
    await expect(this.getTagChip(tagName)).toBeVisible();
  }

  async expectTagNotAssigned(tagName: string): Promise<void> {
    this.logger?.step('expectTagNotAssigned', { tagName });
    await expect(this.getTagChip(tagName)).toBeHidden();
  }
}
