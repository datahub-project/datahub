import { Page, Locator, expect } from '@playwright/test';
import { BaseEntityPage } from '../base-entity.page';
import type { DataHubLogger } from '../../../utils/logger';
import { TIMEOUTS, LOAD_STATES } from '../../../utils/constants';

export class SummaryTabPage extends BaseEntityPage {
  // Properties section
  readonly propertiesSection: Locator;
  readonly getProperty: (type: string) => Locator;
  readonly getPropertyTitle: (type: string) => Locator;
  readonly getPropertyValue: (type: string) => Locator;

  // About section
  readonly aboutSection: Locator;
  readonly editDescriptionButton: Locator;
  readonly descriptionEditor: Locator;
  readonly descriptionEditorInput: Locator;
  readonly publishButton: Locator;
  readonly descriptionViewer: Locator;
  readonly addRelatedButton: Locator;

  // Link management
  readonly urlInput: Locator;
  readonly labelInput: Locator;
  readonly linkFormSubmitButton: Locator;
  readonly addLinkMenuItem: Locator;
  readonly modalConfirmButton: Locator;
  readonly getLink: (url: string, label: string) => Locator;
  readonly getLinkEditButton: (url: string, label: string) => Locator;
  readonly getLinkRemoveButton: (url: string, label: string) => Locator;

  // Navigation
  readonly summaryTabHeader: Locator;

  // Template/Modules section
  readonly templateWrapper: Locator;
  readonly getModule: (type: string) => Locator;
  readonly getModuleContent: (type: string) => Locator;
  readonly assetsModule: Locator;
  readonly hierarchyModule: Locator;
  readonly relatedTermsModule: Locator;

  // Owner validation
  readonly getOwnerElement: (ownerUrn: string) => Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    this.summaryTabHeader = page.getByTestId('Summary-entity-tab-header');

    this.propertiesSection = page.getByTestId('properties-section');
    this.getProperty = (type: string) => page.getByTestId(`property-${type}`);
    this.getPropertyTitle = (type: string) => this.getProperty(type).getByTestId('property-title');
    this.getPropertyValue = (type: string) => this.getProperty(type).getByTestId('property-value');

    this.aboutSection = page.getByTestId('about-section');
    this.editDescriptionButton = page.getByTestId('edit-description-button');
    this.descriptionEditor = page.getByTestId('description-editor');
    this.descriptionEditorInput = this.descriptionEditor.getByRole('textbox', { includeHidden: false });
    this.publishButton = page.getByTestId('publish-button');
    this.descriptionViewer = page.getByTestId('description-viewer');
    this.addRelatedButton = page.getByTestId('add-related-button');

    this.urlInput = page.getByTestId('url-input');
    this.labelInput = page.getByTestId('label-input');
    this.linkFormSubmitButton = page.getByTestId('link-form-modal-submit-button');
    this.addLinkMenuItem = page.getByTestId('menu-item-add-link');
    this.modalConfirmButton = page.getByTestId('modal-confirm-button');
    this.getLink = (url: string, label: string) => this.aboutSection.getByTestId(`${url}-${label}`);
    this.getLinkEditButton = (url: string, label: string) => this.getLink(url, label).getByTestId('edit-link-button');
    this.getLinkRemoveButton = (url: string, label: string) =>
      this.getLink(url, label).getByTestId('remove-link-button');

    this.templateWrapper = page.getByTestId('template-wrapper');
    this.getModule = (type: string) => page.getByTestId(`${type}-module`);
    this.getModuleContent = (type: string) => this.getModule(type).getByTestId('module-content');
    this.assetsModule = this.getModule('assets');
    this.hierarchyModule = this.getModule('hierarchy');
    this.relatedTermsModule = this.getModule('related-terms');

    this.getOwnerElement = (ownerUrn: string) => this.propertiesSection.getByTestId(`owner-${ownerUrn}`);
  }

  async ensurePropertiesSectionVisible(): Promise<void> {
    await expect(this.propertiesSection).toBeVisible();
  }

  async expectPropertyExists(type: string, expectedValue?: string): Promise<void> {
    const property = this.getProperty(type);
    await expect(property).toBeVisible();
    await expect(this.getPropertyTitle(type)).toBeVisible();

    if (expectedValue) {
      await expect(this.getPropertyValue(type)).toContainText(expectedValue, { ignoreCase: true });
    }
  }

  async expectPropertyExistsWithValue(
    type: string,
    expectedValue: string,
    timeout: number = TIMEOUTS.MEDIUM,
  ): Promise<void> {
    const property = this.getProperty(type);
    await expect(property).toBeVisible({ timeout });
    await expect(this.getPropertyTitle(type)).toBeVisible({ timeout });
    await expect(this.getPropertyValue(type)).toContainText(expectedValue, { ignoreCase: true, timeout });
  }

  async ensureAboutSectionVisible(): Promise<void> {
    await expect(this.aboutSection).toBeVisible();
  }

  async updateDescription(description: string, timeout: number = TIMEOUTS.MEDIUM): Promise<void> {
    await expect(this.editDescriptionButton).toBeVisible({ timeout });
    await this.editDescriptionButton.click();

    await expect(this.descriptionEditorInput).toBeVisible({ timeout });
    await this.descriptionEditorInput.clear();
    await this.descriptionEditorInput.fill(description);

    await expect(this.publishButton).toBeVisible({ timeout });
    await this.publishButton.click();
    await expect(this.publishButton).not.toBeAttached({ timeout });
  }

  async expectDescriptionContains(text: string): Promise<void> {
    await expect(this.descriptionViewer).toContainText(text);
  }

  async addLink(url: string, label: string, timeout: number = TIMEOUTS.MEDIUM): Promise<void> {
    await expect(this.addRelatedButton).toBeVisible({ timeout });
    await this.addRelatedButton.click();

    await expect(this.addLinkMenuItem).toBeVisible({ timeout });
    await this.addLinkMenuItem.click();

    await expect(this.urlInput).toBeVisible({ timeout });
    await this.urlInput.clear();
    await this.urlInput.fill(url);

    await expect(this.labelInput).toBeVisible({ timeout });
    await this.labelInput.clear();
    await this.labelInput.fill(label);

    await expect(this.linkFormSubmitButton).toBeVisible({ timeout });
    await this.linkFormSubmitButton.click();
    await expect(this.linkFormSubmitButton).not.toBeAttached({ timeout });
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async updateLink(
    oldUrl: string,
    oldLabel: string,
    newUrl: string,
    newLabel: string,
    timeout: number = TIMEOUTS.MEDIUM,
  ): Promise<void> {
    const editButton = this.getLinkEditButton(oldUrl, oldLabel);
    await expect(editButton).toBeVisible({ timeout });
    await editButton.click();

    await expect(this.urlInput).toBeVisible({ timeout });
    await this.urlInput.clear();
    await this.urlInput.fill(newUrl);

    await expect(this.labelInput).toBeVisible({ timeout });
    await this.labelInput.clear();
    await this.labelInput.fill(newLabel);

    await expect(this.linkFormSubmitButton).toBeVisible({ timeout });
    await this.linkFormSubmitButton.click();
    await expect(this.linkFormSubmitButton).not.toBeAttached({ timeout });
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

    await this.page.reload();
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
    await expect(this.getLink(newUrl, newLabel)).toBeVisible({ timeout });
  }

  async removeLink(url: string, label: string, timeout: number = TIMEOUTS.MEDIUM): Promise<void> {
    const removeButton = this.getLinkRemoveButton(url, label);
    await expect(removeButton).toBeVisible({ timeout });
    await removeButton.click();

    await expect(this.modalConfirmButton).toBeVisible({ timeout });
    await this.modalConfirmButton.click();
    await expect(this.getLink(url, label)).not.toBeAttached({ timeout });
    await this.page.waitForLoadState(LOAD_STATES.NETWORKIDLE);
  }

  async expectLinkExists(url: string, label: string, timeout: number = TIMEOUTS.MEDIUM): Promise<void> {
    const link = this.getLink(url, label);
    await expect(link).toBeVisible({ timeout });
    await expect(link).toContainText(label);
  }

  async expectLinkNotExists(url: string, label: string, timeout: number = TIMEOUTS.SHORT): Promise<void> {
    await expect(this.getLink(url, label)).not.toBeAttached({ timeout });
  }

  async ensureTemplateSectionVisible(): Promise<void> {
    await expect(this.templateWrapper).toBeVisible();
  }

  async expectModuleContentContains(
    type: string,
    expectedValue: string,
    timeout: number = TIMEOUTS.MEDIUM,
  ): Promise<void> {
    const moduleContent = this.getModuleContent(type);
    await expect(moduleContent).toContainText(expectedValue, { timeout });
  }

  async expectOwnerExists(ownerUrn: string, timeout: number = TIMEOUTS.MEDIUM): Promise<void> {
    const ownerElement = this.getOwnerElement(ownerUrn);
    await expect(ownerElement).toBeVisible({ timeout });
  }
}
