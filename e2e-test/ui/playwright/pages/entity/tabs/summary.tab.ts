/**
 * SummaryTab — manages the Summary tab on entity pages.
 *
 * Covers: description editing, link CRUD, properties section, and template modules.
 */

import { Page, Locator, expect } from '@playwright/test';
import type { Tab } from './tab.interface';

export class SummaryPropertiesSection {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  async expectVisible(): Promise<void> {
    await expect(this.page.getByTestId('properties-section')).toBeVisible();
  }

  async expectPropertyVisible(type: string): Promise<void> {
    await expect(this.page.getByTestId(`property-${type}`)).toBeVisible();
  }

  async expectPropertyContains(type: string, name: string, value?: string, dataTestId?: string): Promise<void> {
    const container = this.page.getByTestId(`property-${type}`);
    await expect(container.getByTestId('property-title')).toContainText(name);
    if (value !== undefined) {
      const valueEl = container.getByTestId('property-value');
      await expect(valueEl).toBeVisible();
      const text = await valueEl.textContent();
      expect(text?.toLowerCase()).toMatch(new RegExp(value, 'i'));
    }
    if (dataTestId !== undefined) {
      await expect(container.getByTestId(dataTestId)).toBeAttached();
    }
  }

  async expectPropertyNotVisible(type: string): Promise<void> {
    await expect(this.page.getByTestId(`property-${type}`)).not.toBeAttached();
  }
}

export class TemplateSectionComponent {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  async expectVisible(): Promise<void> {
    await expect(this.page.getByTestId('template-wrapper')).toBeVisible();
  }

  async expectModuleExists(type: string, name: string, value?: string): Promise<void> {
    const module = this.page.getByTestId(`${type}-module`).filter({ hasText: name });
    await module.scrollIntoViewIfNeeded();
    await expect(module.getByTestId('large-module-drag-handle')).toContainText(name);
    if (value !== undefined) {
      await expect(module.getByTestId('module-content')).toContainText(value);
    }
  }

  async expectModuleNotExists(type: string, name: string): Promise<void> {
    await expect(this.page.getByTestId(`${type}-module`).filter({ hasText: name })).not.toBeAttached();
  }
}

export class SummaryTab implements Tab {
  private readonly page: Page;
  readonly properties: SummaryPropertiesSection;
  readonly template: TemplateSectionComponent;

  constructor(page: Page) {
    this.page = page;
    this.properties = new SummaryPropertiesSection(page);
    this.template = new TemplateSectionComponent(page);
  }

  async open(): Promise<void> {
    await this.page.getByTestId('Summary-entity-tab-header').click();
    await this.page.waitForLoadState('networkidle');
  }

  // ── About section ──────────────────────────────────────────────────────────

  async expectAboutSectionVisible(): Promise<void> {
    await expect(this.page.getByTestId('about-section')).toBeVisible();
  }

  async updateDescription(text: string): Promise<void> {
    await this.page.getByTestId('edit-description-button').click();
    await this.page.getByTestId('description-editor').locator('.remirror-editor').clear();
    await this.page.getByTestId('description-editor').locator('.remirror-editor').type(text);
    await this.page.getByTestId('publish-button').click();
    // Wait for the modal to close — publish button disappears
    await expect(this.page.getByTestId('publish-button')).not.toBeAttached({ timeout: 10000 });
  }

  async expectDescriptionContains(text: string): Promise<void> {
    await expect(this.page.getByTestId('description-viewer')).toContainText(text);
  }

  // ── Link management ────────────────────────────────────────────────────────

  async addLink(url: string, label: string): Promise<void> {
    await this.page.getByTestId('add-related-button').click();
    await this.page.waitForTimeout(500);
    await this.page.getByText('Add link').click();
    await this.page.waitForTimeout(500);
    await this.page.getByTestId('url-input').clear();
    await this.page.getByTestId('url-input').fill(url);
    await this.page.getByTestId('label-input').clear();
    await this.page.getByTestId('label-input').fill(label);
    await this.page.getByTestId('link-form-modal-submit-button').click();
  }

  async updateLink(prevLabel: string, url: string, label: string): Promise<void> {
    const linkContainer = this.page
      .getByTestId('about-section')
      .locator('a', { has: this.page.getByText(prevLabel, { exact: true }) })
      .first();
    await linkContainer.getByTestId('edit-link-button').click();
    await this.page.getByTestId('url-input').clear();
    await this.page.getByTestId('url-input').fill(url);
    await this.page.getByTestId('label-input').clear();
    await this.page.getByTestId('label-input').fill(label);
    await this.page.getByTestId('link-form-modal-submit-button').click();
  }

  async removeLink(label: string): Promise<void> {
    const linkContainer = this.page
      .getByTestId('about-section')
      .locator('a', { has: this.page.getByText(label, { exact: true }) })
      .first();
    await linkContainer.getByTestId('remove-link-button').click();
    await this.page.getByTestId('modal-confirm-button').click();
  }

  async expectLinkExists(label: string): Promise<void> {
    const linkEl = this.page.getByTestId('about-section').locator('a', { hasText: label }).first();
    await expect(linkEl).toBeVisible();
    await expect(linkEl).toContainText(label);
  }

  expectLinkNotExists(label: string): Locator {
    return this.page.getByTestId('about-section').locator('a', { hasText: label }).first();
  }
}
