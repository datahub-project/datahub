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
    await expect(this.page.locator('[data-testid="properties-section"]')).toBeVisible();
  }

  async expectPropertyVisible(type: string): Promise<void> {
    await expect(this.page.locator(`[data-testid="property-${type}"]`)).toBeVisible();
  }

  async expectPropertyContains(type: string, name: string, value?: string, dataTestId?: string): Promise<void> {
    const container = this.page.locator(`[data-testid="property-${type}"]`);
    await expect(container.locator('[data-testid="property-title"]')).toContainText(name);
    if (value !== undefined) {
      const valueEl = container.locator('[data-testid="property-value"]');
      await expect(valueEl).toBeVisible();
      const text = await valueEl.textContent();
      expect(text?.toLowerCase()).toMatch(new RegExp(value, 'i'));
    }
    if (dataTestId !== undefined) {
      await expect(container.locator(`[data-testid="${dataTestId}"]`)).toBeAttached();
    }
  }

  async expectPropertyNotVisible(type: string): Promise<void> {
    await expect(this.page.locator(`[data-testid="property-${type}"]`)).not.toBeAttached();
  }
}

export class TemplateSectionComponent {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  async expectVisible(): Promise<void> {
    await expect(this.page.locator('[data-testid="template-wrapper"]')).toBeVisible();
  }

  async expectModuleExists(type: string, name: string, value?: string): Promise<void> {
    const module = this.page
      .locator(`[data-testid="${type}-module"]`)
      .filter({ hasText: name });
    await module.scrollIntoViewIfNeeded();
    await expect(module.locator('[data-testid="large-module-drag-handle"]')).toContainText(name);
    if (value !== undefined) {
      await expect(module.locator('[data-testid="module-content"]')).toContainText(value);
    }
  }

  async expectModuleNotExists(type: string, name: string): Promise<void> {
    await expect(
      this.page.locator(`[data-testid="${type}-module"]`).filter({ hasText: name }),
    ).not.toBeAttached();
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
    await this.page.locator('[data-testid="Summary-entity-tab-header"]').click();
    await this.page.waitForLoadState('networkidle');
  }

  // ── About section ──────────────────────────────────────────────────────────

  async expectAboutSectionVisible(): Promise<void> {
    await expect(this.page.locator('[data-testid="about-section"]')).toBeVisible();
  }

  async updateDescription(text: string): Promise<void> {
    await this.page.locator('[data-testid="edit-description-button"]').click();
    await this.page
      .locator('[data-testid="description-editor"]')
      .locator('.remirror-editor')
      .clear();
    await this.page
      .locator('[data-testid="description-editor"]')
      .locator('.remirror-editor')
      .type(text);
    await this.page.locator('[data-testid="publish-button"]').click();
    // Wait for the modal to close — publish button disappears
    await expect(this.page.locator('[data-testid="publish-button"]')).not.toBeAttached({ timeout: 10000 });
  }

  async expectDescriptionContains(text: string): Promise<void> {
    await expect(this.page.locator('[data-testid="description-viewer"]')).toContainText(text);
  }

  // ── Link management ────────────────────────────────────────────────────────

  async addLink(url: string, label: string): Promise<void> {
    await this.page.locator('[data-testid="add-related-button"]').click();
    await this.page.waitForTimeout(500);
    await this.page.getByText('Add link').click();
    await this.page.waitForTimeout(500);
    await this.page.locator('[data-testid="url-input"]').clear();
    await this.page.locator('[data-testid="url-input"]').fill(url);
    await this.page.locator('[data-testid="label-input"]').clear();
    await this.page.locator('[data-testid="label-input"]').fill(label);
    await this.page.locator('[data-testid="link-form-modal-submit-button"]').click();
  }

  async updateLink(prevUrl: string, prevLabel: string, url: string, label: string): Promise<void> {
    await this.page.locator(`[data-testid="${prevUrl}-${prevLabel}"]`).locator('[data-testid="edit-link-button"]').click();
    await this.page.locator('[data-testid="url-input"]').clear();
    await this.page.locator('[data-testid="url-input"]').fill(url);
    await this.page.locator('[data-testid="label-input"]').clear();
    await this.page.locator('[data-testid="label-input"]').fill(label);
    await this.page.locator('[data-testid="link-form-modal-submit-button"]').click();
  }

  async removeLink(url: string, label: string): Promise<void> {
    await this.page
      .locator(`[data-testid="${url}-${label}"]`)
      .locator('[data-testid="remove-link-button"]')
      .click();
    await this.page.locator('[data-testid="modal-confirm-button"]').click();
  }

  async expectLinkExists(url: string, label: string): Promise<void> {
    const linkEl = this.page
      .locator('[data-testid="about-section"]')
      .locator(`[data-testid="${url}-${label}"]`);
    await expect(linkEl).toBeVisible();
    await expect(linkEl).toContainText(label);
  }

  expectLinkNotExists(url: string, label: string): Locator {
    return this.page
      .locator('[data-testid="about-section"]')
      .locator(`[data-testid="${url}-${label}"]`);
  }
}
