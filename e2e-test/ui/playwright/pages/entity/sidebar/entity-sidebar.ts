/**
 * EntitySidebar — component model for the right-hand sidebar panel.
 *
 * All sections extend BaseSidebarSection. Access sections via typed properties.
 */

import { Page, Locator, expect } from '@playwright/test';
import { TagsSection } from './sections/tags.section';
import { GlossaryTermsSection } from './sections/glossary-terms.section';
import { OwnersSection } from './sections/owners.section';
import { DomainSection } from './sections/domain.section';

export class EntitySidebar {
  private readonly page: Page;

  readonly tags: TagsSection;
  readonly glossaryTerms: GlossaryTermsSection;
  readonly owners: OwnersSection;
  readonly domain: DomainSection;

  private readonly sidebarContainer: Locator;
  private readonly collapseTab: Locator;
  private readonly summaryTab: Locator;
  private readonly propertiesTab: Locator;

  constructor(page: Page) {
    this.page = page;
    this.tags = new TagsSection(page);
    this.glossaryTerms = new GlossaryTermsSection(page);
    this.owners = new OwnersSection(page);
    this.domain = new DomainSection(page);

    this.sidebarContainer = page.locator('#entity-profile-sidebar');
    this.collapseTab = page.locator('#entity-sidebar-tabs-tab-collapse');
    this.summaryTab = page.locator('#entity-sidebar-tabs-tab-Summary');
    this.propertiesTab = page.locator('#entity-sidebar-tabs-tab-Properties');
  }

  async open(): Promise<void> {
    const expanded = await this.sidebarContainer.getAttribute('aria-expanded');
    if (expanded !== 'true') {
      await this.collapseTab.click();
    }
  }

  async openSummaryTab(): Promise<void> {
    await this.open();
    const selected = await this.summaryTab.getAttribute('aria-selected');
    if (selected !== 'true') {
      await this.summaryTab.click();
    }
  }

  async openPropertiesTab(): Promise<void> {
    await this.open();
    await this.propertiesTab.click({ force: true });
  }

  async expectVisible(): Promise<void> {
    await expect(this.sidebarContainer).toBeVisible();
  }
}
