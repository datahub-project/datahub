import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';
import { TIMEOUTS } from '../utils/constants';

/**
 * Page object for Browse V2 sidebar functionality.
 * Handles visibility, collapse/expand, and navigation through the browse hierarchy.
 */
export class BrowseV2Page extends BasePage {
  readonly browseV2Container: Locator;
  readonly browseV2Toggle: Locator;
  readonly browseEntityLocator: (collectionName: string) => Locator;
  readonly browseNodeLocator: (nodeName: string) => Locator;
  readonly browseNodeExpandLocator: (nodeName: string) => Locator;
  readonly browsePlatformHeaderLocator: (platformName: string) => Locator;
  readonly entityHeaderBrowsePathLocator: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    // The browse sidebar container uses both id and data-testid
    this.browseV2Container = page.getByTestId('browse-v2-results');
    this.browseV2Toggle = page.getByTestId('browse-v2-toggle');
    this.browseEntityLocator = (collectionName: string) => page.getByTestId(`browse-entity-${collectionName}`);
    this.browseNodeLocator = (nodeName: string) => page.getByTestId(`browse-node-${nodeName}`);
    this.browseNodeExpandLocator = (nodeName: string) => page.getByTestId(`browse-node-expand-${nodeName}`);
    // Platform header: matches the div with testid, excluding the button element
    this.browsePlatformHeaderLocator = (platformName: string) =>
      page.getByTestId(`browse-platform-${platformName}`).filter({ hasNot: page.getByRole('button') });
    // Entity header breadcrumb area for browse paths
    this.entityHeaderBrowsePathLocator = page.getByTestId('entity-header-test-id');
  }

  async expectBrowseV2Visible(): Promise<void> {
    await expect(this.browseV2Container).toBeVisible({ timeout: TIMEOUTS.EXTRA_LONG });
  }

  async expectBrowseV2NotVisible(): Promise<void> {
    await expect(this.browseV2Container).not.toBeVisible({ timeout: TIMEOUTS.MEDIUM });
  }

  async expectSidebarExpanded(): Promise<void> {
    const width = await this.browseV2Container.evaluate((el) => window.getComputedStyle(el).width);
    // Expanded state should have width > 100px (typically 260px+)
    const widthValue = parseInt(width, 10);
    expect(widthValue).toBeGreaterThan(100);
  }

  async expectSidebarCollapsed(): Promise<void> {
    // Wait for sidebar to actually collapse (width change)
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);
    const width = await this.browseV2Container.evaluate((el) => window.getComputedStyle(el).width);
    // Collapsed state should have width ~63px
    const widthValue = parseInt(width, 10);
    expect(widthValue).toBeLessThan(100);
  }

  async toggleSidebar(): Promise<void> {
    // Ensure the button is visible before clicking
    await this.browseV2Toggle.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    // Click the toggle button
    await this.browseV2Toggle.click();
    // Wait for CSS transition animation and state change to propagate
    // The sidebar animates for 200ms, plus buffer for state updates
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);
    // Give React state a moment to update
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
  }

  async clickBrowsePlatform(platformName: string): Promise<void> {
    const platformHeader = this.browsePlatformHeaderLocator(platformName);
    await platformHeader.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await platformHeader.click({ force: true });
    await this.page.waitForTimeout(TIMEOUTS.QUICK);
  }

  async expandBrowseNode(nodeName: string): Promise<void> {
    const locator = this.browseNodeExpandLocator(nodeName);
    await locator.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await locator.click({ force: true });
    // Wait for animation and DOM updates
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);
  }

  async clickBrowseNode(nodeName: string): Promise<void> {
    const locator = this.browseNodeLocator(nodeName);
    await locator.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await locator.click({ force: true });
    await this.page.waitForTimeout(TIMEOUTS.BETWEEN_OPS);
  }

  async expectBrowseNodeVisible(nodeName: string): Promise<void> {
    await expect(this.browseNodeLocator(nodeName)).toBeVisible();
  }

  async expectBrowsePlatformVisible(platformName: string): Promise<void> {
    const platformHeader = this.browsePlatformHeaderLocator(platformName);
    await expect(platformHeader).toBeVisible();
  }

  async clickBrowsePath(containerUrn: string): Promise<void> {
    // Match the exact URN text in the link's accessible name
    const uriPattern = new RegExp(`^${containerUrn}$`);
    await this.entityHeaderBrowsePathLocator.getByRole('link', { name: uriPattern }).click();
  }

  async expectBrowsePlatformExists(platformName: string, timeout?: number): Promise<void> {
    const platformHeader = this.browsePlatformHeaderLocator(platformName);
    await expect(platformHeader).toBeVisible({ timeout });
  }

  async expectBrowsePathVisible(containerUrn?: string): Promise<void> {
    // Check for browse path link - use URN as accessible name
    const namePattern = containerUrn ? new RegExp(`^${containerUrn}$`) : /urn:li:container:/;
    const containerLink = this.entityHeaderBrowsePathLocator.getByRole('link', {
      name: namePattern,
    });
    await expect(containerLink).toBeVisible();
  }
}
