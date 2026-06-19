/**
 * NavbarPage — Page Object Model for navbar redesign
 * Tests the new navbar redesign with all navigation menus and features
 */

import { expect, Locator } from '@playwright/test';
import type { Page } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class NavbarPage extends BasePage {
  readonly navSidebar: Locator;
  readonly navbarLogo: Locator;
  readonly navbarToggler: Locator;
  readonly navMenuLinks: Locator;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    super(page, logger, logDir);
    this.navSidebar = page.getByTestId('nav-sidebar');
    this.navbarLogo = page.getByTestId('nav-bar-home-logo');
    this.navbarToggler = page.getByTestId('nav-bar-toggler');
    this.navMenuLinks = page.getByTestId('nav-menu-links');
  }

  async navigateToHome(): Promise<void> {
    this.logger?.step('navigate to home', {});
    await this.navigate('/');
    await this.waitForPageLoad();
  }

  async verifyNavbarSidebarVisible(): Promise<void> {
    this.logger?.step('verify navbar sidebar visible', {});
    await expect(this.navSidebar).toBeVisible();
  }

  async verifyNavbarLogoVisible(): Promise<void> {
    this.logger?.step('verify navbar logo visible', {});
    await expect(this.navbarLogo).toBeVisible();
  }

  async verifyNavbarTogglerVisible(): Promise<void> {
    this.logger?.step('verify navbar toggler visible', {});
    await expect(this.navbarToggler).toBeVisible();
  }

  async verifyNavbarMenusExist(): Promise<void> {
    this.logger?.step('verify navbar menus exist', {});
    // Navbar redesign renders multiple menu sections: header (Home), main (Documents, Govern, etc.), footer (Account).
    // Verify at least 2 menus exist to ensure navbar structure is rendered correctly (flexible for menu variations).
    const menuCount = await this.navMenuLinks.count();
    expect(menuCount).toBeGreaterThanOrEqual(2);
  }

  async toggleNavbar(): Promise<void> {
    this.logger?.step('toggle navbar', {});
    await this.navbarToggler.click();
  }

  async isNavbarCollapsed(): Promise<boolean> {
    this.logger?.step('check if navbar is collapsed', {});
    const dataCollapsed = await this.navSidebar.getAttribute('data-collapsed');
    return dataCollapsed === 'true';
  }
}
