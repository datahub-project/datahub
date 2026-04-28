import { Page, Locator } from '@playwright/test';

export class SidebarComponent {
  readonly sidebar: Locator;
  readonly homeLink: Locator;
  readonly searchLink: Locator;
  readonly datasetsLink: Locator;
  readonly dashboardsLink: Locator;

  constructor(private page: Page) {
    this.sidebar = page.locator('[data-testid="sidebar"]');
    this.homeLink = page.locator('[data-testid="sidebar-home"]');
    this.searchLink = page.locator('[data-testid="sidebar-search"]');
    this.datasetsLink = page.locator('[data-testid="sidebar-datasets"]');
    this.dashboardsLink = page.locator('[data-testid="sidebar-dashboards"]');
  }

  async navigateToHome(): Promise<void> {
    await this.homeLink.click();
  }

  async navigateToSearch(): Promise<void> {
    await this.searchLink.click();
  }

  async navigateToDatasets(): Promise<void> {
    await this.datasetsLink.click();
  }

  async isVisible(): Promise<boolean> {
    return await this.sidebar.isVisible();
  }
}
