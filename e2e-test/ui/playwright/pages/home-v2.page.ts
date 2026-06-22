/**
 * HomeV2Page — Page Object Model for home page V2 design
 * Migrated from Cypress homeV2/v2_home.js
 *
 * Focuses on home page V2 specific elements.
 * For navbar testing, use NavbarPage.
 */

import { expect, Locator } from '@playwright/test';
import type { Page } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

export class HomeV2Page extends BasePage {
  readonly homePageContentContainer: Locator;

  constructor(
    protected readonly page: Page,
    protected readonly logger?: DataHubLogger,
    protected readonly logDir?: string,
  ) {
    super(page, logger, logDir);
    this.homePageContentContainer = page.getByTestId('home-page-content-container');
  }

  async skipIntroducePage(): Promise<void> {
    this.logger?.step('skip introduce page', {});
    await this.page.addInitScript(() => {
      localStorage.setItem('skipAcrylIntroducePage', 'true');
    });
  }

  async navigateToHome(): Promise<void> {
    this.logger?.step('navigate to home page', {});
    await this.navigate('/');
    await this.waitForPageLoad();
  }

  async verifyHomePageContentContainerExists(): Promise<void> {
    this.logger?.step('verify home page content container exists', {});
    await expect(this.homePageContentContainer).toBeVisible();
  }
}
