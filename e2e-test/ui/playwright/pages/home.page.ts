import { BasePage } from './base.page';
import type { Locator, Page } from '@playwright/test';
import type { DataHubLogger } from '../utils/logger';

export class HomePage extends BasePage {
  private readonly pageTitle: Locator;
  private readonly searchBar: Locator;
  private readonly templateWrapper: Locator;
  private readonly yourAssetsModule: Locator;
  private readonly domainsModule: Locator;
  private readonly platformsModule: Locator;
  private readonly moduleContainer: Locator;
  private readonly yourAssetsModuleTitle: Locator;
  private readonly domainsModuleTitle: Locator;
  private readonly platformsModuleTitle: Locator;
  private readonly userOwnedEntities: Locator;
  private readonly domainEntities: Locator;
  private readonly platformEntities: Locator;
  private readonly yourAssetsEntityItems: Locator;
  private readonly domainsEntityItems: Locator;
  private readonly platformsEntityItems: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.pageTitle = page.getByTestId('page-title');
    this.searchBar = page.getByTestId('search-bar');
    this.templateWrapper = page.getByTestId('template-wrapper');
    this.yourAssetsModule = page.getByTestId('your-assets-module');
    this.domainsModule = page.getByTestId('domains-module');
    this.platformsModule = page.getByTestId('platforms-module');
    this.moduleContainer = page.getByTestId('modules-container');
    this.yourAssetsModuleTitle = page.getByTestId('your-assets-module-title');
    this.domainsModuleTitle = page.getByTestId('domains-module-title');
    this.platformsModuleTitle = page.getByTestId('platforms-module-title');
    this.userOwnedEntities = page.getByTestId('user-owned-entities');
    this.domainEntities = page.getByTestId('domain-entities');
    this.platformEntities = page.getByTestId('platform-entities');
    this.yourAssetsEntityItems = this.userOwnedEntities.getByTestId('entity-item');
    this.domainsEntityItems = this.domainEntities.getByTestId('entity-item');
    this.platformsEntityItems = this.platformEntities.getByTestId('entity-item');
  }

  async navigateToHome(): Promise<void> {
    await this.navigate('/');
  }

  async waitForPageLoad(): Promise<void> {
    await super.waitForPageLoad();
    await this.templateWrapper.waitFor({ state: 'visible' });
  }

  private async isElementVisible(element: Locator): Promise<boolean> {
    await element.waitFor({ state: 'visible' });
    return true;
  }

  async isTemplateWrapperVisible(): Promise<boolean> {
    return this.isElementVisible(this.templateWrapper);
  }

  async isTemplateLoadedProperly(): Promise<boolean> {
    await this.templateWrapper.waitFor({ state: 'visible' });
    const hasModules = (await this.getModuleCount()) > 0;
    return hasModules;
  }

  async isPageTitleVisible(): Promise<boolean> {
    return this.isElementVisible(this.pageTitle);
  }

  async isSearchBarVisible(): Promise<boolean> {
    return this.isElementVisible(this.searchBar);
  }

  async isYourAssetsModuleVisible(): Promise<boolean> {
    return this.isElementVisible(this.yourAssetsModule);
  }

  async isDomainsModuleVisible(): Promise<boolean> {
    return this.isElementVisible(this.domainsModule);
  }

  async isPlatformsModuleVisible(): Promise<boolean> {
    return this.isElementVisible(this.platformsModule);
  }

  async getYourAssetsModuleHeader(): Promise<string | null> {
    await this.yourAssetsModuleTitle.waitFor({ state: 'visible' });
    return await this.yourAssetsModuleTitle.textContent();
  }

  async getDomainsModuleHeader(): Promise<string | null> {
    await this.domainsModuleTitle.waitFor({ state: 'visible' });
    return await this.domainsModuleTitle.textContent();
  }

  async getPlatformsModuleHeader(): Promise<string | null> {
    await this.platformsModuleTitle.waitFor({ state: 'visible' });
    return await this.platformsModuleTitle.textContent();
  }

  async getYourAssetsEntityCount(): Promise<number> {
    await this.yourAssetsModule.waitFor({ state: 'visible' });
    return await this.yourAssetsEntityItems.count();
  }

  async getDomainsEntityCount(): Promise<number> {
    await this.domainsModule.waitFor({ state: 'visible' });
    return await this.domainsEntityItems.count();
  }

  async getPlatformsEntityCount(): Promise<number> {
    await this.platformsModule.waitFor({ state: 'visible' });
    return await this.platformsEntityItems.count();
  }

  async isModuleContainerVisible(): Promise<boolean> {
    return this.isElementVisible(this.moduleContainer);
  }

  async getModuleCount(): Promise<number> {
    const count1 = await this.yourAssetsModule.count();
    const count2 = await this.domainsModule.count();
    const count3 = await this.platformsModule.count();
    return count1 + count2 + count3;
  }

  async areModulesInExpectedOrder(): Promise<boolean> {
    await this.yourAssetsModule.waitFor({ state: 'visible' });
    await this.domainsModule.waitFor({ state: 'visible' });
    await this.platformsModule.waitFor({ state: 'visible' });

    const yourAssetsBox = await this.yourAssetsModule.boundingBox();
    const domainsBox = await this.domainsModule.boundingBox();
    const platformsBox = await this.platformsModule.boundingBox();

    if (!yourAssetsBox || !domainsBox || !platformsBox) {
      return false;
    }

    // Your Assets should be first (topmost)
    // Domains and Platforms are on the second row, Domains should appear before Platforms
    const yourAssetsFirst = yourAssetsBox.y < domainsBox.y && yourAssetsBox.y < platformsBox.y;
    const domainsBeforePlatforms = domainsBox.x < platformsBox.x; // Side-by-side, check x-position

    return yourAssetsFirst && domainsBeforePlatforms;
  }
}
