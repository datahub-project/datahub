import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage, type PageOptions } from './base.settings.page';
import { TOAST_MESSAGES } from './constants';

export class HomePagePostsPage extends BaseSettingsPage {
  private readonly managePostsContainer: Locator;
  private readonly createPostButton: Locator;
  private readonly postTypeAnnouncementTab: Locator;
  private readonly postTypePinnedLinkTab: Locator;
  private readonly titleInput: Locator;
  private readonly linkInput: Locator;
  private readonly mediaInput: Locator;
  private readonly submitCreateButton: Locator;
  private readonly submitUpdateButton: Locator;
  private readonly confirmButton: Locator;
  private readonly tableRows: Locator;
  private readonly menuEditItem: Locator;
  private readonly menuDeleteItem: Locator;
  private readonly v2HomePageAnnouncementsTab: Locator;

  constructor(page: Page, options?: PageOptions) {
    super(page, options);
    this.managePostsContainer = page.getByTestId('managePostsV2');
    this.createPostButton = page.getByTestId('posts-create-post-v2');
    this.postTypeAnnouncementTab = page.getByTestId('post-type-announcement');
    this.postTypePinnedLinkTab = page.getByTestId('post-type-pinned-link');
    this.titleInput = page.getByTestId('create-post-title');
    this.linkInput = page.getByTestId('create-post-link');
    this.mediaInput = page.getByTestId('create-post-media-location');
    this.submitCreateButton = page.getByTestId('create-post-button');
    this.submitUpdateButton = page.getByTestId('update-post-button');
    this.confirmButton = page.getByTestId('modal-confirm-button');
    this.tableRows = page.getByRole('row');
    this.menuEditItem = page.getByTestId('menu-item-edit');
    this.menuDeleteItem = page.getByTestId('menu-item-delete');
    this.v2HomePageAnnouncementsTab = page.getByTestId('v2-home-page-announcements');
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/posts');
    await expect(this.managePostsContainer).toBeVisible();
    await expect(this.createPostButton).toBeVisible();
  }

  // ── Dynamic selectors for posts ──────────────────────────────────────────
  // These helpers create locators based on runtime data (post titles, etc.)
  private getPostRow(title: string): Locator {
    return this.tableRows.filter({ hasText: new RegExp(title, 'i') });
  }

  private getPostMenuButton(title: string): Locator {
    return this.getPostRow(title).getByTestId('dropdown-menu-item');
  }

  async openCreateForm(): Promise<void> {
    await this.createPostButton.waitFor({ state: 'visible' });
    await this.createPostButton.click();
    await expect(this.submitCreateButton).toBeVisible();
  }

  async selectPostType(type: 'Announcement' | 'Pinned Link'): Promise<void> {
    const tab = type === 'Announcement' ? this.postTypeAnnouncementTab : this.postTypePinnedLinkTab;
    await tab.click();
  }

  async fillTitle(title: string): Promise<void> {
    await this.titleInput.clear();
    await this.titleInput.fill(title);
  }

  async fillLink(url: string): Promise<void> {
    await this.linkInput.clear();
    await this.linkInput.fill(url);
  }

  async fillMediaLocation(url: string): Promise<void> {
    await this.mediaInput.clear();
    await this.mediaInput.fill(url);
  }

  async submitCreate(): Promise<void> {
    await this.submitCreateButton.waitFor({ state: 'visible' });
    await this.submitCreateButton.click();
    // Wait for the toast first (confirms API call completed)
    // Then wait for networkidle to ensure page is fully updated
    await this.waitForToast(TOAST_MESSAGES.CREATED_POST);
    await this.page.waitForLoadState('networkidle');
  }

  async submitUpdate(): Promise<void> {
    await this.submitUpdateButton.waitFor({ state: 'visible' });
    await this.submitUpdateButton.click();
    // Wait for the toast first (confirms API call completed)
    // Then wait for networkidle to ensure page is fully updated
    await this.waitForToast(TOAST_MESSAGES.UPDATED_POST);
    await this.page.waitForLoadState('networkidle');
  }

  async editPost(title: string): Promise<void> {
    const menuButton = this.getPostMenuButton(title);
    await menuButton.waitFor({ state: 'visible' });
    await menuButton.click();
    await this.menuEditItem.waitFor({ state: 'visible' });
    await this.menuEditItem.click();
    await expect(this.submitUpdateButton).toBeVisible();
  }

  async deletePost(title: string): Promise<void> {
    const menuButton = this.getPostMenuButton(title);
    await menuButton.waitFor({ state: 'visible' });
    await menuButton.click();
    await this.menuDeleteItem.waitFor({ state: 'visible' });
    await this.menuDeleteItem.click();
    await this.confirmButton.click();
  }

  async navigateToAnnouncements(): Promise<void> {
    await this.page.goto('/');
    await this.skipOnboarding();
    await this.skipIntroducePage();
    await this.page.keyboard.press('Escape');
    await this.v2HomePageAnnouncementsTab.click();
  }

  async verifyPostVisible(title: string): Promise<void> {
    const isOnHomePage = await this.page.evaluate(() => window.location.pathname === '/');

    if (isOnHomePage) {
      await expect(this.page.getByText(new RegExp(title, 'i'))).toBeVisible();
    } else {
      await expect(this.getPostRow(title)).toBeVisible();
    }
  }

  async verifyPostRemovedFromHome(title: string): Promise<void> {
    await this.page.goto('/');
    await this.skipOnboarding();
    await this.skipIntroducePage();
    await this.page.keyboard.press('Escape');
    await this.v2HomePageAnnouncementsTab.click();
    await this.page.waitForLoadState('networkidle');
    await expect(this.page.getByText(title)).toBeHidden();
  }
}
