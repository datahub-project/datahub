import { Locator, Page, expect } from '@playwright/test';
import { BaseSettingsPage } from './base.settings.page';
import type { DataHubLogger } from '../../utils/logger';

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
  private readonly homePageAnnouncementsTab: Locator;
  private readonly homePageContentContainer: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.managePostsContainer = page.locator('[data-testid="managePostsV2"]');
    this.createPostButton = page.locator('[data-testid="posts-create-post-v2"]');
    this.postTypeAnnouncementTab = page.locator('[data-testid="post-type-announcement"]');
    this.postTypePinnedLinkTab = page.locator('[data-testid="post-type-pinned-link"]');
    this.titleInput = page.locator('[data-testid="create-post-title"]');
    this.linkInput = page.locator('[data-testid="create-post-link"]');
    this.mediaInput = page.locator('[data-testid="create-post-media-location"]');
    this.submitCreateButton = page.locator('[data-testid="create-post-button"]');
    this.submitUpdateButton = page.locator('[data-testid="update-post-button"]');
    this.confirmButton = page.locator('[data-testid="modal-confirm-button"]');
    this.homePageAnnouncementsTab = page.locator('#v2-home-page-announcements');
    this.homePageContentContainer = page.locator('[data-testid="home-page-content-container"]');
  }

  async navigate(): Promise<void> {
    await this.page.goto('/settings/posts');
    await expect(this.managePostsContainer).toBeVisible({ timeout: 15000 });
    await expect(this.createPostButton).toBeVisible({ timeout: 15000 });
  }

  async openCreateForm(): Promise<void> {
    await this.createPostButton.click({ force: true });
    await expect(this.submitCreateButton).toBeVisible({ timeout: 10000 });
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
    await this.submitCreateButton.click({ force: true });
    // Wait for the modal to close (the button disappears when the modal unmounts).
    await expect(this.submitCreateButton).not.toBeVisible({ timeout: 10000 });
    // Wait for the backend mutation to complete before navigating away.
    await expect(this.page.getByText('Created Post!')).toBeVisible({ timeout: 10000 });
  }

  async submitUpdate(): Promise<void> {
    await this.submitUpdateButton.click({ force: true });
    // Wait for the modal to close.
    await expect(this.submitUpdateButton).not.toBeVisible({ timeout: 10000 });
  }

  async editPost(title: string): Promise<void> {
    await this.page.locator('tr').filter({ hasText: title }).locator('[data-testid="dropdown-menu-item"]').first().click();
    await this.page.locator('[data-testid="menu-item-edit"]').click();
  }

  async deletePost(title: string): Promise<void> {
    await this.page.locator('tr').filter({ hasText: title }).locator('[data-testid="dropdown-menu-item"]').first().click();
    await this.page.locator('[data-testid="menu-item-delete"]').click();
    await this.confirmButton.click();
  }

  async navigateToAnnouncements(): Promise<void> {
    // The announcements tab lives on the home page, not the settings page.
    await this.page.goto('/');
    await expect(this.homePageContentContainer).toBeVisible({ timeout: 15000 });
    await this.homePageAnnouncementsTab.waitFor({ state: 'visible', timeout: 15000 });
    await this.homePageAnnouncementsTab.click();
  }

  async verifyPostVisible(title: string): Promise<void> {
    await expect(this.page.getByText(title).first()).toBeVisible({ timeout: 15000 });
  }

  async verifyPostRemovedFromHome(title: string): Promise<void> {
    await this.page.goto('/');
    await expect(this.homePageContentContainer).toBeVisible({ timeout: 15000 });
    await this.page.reload();
    await expect(this.page.getByText(title)).not.toBeVisible({ timeout: 10000 });
  }
}
