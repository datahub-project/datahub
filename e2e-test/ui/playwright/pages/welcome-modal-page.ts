import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base-page';

export class WelcomeModalPage extends BasePage {
  readonly modal: Locator;
  readonly modalTitle: Locator;
  readonly closeButton: Locator;
  readonly carouselDots: Locator;
  readonly lastCarouselDot: Locator;
  readonly activeCarouselDot: Locator;
  readonly getStartedButton: Locator;
  readonly docsLink: Locator;

  // Slide headings
  readonly slide1Heading: Locator;
  readonly slide2Heading: Locator;
  readonly slide3Heading: Locator;
  readonly finalSlideHeading: Locator;

  constructor(page: Page) {
    super(page);

    // Modal selectors (from feature doc)
    this.modal = page.getByRole('dialog');
    this.modalTitle = page.getByRole('heading', { name: 'Welcome to DataHub' });
    this.closeButton = page.locator('[data-testid="modal-close-icon"]');

    // Carousel navigation selectors
    this.carouselDots = page.locator('.slick-dots li');
    this.lastCarouselDot = page.locator('.slick-dots li').last();
    this.activeCarouselDot = page.locator('.slick-dots li.slick-active');

    // CTA selectors (on final slide)
    this.getStartedButton = page.getByRole('button', { name: /get started/i });
    this.docsLink = page.getByRole('link', { name: 'DataHub Docs' });

    // Slide heading selectors
    this.slide1Heading = page.getByRole('heading', { name: 'Find Any Asset, Anywhere' });
    this.slide2Heading = page.getByRole('heading', { name: "Understand Your Data's Origin" });
    this.slide3Heading = page.getByRole('heading', { name: 'Manage Breaking Changes Confidently' });
    this.finalSlideHeading = page.getByRole('heading', { name: 'Ready to Get Started?' });
  }

  async navigateToHome(): Promise<void> {
    const currentUrl = this.page.url();
    const isAlreadyOnHome = currentUrl.endsWith('/') || currentUrl.includes('localhost:9002/#') || currentUrl.includes('localhost:9002#');

    if (isAlreadyOnHome) {
      // Force reload if already on home to ensure component remounts with fresh localStorage
      await this.page.reload({ waitUntil: 'domcontentloaded' });
    } else {
      await this.navigate('/');
    }
    await this.waitForPageLoad();
  }

  async expectModalVisible(): Promise<void> {
    await expect(this.modal).toBeVisible({ timeout: 10000 });
  }

  async waitForCarouselReady(): Promise<void> {
    // Wait for loading state to finish - the loading text disappears when carousel loads
    const loadingText = this.page.getByText('Loading...');
    await loadingText.waitFor({ state: 'hidden', timeout: 15000 }).catch(() => {
      // If loading text isn't found, it might already be hidden - that's OK
    });

    // Wait for carousel dots to be visible (more reliable than .slick-slider)
    const carouselDots = this.page.locator('.slick-dots');
    await carouselDots.waitFor({ state: 'visible', timeout: 15000 });
  }

  async expectModalNotVisible(): Promise<void> {
    await expect(this.modal).not.toBeVisible();
  }

  async expectModalTitle(): Promise<void> {
    await expect(this.modalTitle).toBeVisible();
  }

  async closeViaButton(): Promise<void> {
    // Wait for carousel to be ready first (modal might be in loading state)
    await this.waitForCarouselReady();
    // Wait for close button to be visible and clickable
    await this.closeButton.waitFor({ state: 'visible', timeout: 5000 });
    await this.closeButton.click();
    await this.modal.waitFor({ state: 'hidden' });
  }

  async closeViaGetStarted(): Promise<void> {
    await this.getStartedButton.click();
    await this.modal.waitFor({ state: 'hidden' });
  }

  async closeViaEscape(): Promise<void> {
    await this.page.keyboard.press('Escape');
    await this.modal.waitFor({ state: 'hidden' });
  }

  async closeViaOutsideClick(): Promise<void> {
    // Click outside the modal (on backdrop)
    await this.page.mouse.click(10, 10);
    await this.modal.waitFor({ state: 'hidden' });
  }

  async clickCarouselDot(index: number): Promise<void> {
    await this.carouselDots.nth(index).click();
    await this.page.waitForTimeout(500); // Wait for carousel animation
  }

  async clickLastCarouselDot(): Promise<void> {
    await this.lastCarouselDot.click();
    await this.page.waitForTimeout(500); // Wait for carousel animation
  }

  async expectSlide1Visible(): Promise<void> {
    await this.waitForCarouselReady();
    await expect(this.slide1Heading).toBeVisible();
  }

  async expectSlide2Visible(): Promise<void> {
    await this.waitForCarouselReady();
    await expect(this.slide2Heading).toBeVisible();
  }

  async expectSlide3Visible(): Promise<void> {
    await this.waitForCarouselReady();
    await expect(this.slide3Heading).toBeVisible();
  }

  async expectFinalSlideVisible(): Promise<void> {
    await this.waitForCarouselReady();
    await expect(this.finalSlideHeading).toBeVisible();
  }

  async expectGetStartedButtonVisible(): Promise<void> {
    await expect(this.getStartedButton).toBeVisible();
  }

  async expectDocsLinkVisible(): Promise<void> {
    await expect(this.docsLink).toBeVisible();
  }

  async getActiveSlideIndex(): Promise<number> {
    return this.page.evaluate(() => {
      const activeDot = document.querySelector('.slick-dots li.slick-active');
      const allDots = Array.from(document.querySelectorAll('.slick-dots li'));
      return allDots.indexOf(activeDot as Element);
    });
  }

  async waitForSlideChange(expectedIndex: number, timeout = 15000): Promise<void> {
    await this.page.waitForFunction(
      (index) => {
        const activeDot = document.querySelector('.slick-dots li.slick-active');
        const allDots = Array.from(document.querySelectorAll('.slick-dots li'));
        return allDots.indexOf(activeDot as Element) === index;
      },
      expectedIndex,
      { timeout }
    );
  }

  clearLocalStorage(): Promise<void> {
    return this.page.evaluate(() => {
      localStorage.removeItem('skipWelcomeModal');
    });
  }

  async setSkipWelcomeModal(): Promise<void> {
    await this.page.evaluate(() => {
      localStorage.setItem('skipWelcomeModal', 'true');
    });
    // Note: navigateToHome() will handle reloading if needed
  }

  async getSkipWelcomeModalValue(): Promise<string | null> {
    return this.page.evaluate(() => {
      return localStorage.getItem('skipWelcomeModal');
    });
  }
}
