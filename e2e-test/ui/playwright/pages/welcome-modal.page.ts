import { Page, Locator, expect } from '@playwright/test';
import { BasePage } from './base.page';
import type { DataHubLogger } from '../utils/logger';

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

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);

    // Modal selectors — scoped to the Welcome to DataHub dialog so that other
    // dialogs on the page (e.g. confirmation prompts, onboarding tours) do not
    // cause false failures in expectModalVisible / expectModalNotVisible.
    this.modal = page.getByRole('dialog').filter({ hasText: 'Welcome to DataHub' });
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
    const isAlreadyOnHome =
      currentUrl.endsWith('/') || currentUrl.includes('localhost:9002/#') || currentUrl.includes('localhost:9002#');

    if (isAlreadyOnHome) {
      // Force reload if already on home to ensure component remounts with fresh localStorage
      await this.page.reload({ waitUntil: 'domcontentloaded' });
    } else {
      await this.navigate('/');
    }
    await this.waitForPageLoad();
  }

  async expectModalVisible(): Promise<void> {
    // Extended timeout: the modal is rendered asynchronously after login completes and
    // the React component tree settles. Playwright's default 5 s is sometimes too short.
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

  async closeViaButton(): Promise<void> {
    // Only wait for carousel to be ready if the loading indicator is actually present.
    const loadingText = this.page.getByText('Loading...');
    if (await loadingText.isVisible()) {
      await this.waitForCarouselReady();
    }
    // Ant Design renders the close icon only after the modal open animation completes;
    // waitFor is needed because Playwright's click() auto-wait checks actionability but
    // the element may not yet be in the DOM during the animation frame.
    await this.closeButton.waitFor({ state: 'visible', timeout: 5000 });
    await this.closeButton.click();
    await this.modal.waitFor({ state: 'hidden' });
  }

  async closeViaGetStarted(): Promise<void> {
    // clickLastCarouselDot already waited for the button to be attached.
    // Short safety timeout here in case of unexpected state; the button is positioned
    // absolutely at the bottom of the carousel container (bottom: -2px) so we
    // scroll it into view and force-click to bypass viewport checks.
    await this.getStartedButton.waitFor({ state: 'attached', timeout: 5000 });
    await this.getStartedButton.scrollIntoViewIfNeeded();
    await this.getStartedButton.click({ force: true });
    await this.modal.waitFor({ state: 'hidden' });
  }

  async closeViaEscape(): Promise<void> {
    // Ensure the modal has focus so the Escape key event is captured by Ant Design.
    await this.modal.click({ position: { x: 5, y: 5 }, force: true }).catch(() => {});
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
    // react-slick does not emit a DOM signal when the slide CSS transition finishes (~300 ms).
    // There is no reliable locator to await, so a short fixed delay is the least-fragile option.
    await this.page.waitForTimeout(500);
  }

  async clickLastCarouselDot(): Promise<void> {
    // Ensure carousel dots are rendered before clicking.
    await this.waitForCarouselReady();
    await this.lastCarouselDot.waitFor({ state: 'visible', timeout: 10000 });
    await this.lastCarouselDot.click();

    // React-slick fires afterChange via setTimeout(fn, speed) — NOT via transitionend.
    // A ResizeObserver on .slick-list can call onWindowResized(), which clears
    // animationEndCallback (line 242 in inner-slider.js) if the DOM shifts during
    // animation (e.g. video/image layout reflows). This silently drops afterChange,
    // so setCurrentSlide(lastIndex) is never called and the Get Started button
    // never renders.
    //
    // If the carousel is already on the last slide when clicked, slideHandler
    // returns early with state=null (no transition, no new callback).
    //
    // Fix: wait for the button as the ground truth. On failure, navigate to slide 0
    // first (so the next click is a real transition) then re-click the last dot.
    const buttonReady = await this.getStartedButton
      .waitFor({ state: 'attached', timeout: 5000 })
      .then(() => true)
      .catch(() => false);

    if (!buttonReady) {
      // Move to slide 0 so the carousel's internal currentSlide != lastIndex,
      // ensuring the next click triggers a full slideHandler run (not early-return).
      await this.carouselDots.first().click({ force: true });
      // slick-active is set at animation START, not when afterChange fires.
      // We must wait for afterChange to complete for slide 0 before re-clicking the
      // last dot — otherwise react-slick's internal currentSlide has not committed yet
      // and the subsequent click may trigger slideHandler while still mid-animation,
      // causing afterChange to be dropped again by the ResizeObserver race.
      // react-slick's default animation speed is 500 ms, so 700 ms covers the
      // setTimeout(afterChange, speed) call with margin.
      await this.waitForSlideChange(0, 5000).catch(() => {});
      await this.page.waitForTimeout(700);
      await this.lastCarouselDot.click({ force: true });
      await this.getStartedButton.waitFor({ state: 'attached', timeout: 20000 });
    }
  }

  async expectSlide1Visible(): Promise<void> {
    await this.waitForCarouselReady();
    await expect(this.slide1Heading).toBeVisible();
  }

  async expectSlide2Visible(): Promise<void> {
    await this.waitForCarouselReady();
    // Wait for the active dot to move to index 1 before asserting the heading.
    // This is reliable with both real and fake (page.clock) timers since
    // waitForFunction polls the DOM using real Playwright time.
    await this.waitForSlideChange(1);
    await expect(this.slide2Heading).toBeVisible();
  }

  async expectSlide3Visible(): Promise<void> {
    await this.waitForCarouselReady();
    await expect(this.slide3Heading).toBeVisible();
  }

  async expectFinalSlideVisible(): Promise<void> {
    await this.waitForCarouselReady();
    // clickLastCarouselDot already confirmed the Get Started button is attached,
    // which requires afterChange to have fired and React to have re-rendered with
    // currentSlide === lastIndex. The heading is visible in the same render cycle.
    await expect(this.finalSlideHeading).toBeVisible();
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
      { timeout },
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
