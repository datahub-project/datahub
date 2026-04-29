/**
 * Welcome Modal tests — comprehensive onboarding flow coverage.
 *
 * Uses login-test for a fresh, unauthenticated context per test.
 * loginPage is provided by the fixture. WelcomeModalPage is constructed
 * in beforeEach with logger so all interactions are structured-logged.
 *
 * beforeEach: navigate to login, clear skipWelcomeModal from localStorage,
 * log in — so every test starts on the homepage with the modal visible.
 */

import { test, expect } from '../../fixtures/login-test';
import { WelcomeModalPage } from '../../pages/welcome-modal.page';
import { users } from '../../data/users';

test.describe('Welcome to DataHub Modal', () => {
  let welcomeModalPage: WelcomeModalPage;

  test.beforeEach(async ({ page, loginPage, logger, logDir }) => {
    welcomeModalPage = new WelcomeModalPage(page, logger, logDir);
    const { username, password } = users.admin;
    await loginPage.navigateToLogin();
    await welcomeModalPage.clearLocalStorage();
    await loginPage.login(username, password);
  });

  test.describe('First-Time User Experience', () => {
    test('should display modal automatically on first visit to homepage', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await expect(welcomeModalPage.modalTitle).toBeVisible();
      await welcomeModalPage.expectSlide1Visible();
    });

    test('should not display modal when skipWelcomeModal is set', async () => {
      await welcomeModalPage.setSkipWelcomeModal();
      await welcomeModalPage.navigateToHome();
      await expect(welcomeModalPage.modal).toBeHidden();
    });

    test('should set skipWelcomeModal in localStorage after closing', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaButton();
      const skipValue = await welcomeModalPage.getSkipWelcomeModalValue();
      expect(skipValue).toBe('true');
    });
  });

  test.describe('Carousel Navigation', () => {
    test('should display first slide by default', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.expectSlide1Visible();
    });

    test('should navigate to slides via carousel dots', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickCarouselDot(1);
      await welcomeModalPage.expectSlide2Visible();
      await welcomeModalPage.clickCarouselDot(2);
      await welcomeModalPage.expectSlide3Visible();
    });

    test('should navigate to final slide and display CTA elements', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();
      await expect(welcomeModalPage.getStartedButton).toBeVisible();
      await expect(welcomeModalPage.docsLink).toBeVisible();
    });

    test.skip('should auto-advance slides after 10 seconds', async () => {
      // Skipped in CI: react-slick autoplay is paused in headless Chromium
      // (window focus / visibilityState is not reliable), causing the 10-second
      // timer to never fire.  Covered by manual / headed local runs.
      test.setTimeout(25_000);
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.expectSlide1Visible();
      await welcomeModalPage.waitForSlideChange(1, 13_000);
    });
  });

  test.describe('Close Interactions', () => {
    test('should close modal via close button', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaButton();
      await expect(welcomeModalPage.modal).toBeHidden();
    });

    test('should close modal via Get Started button', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();
      await welcomeModalPage.closeViaGetStarted();
      await expect(welcomeModalPage.modal).toBeHidden();
    });

    test('should close modal via ESC key', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaEscape();
      await expect(welcomeModalPage.modal).toBeHidden();
    });

    test('should close modal by clicking outside', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaOutsideClick();
      await expect(welcomeModalPage.modal).toBeHidden();
    });
  });

  test.describe('LocalStorage Persistence', () => {
    test('should persist skipWelcomeModal after closing via close button', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaButton();
      const skipValue = await welcomeModalPage.getSkipWelcomeModalValue();
      expect(skipValue).toBe('true');
    });

    test('should persist skipWelcomeModal after closing via Get Started', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.closeViaGetStarted();
      const skipValue = await welcomeModalPage.getSkipWelcomeModalValue();
      expect(skipValue).toBe('true');
    });

    test('should not show modal again after localStorage flag is set', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaButton();
      await welcomeModalPage.navigateToHome();
      await expect(welcomeModalPage.modal).toBeHidden();
    });
  });

  test.describe('Documentation Link', () => {
    test('should display DataHub Docs link on final slide', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();
      await expect(welcomeModalPage.docsLink).toBeVisible();
      await expect(welcomeModalPage.docsLink).toHaveAttribute(
        'href',
        'https://docs.datahub.com/docs/category/features',
      );
    });

    test('should open DataHub Docs in new tab', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();
      await expect(welcomeModalPage.docsLink).toHaveAttribute('target', '_blank');
      await expect(welcomeModalPage.docsLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  test.describe('Analytics Tracking', () => {
    test('should track modal view event on open', async ({ page }) => {
      const trackRequests: Record<string, unknown>[] = [];
      await page.route('**/track', (route) => {
        const postData = route.request().postData();
        if (postData) trackRequests.push(JSON.parse(postData) as Record<string, unknown>);
        void route.continue();
      });
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await page.waitForTimeout(1000);
      const viewEvent = trackRequests.find((r) => r['type'] === 'WelcomeToDataHubModalViewEvent');
      expect(viewEvent).toBeDefined();
    });

    test('should track interact event on slide change', async ({ page }) => {
      const trackRequests: Record<string, unknown>[] = [];
      await page.route('**/track', (route) => {
        const postData = route.request().postData();
        if (postData) trackRequests.push(JSON.parse(postData) as Record<string, unknown>);
        void route.continue();
      });
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickCarouselDot(1);
      await page.waitForTimeout(1000);
      const interactEvents = trackRequests.filter((r) => r['type'] === 'WelcomeToDataHubModalInteractEvent');
      expect(interactEvents.length).toBeGreaterThan(0);
    });

    test('should track exit event with correct exit method', async ({ page }) => {
      const trackRequests: Record<string, unknown>[] = [];
      await page.route('**/track', (route) => {
        const postData = route.request().postData();
        if (postData) trackRequests.push(JSON.parse(postData) as Record<string, unknown>);
        void route.continue();
      });
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaButton();
      await page.waitForTimeout(1000);
      const exitEvent = trackRequests.find((r) => r['type'] === 'WelcomeToDataHubModalExitEvent');
      expect(exitEvent).toBeDefined();
      expect(exitEvent?.['exitMethod']).toBe('close_button');
    });

    test('should track documentation link click event', async ({ page }) => {
      const trackRequests: Record<string, unknown>[] = [];
      await page.route('**/track', (route) => {
        const postData = route.request().postData();
        if (postData) trackRequests.push(JSON.parse(postData) as Record<string, unknown>);
        void route.continue();
      });
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();
      await welcomeModalPage.docsLink.click();
      await page.waitForTimeout(1000);
      const linkClickEvent = trackRequests.find(
        (r) => r['type'] === 'WelcomeToDataHubModalClickViewDocumentationEvent',
      );
      expect(linkClickEvent).toBeDefined();
      expect(linkClickEvent?.['url']).toBe('https://docs.datahub.com/docs/category/features');
    });
  });

  test.describe('Edge Cases', () => {
    test('should handle rapid carousel navigation', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickCarouselDot(1);
      await welcomeModalPage.clickCarouselDot(2);
      await welcomeModalPage.clickCarouselDot(0);
      await welcomeModalPage.expectSlide1Visible();
    });

    test('should reset carousel to first slide when reopened', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickCarouselDot(2);
      await welcomeModalPage.expectSlide3Visible();
      await welcomeModalPage.closeViaButton();
      await welcomeModalPage.clearLocalStorage();
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.expectSlide1Visible();
    });

    test('should handle closing modal mid-carousel', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickCarouselDot(1);
      await welcomeModalPage.expectSlide2Visible();
      await welcomeModalPage.closeViaButton();
      await expect(welcomeModalPage.modal).toBeHidden();
      const skipValue = await welcomeModalPage.getSkipWelcomeModalValue();
      expect(skipValue).toBe('true');
    });
  });
});
