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
import { WelcomeModalPage } from '../../pages/welcome-modal-page';
import { resolvedUsers } from '../../fixtures/users';

test.describe('Welcome to DataHub Modal', () => {
  let welcomeModalPage: WelcomeModalPage;

  test.beforeEach(async ({ page, loginPage, logger, logDir }) => {
    welcomeModalPage = new WelcomeModalPage(page, logger, logDir);
    const { username, password } = resolvedUsers.admin;
    await loginPage.navigateToLogin();
    await welcomeModalPage.clearLocalStorage();
    await loginPage.login(username, password);
  });

  test.describe('First-Time User Experience', () => {
    test('should display modal automatically on first visit to homepage', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.expectModalTitle();
      await welcomeModalPage.expectSlide1Visible();
    });

    test('should not display modal when skipWelcomeModal is set', async () => {
      await welcomeModalPage.setSkipWelcomeModal();
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalNotVisible();
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
      await welcomeModalPage.expectGetStartedButtonVisible();
      await welcomeModalPage.expectDocsLinkVisible();
    });

    test('should auto-advance slides after 10 seconds', async ({ page }) => {
      await page.clock.install();
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.expectSlide1Visible();
      await page.clock.fastForward(10_000);
      await welcomeModalPage.expectSlide2Visible();
    });
  });

  test.describe('Close Interactions', () => {
    test('should close modal via close button', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaButton();
      await welcomeModalPage.expectModalNotVisible();
    });

    test('should close modal via Get Started button', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();
      await welcomeModalPage.closeViaGetStarted();
      await welcomeModalPage.expectModalNotVisible();
    });

    test('should close modal via ESC key', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaEscape();
      await welcomeModalPage.expectModalNotVisible();
    });

    test('should close modal by clicking outside', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaOutsideClick();
      await welcomeModalPage.expectModalNotVisible();
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
      await welcomeModalPage.expectModalNotVisible();
    });
  });

  test.describe('Documentation Link', () => {
    test('should display DataHub Docs link on final slide', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();
      await welcomeModalPage.expectDocsLinkVisible();
      const docsHref = await welcomeModalPage.docsLink.getAttribute('href');
      expect(docsHref).toBe('https://docs.datahub.com/docs/category/features');
    });

    test('should open DataHub Docs in new tab', async () => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();
      const target = await welcomeModalPage.docsLink.getAttribute('target');
      expect(target).toBe('_blank');
      const rel = await welcomeModalPage.docsLink.getAttribute('rel');
      expect(rel).toBe('noopener noreferrer');
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
      const interactEvents = trackRequests.filter(
        (r) => r['type'] === 'WelcomeToDataHubModalInteractEvent',
      );
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
      await welcomeModalPage.expectModalNotVisible();
      const skipValue = await welcomeModalPage.getSkipWelcomeModalValue();
      expect(skipValue).toBe('true');
    });
  });
});
