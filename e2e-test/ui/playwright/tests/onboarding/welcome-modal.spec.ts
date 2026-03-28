import { test as base, expect } from '@playwright/test';
import { LoginPage } from '../../pages/login-page';
import { WelcomeModalPage } from '../../pages/welcome-modal-page';

/**
 * Welcome Modal tests - These tests verify onboarding flow with login
 * Therefore, they MUST NOT use the shared authentication state
 */
const test = base.extend<{
  loginPage: LoginPage;
  welcomeModalPage: WelcomeModalPage;
}>({
  loginPage: async ({ page }, use) => {
    await use(new LoginPage(page));
  },
  welcomeModalPage: async ({ page }, use) => {
    await use(new WelcomeModalPage(page));
  },
});

// Explicitly disable shared auth state for onboarding tests
test.use({ storageState: { cookies: [], origins: [] } });

test.describe('Welcome to DataHub Modal', () => {
  test.beforeEach(async ({ loginPage, welcomeModalPage }) => {
    await loginPage.navigateToLogin();
    await welcomeModalPage.clearLocalStorage();
    await loginPage.login('datahub', 'datahub');
  });

  test.describe('First-Time User Experience', () => {
    test('should display modal automatically on first visit to homepage', async ({
      welcomeModalPage,
    }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.expectModalTitle();
      await welcomeModalPage.expectSlide1Visible();
    });

    test('should not display modal when skipWelcomeModal is set', async ({
      welcomeModalPage,
    }) => {
      await welcomeModalPage.setSkipWelcomeModal();
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalNotVisible();
    });

    test('should set skipWelcomeModal in localStorage after closing', async ({
      welcomeModalPage,
    }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaButton();

      const skipValue = await welcomeModalPage.getSkipWelcomeModalValue();
      expect(skipValue).toBe('true');
    });
  });

  test.describe('Carousel Navigation', () => {
    test('should display first slide by default', async ({ welcomeModalPage }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.expectSlide1Visible();
    });

    test('should navigate to slides via carousel dots', async ({ welcomeModalPage }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.clickCarouselDot(1);
      await welcomeModalPage.expectSlide2Visible();

      await welcomeModalPage.clickCarouselDot(2);
      await welcomeModalPage.expectSlide3Visible();
    });

    test('should navigate to final slide and display CTA elements', async ({
      welcomeModalPage,
    }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();
      await welcomeModalPage.expectGetStartedButtonVisible();
      await welcomeModalPage.expectDocsLinkVisible();
    });

    test.skip('should auto-advance slides after 10 seconds', async ({
      welcomeModalPage,
      page,
    }) => {
      // Note: This test is skipped because autoplay behavior depends on video loading
      // and can be flaky in test environments. Manual navigation is tested separately.
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.expectSlide1Visible();

      // Wait for carousel animation to complete and videos to load
      await page.waitForTimeout(1000);

      // Carousel auto-advances after 10 seconds, add buffer for animation
      await welcomeModalPage.waitForSlideChange(1, 12000);
      await welcomeModalPage.expectSlide2Visible();
    });
  });

  test.describe('Close Interactions', () => {
    test('should close modal via close button', async ({ welcomeModalPage }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.closeViaButton();
      await welcomeModalPage.expectModalNotVisible();
    });

    test('should close modal via Get Started button', async ({ welcomeModalPage }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();

      await welcomeModalPage.closeViaGetStarted();
      await welcomeModalPage.expectModalNotVisible();
    });

    test('should close modal via ESC key', async ({ welcomeModalPage }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.closeViaEscape();
      await welcomeModalPage.expectModalNotVisible();
    });

    test('should close modal by clicking outside', async ({ welcomeModalPage }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.closeViaOutsideClick();
      await welcomeModalPage.expectModalNotVisible();
    });
  });

  test.describe('LocalStorage Persistence', () => {
    test('should persist skipWelcomeModal after closing via close button', async ({
      welcomeModalPage,
    }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaButton();

      const skipValue = await welcomeModalPage.getSkipWelcomeModalValue();
      expect(skipValue).toBe('true');
    });

    test('should persist skipWelcomeModal after closing via Get Started', async ({
      welcomeModalPage,
    }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.closeViaGetStarted();

      const skipValue = await welcomeModalPage.getSkipWelcomeModalValue();
      expect(skipValue).toBe('true');
    });

    test('should not show modal again after localStorage flag is set', async ({
      welcomeModalPage,
    }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();
      await welcomeModalPage.closeViaButton();

      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalNotVisible();
    });
  });

  test.describe('Documentation Link', () => {
    test('should display DataHub Docs link on final slide', async ({ welcomeModalPage }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();
      await welcomeModalPage.expectDocsLinkVisible();

      const docsHref = await welcomeModalPage.docsLink.getAttribute('href');
      expect(docsHref).toBe('https://docs.datahub.com/docs/category/features');
    });

    test('should open DataHub Docs in new tab', async ({ welcomeModalPage }) => {
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
    test('should track modal view event on open', async ({ page, welcomeModalPage }) => {
      const trackRequests: any[] = [];

      await page.route('**/track', (route) => {
        const postData = route.request().postData();
        if (postData) {
          trackRequests.push(JSON.parse(postData));
        }
        route.continue();
      });

      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await page.waitForTimeout(1000);

      const viewEvent = trackRequests.find(
        (req) => req.type === 'WelcomeToDataHubModalViewEvent'
      );
      expect(viewEvent).toBeDefined();
    });

    test('should track interact event on slide change', async ({ page, welcomeModalPage }) => {
      const trackRequests: any[] = [];

      await page.route('**/track', (route) => {
        const postData = route.request().postData();
        if (postData) {
          trackRequests.push(JSON.parse(postData));
        }
        route.continue();
      });

      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.clickCarouselDot(1);
      await page.waitForTimeout(1000);

      const interactEvents = trackRequests.filter(
        (req) => req.type === 'WelcomeToDataHubModalInteractEvent'
      );
      expect(interactEvents.length).toBeGreaterThan(0);
    });

    test('should track exit event with correct exit method', async ({
      page,
      welcomeModalPage,
    }) => {
      const trackRequests: any[] = [];

      await page.route('**/track', (route) => {
        const postData = route.request().postData();
        if (postData) {
          trackRequests.push(JSON.parse(postData));
        }
        route.continue();
      });

      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.closeViaButton();
      await page.waitForTimeout(1000);

      const exitEvent = trackRequests.find(
        (req) => req.type === 'WelcomeToDataHubModalExitEvent'
      );
      expect(exitEvent).toBeDefined();
      expect(exitEvent.exitMethod).toBe('close_button');
    });

    test('should track documentation link click event', async ({ page, welcomeModalPage }) => {
      const trackRequests: any[] = [];

      await page.route('**/track', (route) => {
        const postData = route.request().postData();
        if (postData) {
          trackRequests.push(JSON.parse(postData));
        }
        route.continue();
      });

      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.clickLastCarouselDot();
      await welcomeModalPage.expectFinalSlideVisible();

      await welcomeModalPage.docsLink.click();
      await page.waitForTimeout(1000);

      const linkClickEvent = trackRequests.find(
        (req) => req.type === 'WelcomeToDataHubModalClickViewDocumentationEvent'
      );
      expect(linkClickEvent).toBeDefined();
      expect(linkClickEvent.url).toBe('https://docs.datahub.com/docs/category/features');
    });
  });

  test.describe('Edge Cases', () => {
    test('should handle rapid carousel navigation', async ({ welcomeModalPage }) => {
      await welcomeModalPage.navigateToHome();
      await welcomeModalPage.expectModalVisible();

      await welcomeModalPage.clickCarouselDot(1);
      await welcomeModalPage.clickCarouselDot(2);
      await welcomeModalPage.clickCarouselDot(0);

      await welcomeModalPage.expectSlide1Visible();
    });

    test('should reset carousel to first slide when reopened', async ({ welcomeModalPage }) => {
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

    test('should handle closing modal mid-carousel', async ({ welcomeModalPage }) => {
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
