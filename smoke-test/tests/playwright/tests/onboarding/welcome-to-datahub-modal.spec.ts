import { test, expect, Page } from '@playwright/test';
import { 
  loginForOnboarding, 
  skipIntroducePage, 
  enableThemeV2, 
  clearOnboardingStorage,
  interceptTrackingEvents 
} from '../utils';

interface TrackingEvent {
  type: string;
  exitMethod?: string;
  currentSlide?: number;
  totalSlides?: number;
}

test.describe('WelcomeToDataHubModal', () => {
  const SKIP_WELCOME_MODAL_KEY = 'skipWelcomeModal';

  test.beforeEach(async ({ page }: { page: Page }) => {
    // Navigate to the app first to avoid localStorage security errors
    await page.goto('/');
    // Clear localStorage and set up initial state
    await clearOnboardingStorage(page);
    await skipIntroducePage(page);
    await enableThemeV2(page);
  });

  test.afterEach(async ({ page }: { page: Page }) => {
    await clearOnboardingStorage(page);
  });

  test('should display the modal for first-time users', async ({ page }: { page: Page }) => {
    // Intercept tracking events
    await page.route('**/track**', (route) => route.fulfill({ status: 200 }));

    // Login for onboarding
    await loginForOnboarding(page);
    await page.goto('/');

    // Wait for and verify modal is visible
    const modal = page.getByRole('dialog');
    await expect(modal).toBeVisible();

    // Click the last carousel dot
    const carouselDots = modal.locator('li');
    const lastDot = carouselDots.last();
    await lastDot.click();

    // Click Get Started button
    const getStartedButton = page.getByRole('button', { name: /get started/i });
    await getStartedButton.click();
    
    // Verify modal is closed
    await expect(modal).not.toBeVisible();

    // Verify localStorage was set
    const skipModalValue = await page.evaluate((key: string) => {
      try {
        return localStorage.getItem(key);
      } catch (e) {
        console.warn('Failed to read localStorage:', e);
        return null;
      }
    }, SKIP_WELCOME_MODAL_KEY);
    
    expect(skipModalValue).toBe('true');
  });

  test('should not display the modal if user has already seen it', async ({ page }: { page: Page }) => {
    // Set localStorage to indicate user has already seen modal
    await page.evaluate((key: string) => {
      try {
        localStorage.setItem(key, 'true');
      } catch (e) {
        console.warn('Failed to set localStorage:', e);
      }
    }, SKIP_WELCOME_MODAL_KEY);

    // Login for onboarding
    await loginForOnboarding(page);
    await page.goto('/');

    // Verify modal does not exist
    const modal = page.getByRole('dialog');
    await expect(modal).not.toBeVisible();
  });

  test('should handle user interactions and track events', async ({ page }: { page: Page }) => {
    // Set up tracking event interception
    const trackEvents = await interceptTrackingEvents(page);

    // Login for onboarding
    await loginForOnboarding(page);
    await page.goto('/');

    // Wait for modal to be visible
    const modal = page.getByRole('dialog');
    await expect(modal).toBeVisible();
    
    // Click close button - use more specific selector since getByLabelText may not exist
    const closeButton = page.locator('[aria-label*="close" i], [title*="close" i], .ant-modal-close');
    await closeButton.first().click();
    
    // Verify modal is closed
    await expect(modal).not.toBeVisible();

    // Wait for tracking events and verify the Exit event
    await page.waitForTimeout(1000); // Give time for events to be captured
    
    const modalExitEvent = trackEvents.find(
      (event: TrackingEvent) => event.type === 'WelcomeToDataHubModalExitEvent'
    );

    expect(modalExitEvent).toBeDefined();
    expect(modalExitEvent?.exitMethod).toBe('close_button');
    expect(typeof modalExitEvent?.currentSlide).toBe('number');
    expect(typeof modalExitEvent?.totalSlides).toBe('number');

    // Verify localStorage was set
    const skipModalValue = await page.evaluate((key: string) => {
      try {
        return localStorage.getItem(key);
      } catch (e) {
        console.warn('Failed to read localStorage:', e);
        return null;
      }
    }, SKIP_WELCOME_MODAL_KEY);
    
    expect(skipModalValue).toBe('true');
  });
});