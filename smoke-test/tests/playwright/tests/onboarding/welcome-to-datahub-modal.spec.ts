import { test, expect, Page } from '@playwright/test';
import { 
  loginForOnboarding, 
  skipOnboardingTour, 
  enableThemeV2,
  interceptTrackingEvents, 
  skipWelcomeModal
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
    await skipOnboardingTour(page);
    await enableThemeV2(page);
  });

  test('should display the modal for first-time users', async ({ page }: { page: Page }) => {
    // Intercept tracking events
    await page.route('**/track**', (route) => route.fulfill({ status: 200 }));

    // Login for onboarding
    await loginForOnboarding(page);
    await page.goto('/');

    // Wait for and verify modal is visible
    let modal = page.getByRole('dialog');
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

    // Login for onboarding
    await loginForOnboarding(page);
    await page.goto('/');

    // Verify modal does not exist
    modal = page.getByRole('dialog');
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
    
    // Click close button
    await page.getByLabel('Close').first().click();
    
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