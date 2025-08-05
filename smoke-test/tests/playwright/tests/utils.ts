import { Page, Route } from '@playwright/test';

export const SKIP_INTRODUCE_PAGE_KEY = "skipAcrylIntroducePage";
export const SKIP_ONBOARDING_TOUR_KEY = "skipOnboardingTour";
export const SKIP_WELCOME_MODAL_KEY = "skipWelcomeModal";
export const THEME_V2_STATUS_KEY = "isThemeV2Enabled";

/**
 * Utility functions for Playwright tests
 */

interface GraphQLRequest {
  operationName?: string;
  query?: string;
  variables?: Record<string, any>;
}

interface GraphQLResponse {
  data: {
    appConfig: {
      featureFlags: {
        themeV2Enabled: boolean;
        themeV2Default: boolean;
      };
    };
  };
}

interface TrackingEvent {
  type: string;
  exitMethod?: string;
  currentSlide?: number;
  totalSlides?: number;
  [key: string]: any;
}

export const hasOperationName = (postData: GraphQLRequest | null, operationName: string): boolean => {
  return postData?.operationName === operationName;
};

/**
 * Helper to intercept and modify GraphQL responses
 */
export const interceptGraphQL = async (
  page: Page, 
  operationName: string, 
  modifier?: (json: GraphQLResponse) => void
): Promise<void> => {
  await page.route('/api/v2/graphql', async (route: Route) => {
    const request = route.request();
    const postData = request.postDataJSON() as GraphQLRequest;
    
    if (hasOperationName(postData, operationName)) {
      const response = await route.fetch();
      const json = await response.json() as GraphQLResponse;
      
      if (modifier) {
        modifier(json);
      }
      
      await route.fulfill({ response, json });
    } else {
      await route.continue();
    }
  });
};

/**
 * Login for onboarding tests (without setting skip flags)
 */
export const loginForOnboarding = async (
  page: Page, 
  username?: string, 
  password?: string
): Promise<void> => {
  await page.request.post('/logIn', {
    data: {
      username: username || process.env.ADMIN_USERNAME || 'datahub',
      password: password || process.env.ADMIN_PASSWORD || 'datahub'
    }
  });
};


export const setLocalStorage = async (page: Page, key: string, value?: string): Promise<void> => {
  await page.evaluate(({ key, value }) => {
    try {
      localStorage.setItem(key, value || 'true');
    } catch (e) {
      console.warn('Failed to set localStorage:', e);
    }
  }, { key, value });
};

/**
 * Skip onboarding tour
 */
export const skipOnboardingTour = async (page: Page): Promise<void> => {
  await setLocalStorage(page, SKIP_ONBOARDING_TOUR_KEY);
};


/**
 * Skip welcome modal
 */
export const skipWelcomeModal = async (page: Page): Promise<void> => {
  await setLocalStorage(page, SKIP_WELCOME_MODAL_KEY);
};

/**
 * Skip introduce page
 */
export const skipIntroducePage = async (page: Page): Promise<void> => {
  await setLocalStorage(page, SKIP_INTRODUCE_PAGE_KEY);
};

/**
 * Enable theme V2
 */
export const enableThemeV2 = async (page: Page): Promise<void> => {
  await setLocalStorage(page, THEME_V2_STATUS_KEY);

  await interceptGraphQL(page, 'appConfig', (json: GraphQLResponse) => {
    json.data.appConfig.featureFlags.themeV2Enabled = true;
    json.data.appConfig.featureFlags.themeV2Default = true;
  });
};

/**
 * Set up tracking event interception
 */
export const interceptTrackingEvents = async (page: Page): Promise<TrackingEvent[]> => {
  const events: TrackingEvent[] = [];
  
  await page.route('**/track**', (route: Route) => {
    const request = route.request();
    const postData = request.postDataJSON() as TrackingEvent;
    events.push(postData);
    route.fulfill({ status: 200 });
  });
  
  return events;
};