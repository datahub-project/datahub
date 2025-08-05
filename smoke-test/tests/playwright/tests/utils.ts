import { Page, Route } from '@playwright/test';

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

/**
 * Skip onboarding tour
 */
export const skipOnboardingTour = async (page: Page): Promise<void> => {
  await page.evaluate(() => {
    try {
      localStorage.setItem('skipOnboardingTour', 'true');
    } catch (e) {
      console.warn('Failed to set localStorage:', e);
    }
  });
};


/**
 * Skip onboarding tour
 */
export const skipWelcomeModal = async (page: Page): Promise<void> => {
  await page.evaluate(() => {
    try {
      localStorage.setItem('skipWelcomeModal', 'true');
    } catch (e) {
      console.warn('Failed to set localStorage:', e);
    }
  });
};
/**
 * Set up theme V2 configuration
 */
export const enableThemeV2 = async (page: Page): Promise<void> => {
  await page.evaluate(() => {
    try {
      localStorage.setItem('isThemeV2Enabled', 'true');
    } catch (e) {
      console.warn('Failed to set localStorage:', e);
    }
  });

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