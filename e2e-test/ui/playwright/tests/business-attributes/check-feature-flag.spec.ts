/**
 * Business Attribute feature flag diagnostic test.
 *
 * Uses base-test (authenticated context). Queries the feature flag directly
 * via page.request — no GraphQLHelper fixture needed.
 */

import { test, expect } from '../../fixtures/base-test';

test.describe('Business Attribute Feature Flag', () => {
  test('should check if business attribute feature is enabled', async ({ page, logger }) => {
    const response = await page.request.post('/api/v2/graphql', {
      data: {
        query: `query { appConfig { featureFlags { businessAttributeEntityEnabled } } }`,
        variables: {},
      },
      headers: { 'Content-Type': 'application/json' },
    });

    const json = (await response.json()) as {
      data?: { appConfig?: { featureFlags?: { businessAttributeEntityEnabled?: boolean } } };
    };
    const businessAttributeEnabled = json?.data?.appConfig?.featureFlags?.businessAttributeEntityEnabled;

    logger.info('feature flag check', { businessAttributeEnabled });

    if (!businessAttributeEnabled) {
      logger.warn(
        'Business Attribute feature is disabled — set BUSINESS_ATTRIBUTE_ENTITY_ENABLED=true and restart DataHub',
      );
      test.skip(true, 'Business Attribute feature is not enabled');
    }

    expect(businessAttributeEnabled).toBe(true);
  });
});
