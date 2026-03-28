import { test, expect } from '../../fixtures/test-context';

test.describe('Business Attribute Feature Flag', () => {
  test('should check if business attribute feature is enabled', async ({ page, graphqlHelper }) => {
    const query = `
      query {
        appConfig {
          featureFlags {
            businessAttributeEntityEnabled
          }
        }
      }
    `;

    const response = await graphqlHelper.executeQuery(query);

    console.log('Feature flag response:', JSON.stringify(response, null, 2));

    const businessAttributeEnabled =
      response?.data?.appConfig?.featureFlags?.businessAttributeEntityEnabled;

    console.log('Business Attribute Enabled:', businessAttributeEnabled);

    if (!businessAttributeEnabled) {
      console.log('');
      console.log('ℹ️  Business Attribute feature is disabled');
      console.log('');
      console.log('To enable it, set the environment variable and restart DataHub:');
      console.log('  export BUSINESS_ATTRIBUTE_ENTITY_ENABLED=true');
      console.log('  ./gradlew quickstartDebug');
      console.log('');
      test.skip(!businessAttributeEnabled, 'Business Attribute feature is not enabled');
    }

    expect(businessAttributeEnabled).toBe(true);
  });
});
