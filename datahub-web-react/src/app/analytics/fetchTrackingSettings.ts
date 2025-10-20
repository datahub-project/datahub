/**
 * Fetch analytics configurations from the GraphQL backend.
 */
import { resolveRuntimePath } from '@utils/runtimeBasePath';

export async function fetchTrackingSettings() {
    const { NODE_ENV } = import.meta.env;

    // Skip making requests during unit tests
    if (NODE_ENV === 'test') {
        return null;
    }

    const query = `
      query AppConfigQuery {
        appConfig {
          telemetryConfig {
            enableThirdPartyLogging
            mixpanel {
              enabled
              token
            }
            googleAnalytics {
              enabled
              measurementId
            }
          }
        }
      }
    `;

    const response = await fetch(resolveRuntimePath('/api/v2/graphql'), {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
            query,
        }),
    });

    const result = await response.json();

    if (result.errors) {
        console.error(`Failed to fetch tracking settings`, result.errors);
        return null;
    }

    const telemetryConfig = result.data?.appConfig?.telemetryConfig;

    if (!telemetryConfig) {
        throw new Error('No telemetry configuration found in the response.');
    }

    // Extract the telemetry settings
    const { mixpanel, googleAnalytics, enableThirdPartyLogging } = telemetryConfig;

    return {
        mixpanel,
        googleAnalytics,
        enableThirdPartyLogging,
    };
}
