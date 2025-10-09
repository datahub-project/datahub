import * as Sentry from '@sentry/react';
import { useEffect } from 'react';

import { useAppConfig } from '@app/useAppConfig';

/**
 * Hook to conditionally initialize Sentry based on telemetry configuration.
 * Only initializes Sentry if third-party logging is enabled in the app config.
 */
export const useSentryInit = () => {
    const { config, loaded } = useAppConfig();

    useEffect(() => {
        // Only proceed if config is loaded and third-party logging is enabled
        if (loaded && config.telemetryConfig.enableThirdPartyLogging) {
            Sentry.init({
                dsn: 'https://50799ff93031aceb3246b8b31ca063ad@o4504487219363840.ingest.us.sentry.io/4508738535424000',
                defaultIntegrations: false, // no default error/performance capture
            });
        }
    }, [loaded, config.telemetryConfig.enableThirdPartyLogging]);
};
