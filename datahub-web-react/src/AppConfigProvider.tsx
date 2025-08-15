import '@src/App.less';

import React, { useEffect } from 'react';

import { SERVER_VERSION_KEY, THIRD_PARTY_LOGGING_KEY } from '@app/analytics/analytics';
import { checkAuthStatus } from '@app/auth/checkAuthStatus';
import { AppConfigContext, DEFAULT_APP_CONFIG } from '@src/appConfigContext';

import { useAppConfigQuery } from '@graphql/app.generated';

function changeFavicon(src) {
    const links = document.querySelectorAll("link[rel~='icon']") as any;
    if (!links || links.length === 0) {
        const link = document.createElement('link');
        link.rel = 'icon';
        document.getElementsByTagName('head')[0].appendChild(link);
    }
    links.forEach((link) => {
        // eslint-disable-next-line no-param-reassign
        link.href = src;
    });
}

const AppConfigProvider = ({ children }: { children: React.ReactNode }) => {
    const { data: appConfigData, refetch } = useAppConfigQuery({ fetchPolicy: 'no-cache' });

    const refreshAppConfig = () => {
        refetch();
    };

    useEffect(() => {
        if (appConfigData && appConfigData.appConfig) {
            if (appConfigData.appConfig.telemetryConfig.enableThirdPartyLogging) {
                localStorage.setItem(THIRD_PARTY_LOGGING_KEY, 'true');
                checkAuthStatus(); // identify in analyitcs once we receive config response
            } else {
                localStorage.setItem(THIRD_PARTY_LOGGING_KEY, 'false');
            }
            if (appConfigData.appConfig.appVersion) {
                localStorage.setItem(SERVER_VERSION_KEY, appConfigData.appConfig.appVersion);
            }
            changeFavicon(appConfigData.appConfig.visualConfig.faviconUrl);
        }
    }, [appConfigData]);

    return (
        <AppConfigContext.Provider
            value={{
                config: appConfigData?.appConfig || DEFAULT_APP_CONFIG,
                loaded: !!appConfigData,
                refreshContext: refreshAppConfig,
            }}
        >
            {children}
        </AppConfigContext.Provider>
    );
};

export default AppConfigProvider;
