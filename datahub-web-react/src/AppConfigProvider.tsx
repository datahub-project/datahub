import React from 'react';
import './App.less';
import { AppConfigContext, DEFAULT_APP_CONFIG } from './appConfigContext';
import { useAppConfigQuery } from './graphql/app.generated';

const AppConfigProvider = ({ children }: { children: React.ReactNode }) => {
    const { data: appConfigData, refetch } = useAppConfigQuery();

    const refreshAppConfig = () => {
        refetch();
    };

    return (
        <AppConfigContext.Provider
            value={{ config: appConfigData?.appConfig || DEFAULT_APP_CONFIG, refreshContext: refreshAppConfig }}
        >
            {children}
        </AppConfigContext.Provider>
    );
};

export default AppConfigProvider;
