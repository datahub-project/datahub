import React from 'react';
import { AppConfig } from './types.generated';

export const DEFAULT_APP_CONFIG = {
    analyticsConfig: {
        enabled: false,
    },
    policiesConfig: {
        enabled: false,
        platformPrivileges: [],
        resourcePrivileges: [],
    },
};

export const AppConfigContext = React.createContext<{
    config: AppConfig;
    refreshContext: () => void;
}>({ config: DEFAULT_APP_CONFIG, refreshContext: () => null });
