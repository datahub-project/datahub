/// <reference types="vite/client" />
/// <reference types="vite-plugin-svgr/client" />
/// <reference types="@testing-library/jest-dom" />
import type { FeatureFlags } from '@src/appConfigContext';

declare global {
    interface Window {
        datahub?: {
            features?: FeatureFlags & {
                _loaded?: string;
                _version?: string;
            };
            appConfig?: AppConfig;
        };
    }
}
