/// <reference types="vite/client" />
/// <reference types="vite-plugin-svgr/client" />
/// <reference types="@testing-library/jest-dom" />
import type { FeatureFlagsConfig } from '@src/types.generated';

declare global {
    interface Window {
        datahub?: {
            features?: FeatureFlagsConfig & {
                _loaded?: string;
                _version?: string;
            };
            appConfig?: AppConfig;
        };
    }
}
