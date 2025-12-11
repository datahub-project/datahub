/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
