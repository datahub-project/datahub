/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useContext } from 'react';

import { AppConfigContext } from '@src/appConfigContext';

/**
 * Fetch an instance of AppConfig from the React context.
 */
export function useAppConfig() {
    return useContext(AppConfigContext);
}

export function useIsShowAcrylInfoEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showAcrylInfo;
}

export function useIsNestedDomainsEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.nestedDomainsEnabled;
}

export function useBusinessAttributesFlag() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.businessAttributeEntityEnabled;
}

export function useIsAppConfigContextLoaded() {
    const appConfig = useAppConfig();
    return appConfig.loaded;
}

export function useIsEditableDatasetNameEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.editableDatasetNameEnabled;
}

export function useIsShowSeparateSiblingsEnabled() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showSeparateSiblings;
}

export function useShowIntroducePage() {
    const appConfig = useAppConfig();
    return appConfig.config.featureFlags.showIntroducePage;
}
