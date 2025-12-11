/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect } from 'react';

import { useAppConfig } from '@app/useAppConfig';
import { useIsThemeV2 } from '@app/useIsThemeV2';

export function useShowNavBarRedesign() {
    const appConfig = useAppConfig();
    const isThemeV2Enabled = useIsThemeV2();

    if (!appConfig.loaded) {
        return loadFromLocalStorage();
    }

    return appConfig.config.featureFlags.showNavBarRedesign && isThemeV2Enabled;
}

export function useSetNavBarRedesignEnabled() {
    const isNavBarRedesignEnabled = useShowNavBarRedesign();

    useEffect(() => {
        setThemeV2LocalStorage(isNavBarRedesignEnabled);
    }, [isNavBarRedesignEnabled]);
}

function setThemeV2LocalStorage(isNavBarRedesignEnabled: boolean) {
    if (loadFromLocalStorage() !== isNavBarRedesignEnabled) {
        saveToLocalStorage(isNavBarRedesignEnabled);
    }
}

const NAV_BAR_REDESIGN_STATUS_KEY = 'isNavBarRedesignEnabled';

function loadFromLocalStorage(): boolean {
    return localStorage.getItem(NAV_BAR_REDESIGN_STATUS_KEY) === 'true';
}

function saveToLocalStorage(isNavBarRedesignEnabled: boolean) {
    localStorage.setItem(NAV_BAR_REDESIGN_STATUS_KEY, String(isNavBarRedesignEnabled));
}
