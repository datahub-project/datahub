/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext } from 'react';

import { GetHomePageSettingsQuery } from '@graphql/app.generated';

export const DEFAULT_GLOBAL_SETTINGS = {
    globalHomePageSettings: {
        defaultTemplate: null,
    },
};

export const GlobalSettingsContext = React.createContext<{
    settings: GetHomePageSettingsQuery;
    loaded: boolean;
}>({ settings: DEFAULT_GLOBAL_SETTINGS, loaded: false });

export function useGlobalSettings() {
    return useContext(GlobalSettingsContext);
}
