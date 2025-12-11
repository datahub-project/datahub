/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { DEFAULT_GLOBAL_SETTINGS, GlobalSettingsContext } from '@app/context/GlobalSettingsContext';

import { useGetHomePageSettingsQuery } from '@graphql/app.generated';

export default function GlobalSettingsProvider({ children }: { children: React.ReactNode }) {
    const { data } = useGetHomePageSettingsQuery();

    return (
        <GlobalSettingsContext.Provider
            value={{
                settings: data || DEFAULT_GLOBAL_SETTINGS,
                loaded: !!data,
            }}
        >
            {children}
        </GlobalSettingsContext.Provider>
    );
}
