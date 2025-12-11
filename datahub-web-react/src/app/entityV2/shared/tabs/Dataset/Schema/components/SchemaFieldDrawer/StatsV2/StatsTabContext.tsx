/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { createContext } from 'react';

import { StatsProps } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsSidebarView';

interface ContextProps {
    properties?: StatsProps['properties'];
}

export const StatsSectionsContext = createContext<ContextProps>({});

interface ProviderProps {
    properties: StatsProps['properties'];
    children: React.ReactNode;
}

export const StatsTabContextProvider = ({ properties, children }: ProviderProps) => {
    return <StatsSectionsContext.Provider value={{ properties }}>{children}</StatsSectionsContext.Provider>;
};
