import React, { createContext } from 'react';
import { StatsProps } from '../StatsSidebarView';

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
