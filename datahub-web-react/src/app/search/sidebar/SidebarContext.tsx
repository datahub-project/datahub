import React, { ReactNode, createContext, useContext } from 'react';
import { FacetFilterInput } from '../../../types.generated';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    ENTITY_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    PLATFORM_FILTER_NAME,
} from '../utils/constants';

type SidebarContextValue = {
    selectedFilters: Array<FacetFilterInput>;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
};

const SidebarContext = createContext<SidebarContextValue | null>(null);

type Props = {
    children: ReactNode;
    selectedFilters: Array<FacetFilterInput>;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
};

export const SidebarProvider = ({ children, selectedFilters, onChangeFilters }: Props) => {
    return (
        <SidebarContext.Provider
            value={{
                selectedFilters,
                onChangeFilters,
            }}
        >
            {children}
        </SidebarContext.Provider>
    );
};

const useSidebarContext = () => {
    const context = useContext(SidebarContext);
    if (context === null) throw new Error(`${useSidebarContext.name} must be used under a ${SidebarProvider.name}`);
    return context;
};

export const useSelectedFilters = () => {
    return useSidebarContext().selectedFilters;
};

// todo - clean up these .values[0] things because they are not foolproof, ie. we need an "in" check
export const useEntityFilterValue = () => {
    return useSelectedFilters().find((filter) => filter.field === ENTITY_FILTER_NAME)?.values?.[0];
};

export const useEnvironmentFilterValue = () => {
    return useSelectedFilters().find((filter) => filter.field === ORIGIN_FILTER_NAME)?.values?.[0];
};

export const usePlatformFilterValue = () => {
    return useSelectedFilters().find((filter) => filter.field === PLATFORM_FILTER_NAME)?.values?.[0];
};

export const useBrowseFilterValue = () => {
    return useSelectedFilters().find((filter) => filter.field === BROWSE_PATH_V2_FILTER_NAME)?.values?.[0];
};

export const useOnChangeFilters = () => {
    return useSidebarContext().onChangeFilters;
};
