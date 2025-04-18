import React, { ReactNode, createContext, useContext } from 'react';
import { FacetFilterInput, FilterOperator } from '../../../types.generated';

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

export const useHasFilterField = (field: string) => {
    const selectedFilters = useSelectedFilters();
    return selectedFilters.some(
        (filter) => filter.field === field && filter.condition === FilterOperator.Equal && !filter.negated,
    );
};

export const useHasFilterValue = (field: string, value: string | undefined, { prefix = false } = {}) => {
    const selectedFilters = useSelectedFilters();
    return (
        !!value &&
        selectedFilters.some(
            (filter) =>
                filter.field === field &&
                filter.condition === FilterOperator.Equal &&
                !filter.negated &&
                (prefix ? filter.values?.some((f) => f.startsWith(value)) : filter.values?.includes(value)),
        )
    );
};

export const useOnChangeFilters = () => {
    return useSidebarContext().onChangeFilters;
};
