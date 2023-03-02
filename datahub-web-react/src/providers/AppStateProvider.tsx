import React, { useEffect, useState } from 'react';
import { AppStateContext } from './AppStateContext';
import { QuickFilter } from '../types.generated';
import { useGetQuickFiltersQuery } from '../graphql/quickFilters.generated';

export default function AppStateProvider({ children }: { children: React.ReactNode }) {
    const { data } = useGetQuickFiltersQuery({ variables: { input: {} } });
    const [quickFilters, setQuickFilters] = useState<QuickFilter[] | null>(null);
    const [selectedQuickFilter, setSelectedQuickFilter] = useState<QuickFilter | null>(null);

    useEffect(() => {
        if (data && data.getQuickFilters && quickFilters === null) {
            setQuickFilters(data.getQuickFilters.quickFilters as QuickFilter[]);
        }
    }, [data, quickFilters]);

    return (
        <AppStateContext.Provider
            value={{ quickFilters, setQuickFilters, selectedQuickFilter, setSelectedQuickFilter }}
        >
            {children}
        </AppStateContext.Provider>
    );
}
