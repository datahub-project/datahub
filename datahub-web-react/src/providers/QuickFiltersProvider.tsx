import React, { useEffect, useState } from 'react';
import { QuickFiltersContext } from './QuickFiltersContext';
import { QuickFilter } from '../types.generated';
import { useGetQuickFiltersQuery } from '../graphql/quickFilters.generated';

export default function QuickFiltersProvider({ children }: { children: React.ReactNode }) {
    const { data } = useGetQuickFiltersQuery({ variables: { input: {} } });
    const [quickFilters, setQuickFilters] = useState<QuickFilter[] | null>(null);
    const [selectedQuickFilter, setSelectedQuickFilter] = useState<QuickFilter | null>(null);

    useEffect(() => {
        if (data && data.getQuickFilters && quickFilters === null) {
            setQuickFilters(data.getQuickFilters.quickFilters as QuickFilter[]);
        }
    }, [data, quickFilters]);

    return (
        <QuickFiltersContext.Provider
            value={{ quickFilters, setQuickFilters, selectedQuickFilter, setSelectedQuickFilter }}
        >
            {children}
        </QuickFiltersContext.Provider>
    );
}
