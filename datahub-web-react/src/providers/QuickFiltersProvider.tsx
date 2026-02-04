import React, { useEffect, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { QuickFiltersContext } from '@providers/QuickFiltersContext';

import { useGetQuickFiltersQuery } from '@graphql/quickFilters.generated';
import { QuickFilter } from '@types';

export default function QuickFiltersProvider({ children }: { children: React.ReactNode }) {
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;

    const { data } = useGetQuickFiltersQuery({
        variables: { input: { viewUrn } },
        fetchPolicy: 'cache-first',
    });
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
