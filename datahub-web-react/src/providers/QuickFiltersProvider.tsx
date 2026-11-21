import React, { useEffect, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useIsSearchStartupReleased } from '@app/searchV2/useIsSearchStartupReleased';
import { QuickFiltersContext } from '@providers/QuickFiltersContext';

import { useGetQuickFiltersQuery } from '@graphql/quickFilters.generated';
import { QuickFilter } from '@types';

export default function QuickFiltersProvider({ children }: { children: React.ReactNode }) {
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;
    const isSearchStartupReleased = useIsSearchStartupReleased();

    // Variables change with the selected view, which re-runs the query. The result is rendered
    // in the search bar dropdown only, so it can wait for the first /search results.
    const { data } = useGetQuickFiltersQuery({
        variables: { input: { viewUrn } },
        skip: !isSearchStartupReleased,
        fetchPolicy: 'cache-and-network',
    });
    const [quickFilters, setQuickFilters] = useState<QuickFilter[] | null>(null);
    const [selectedQuickFilter, setSelectedQuickFilter] = useState<QuickFilter | null>(null);

    useEffect(() => {
        const latestQuickFilters = data?.getQuickFilters?.quickFilters;
        if (latestQuickFilters) {
            setQuickFilters(latestQuickFilters as QuickFilter[]);
        }
    }, [data]);

    return (
        <QuickFiltersContext.Provider
            value={{ quickFilters, setQuickFilters, selectedQuickFilter, setSelectedQuickFilter }}
        >
            {children}
        </QuickFiltersContext.Provider>
    );
}
