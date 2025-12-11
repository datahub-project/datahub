/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { QuickFiltersContext } from '@providers/QuickFiltersContext';

import { useGetQuickFiltersQuery } from '@graphql/quickFilters.generated';
import { QuickFilter } from '@types';

export default function QuickFiltersProvider({ children }: { children: React.ReactNode }) {
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;

    const { data, refetch } = useGetQuickFiltersQuery({ variables: { input: { viewUrn } } });
    const [quickFilters, setQuickFilters] = useState<QuickFilter[] | null>(null);
    const [selectedQuickFilter, setSelectedQuickFilter] = useState<QuickFilter | null>(null);

    useEffect(() => {
        if (data && data.getQuickFilters && quickFilters === null) {
            setQuickFilters(data.getQuickFilters.quickFilters as QuickFilter[]);
        }
    }, [data, quickFilters]);

    // refetch and update quick filters whenever viewUrn changes
    useEffect(() => {
        refetch({ input: { viewUrn } }).then((result) => {
            if (result.data.getQuickFilters?.quickFilters) {
                setQuickFilters(result.data.getQuickFilters.quickFilters as QuickFilter[]);
            }
        });
    }, [viewUrn, refetch]);

    return (
        <QuickFiltersContext.Provider
            value={{ quickFilters, setQuickFilters, selectedQuickFilter, setSelectedQuickFilter }}
        >
            {children}
        </QuickFiltersContext.Provider>
    );
}
