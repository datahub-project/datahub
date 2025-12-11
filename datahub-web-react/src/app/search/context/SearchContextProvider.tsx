/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';
import React, { useCallback, useMemo, useState } from 'react';
import { useHistory, useLocation } from 'react-router';

import { SearchContext } from '@app/search/context/SearchContext';
import { updateUrlParam } from '@app/shared/updateUrlParam';

export default function SearchContextProvider({ children }: { children: React.ReactNode }) {
    const history = useHistory();
    const location = useLocation();
    const params = useMemo(() => QueryString.parse(location.search, { arrayFormat: 'comma' }), [location.search]);
    const query = (params.query ? decodeURIComponent(params.query as string) : undefined) as string | undefined;
    const selectedSortOption = params.sortOption as string | undefined;

    function setSelectedSortOption(selectedOption: string) {
        updateUrlParam(history, 'sortOption', selectedOption);
    }

    const localStorageIsFullCardView = localStorage.getItem('isFullViewCard');
    const [isFullViewCard, setIsFullViewCardState] = useState(
        localStorageIsFullCardView === null ? true : localStorageIsFullCardView === 'true',
    );

    const setIsFullViewCard = useCallback((value: boolean) => {
        setIsFullViewCardState(value);
        localStorage.setItem('isFullViewCard', value.toString());
    }, []);

    return (
        <SearchContext.Provider
            value={{
                query,
                selectedSortOption,
                isFullViewCard,
                setSelectedSortOption,
                setIsFullViewCard,
            }}
        >
            {children}
        </SearchContext.Provider>
    );
}
