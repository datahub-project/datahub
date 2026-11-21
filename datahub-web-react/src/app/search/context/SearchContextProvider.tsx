import * as QueryString from 'query-string';
import React, { useCallback, useMemo, useState } from 'react';
import { useHistory, useLocation } from 'react-router';

import { SearchContext } from '@app/search/context/SearchContext';
import { updateUrlParam } from '@app/sharedV2/updateUrlParam';

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

    // Latches once the first /search results request has settled, so startup queries held
    // behind it (see useIsSearchStartupReleased) are never held again for this page load.
    const [isSearchSettled, setIsSearchSettled] = useState(false);
    const markSearchSettled = useCallback(() => setIsSearchSettled(true), []);

    return (
        <SearchContext.Provider
            value={{
                query,
                selectedSortOption,
                isFullViewCard,
                setSelectedSortOption,
                setIsFullViewCard,
                isSearchSettled,
                markSearchSettled,
            }}
        >
            {children}
        </SearchContext.Provider>
    );
}
