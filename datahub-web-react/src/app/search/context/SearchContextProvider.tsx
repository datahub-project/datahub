import * as QueryString from 'query-string';
import { useHistory, useLocation } from 'react-router';
import React, { useMemo } from 'react';
import { SearchContext } from './SearchContext';
import { updateUrlParam } from '../../shared/updateUrlParam';

export default function SearchContextProvider({ children }: { children: React.ReactNode }) {
    const history = useHistory();
    const location = useLocation();
    const params = useMemo(() => QueryString.parse(location.search, { arrayFormat: 'comma' }), [location.search]);
    const selectedSortOption = params.sortOption as string | undefined;

    function setSelectedSortOption(selectedOption: string) {
        updateUrlParam(history, 'sortOption', selectedOption);
    }

    return (
        <SearchContext.Provider value={{ selectedSortOption, setSelectedSortOption }}>
            {children}
        </SearchContext.Provider>
    );
}
