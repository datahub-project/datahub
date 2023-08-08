import * as QueryString from 'query-string';
import { useHistory, useLocation } from 'react-router';
import React, { useEffect, useMemo, useState } from 'react';
import { SearchContext } from './SearchContext';
import { updateUrlParam } from '../utils/utils';
import { DEFAULT_SORT_OPTION, SORT_OPTIONS } from './constants';

export default function SearchContextProvider({ children }: { children: React.ReactNode }) {
    const [selectedSortOption, setSelectedSortOption] = useState(DEFAULT_SORT_OPTION);
    const history = useHistory();
    const location = useLocation();
    const params = useMemo(() => QueryString.parse(location.search, { arrayFormat: 'comma' }), [location.search]);
    const urlSortOption = params.sortOption as string;

    useEffect(() => {
        if (urlSortOption && urlSortOption !== selectedSortOption && urlSortOption in SORT_OPTIONS) {
            setSelectedSortOption(urlSortOption);
        }
    }, [urlSortOption, selectedSortOption]);

    function updateSelectedSortOption(selectedOption: string) {
        updateUrlParam(history, 'sortOption', selectedOption);
        setSelectedSortOption(selectedOption);
    }

    return (
        <SearchContext.Provider value={{ selectedSortOption, setSelectedSortOption: updateSelectedSortOption }}>
            {children}
        </SearchContext.Provider>
    );
}
