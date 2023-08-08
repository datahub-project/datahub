import React, { useState } from 'react';
import { SearchContext } from './SearchContext';
import { SortOrder } from '../../../types.generated';

export const RECOMMENDED = 'recommended';
const NAME_FIELD = 'name';

const DEFAULT_SORT_OPTION = RECOMMENDED;

export const SORT_OPTIONS = {
    [RECOMMENDED]: { label: 'Recommended', field: RECOMMENDED, sortOrder: SortOrder.Descending },
    [`${NAME_FIELD}_${SortOrder.Ascending}`]: { label: 'A to Z', field: NAME_FIELD, sortOrder: SortOrder.Ascending },
    [`${NAME_FIELD}_${SortOrder.Descending}`]: { label: 'Z to A', field: NAME_FIELD, sortOrder: SortOrder.Descending },
};

export default function SearchContextProvider({ children }: { children: React.ReactNode }) {
    const [selectedSortOption, setSelectedSortOption] = useState(DEFAULT_SORT_OPTION);

    return (
        <SearchContext.Provider value={{ selectedSortOption, setSelectedSortOption }}>
            {children}
        </SearchContext.Provider>
    );
}
