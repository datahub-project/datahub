import { useContext } from 'react';

import SearchFiltersContext from '@app/searchV2/filtersV2/context/SearchFiltersContext';

export default function useSearchFiltersContext() {
    return useContext(SearchFiltersContext);
}
