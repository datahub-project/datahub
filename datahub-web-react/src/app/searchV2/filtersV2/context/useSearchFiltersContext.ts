import { useContext } from 'react';
<<<<<<< HEAD

import SearchFiltersContext from '@app/searchV2/filtersV2/context/SearchFiltersContext';
=======
import SearchFiltersContext from './SearchFiltersContext';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

export default function useSearchFiltersContext() {
    return useContext(SearchFiltersContext);
}
