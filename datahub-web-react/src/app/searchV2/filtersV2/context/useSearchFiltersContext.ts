import { useContext } from 'react';
import SearchFiltersContext from './SearchFiltersContext';

export default function useSearchFiltersContext() {
    return useContext(SearchFiltersContext);
}
