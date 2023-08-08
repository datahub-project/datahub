import { useContext } from 'react';
import { SearchContext } from './SearchContext';

export function useSearchContext() {
    return useContext(SearchContext);
}
