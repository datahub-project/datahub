import { useQuickFiltersContext } from '@src/providers/QuickFiltersContext';
import { useCallback } from 'react';
import { useHistory } from 'react-router';
import analytics, { EventType } from '../analytics';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';

export default function useSearchViewAll() {
    const history = useHistory();
    const { setSelectedQuickFilter } = useQuickFiltersContext();

    return useCallback(() => {
        analytics.event({ type: EventType.SearchBarExploreAllClickEvent });
        setSelectedQuickFilter(null);
        navigateToSearchUrl({ query: '*', history });
    }, [history, setSelectedQuickFilter]);
}
