import { useCallback } from 'react';
import { useHistory } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { useQuickFiltersContext } from '@src/providers/QuickFiltersContext';

export default function useSearchViewAll() {
    const history = useHistory();
    const { setSelectedQuickFilter } = useQuickFiltersContext();

    return useCallback(() => {
        analytics.event({ type: EventType.SearchBarExploreAllClickEvent });
        setSelectedQuickFilter(null);
        navigateToSearchUrl({ query: '*', history });
    }, [history, setSelectedQuickFilter]);
}
