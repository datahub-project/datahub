import React, { useMemo } from 'react';

import ViewAllResults from '@app/searchV2/searchBarV2/components/ViewAllResults';
import { EXACT_AUTOCOMPLETE_OPTION_TYPE } from '@app/searchV2/searchBarV2/constants';
import { EXACT_SEARCH_PREFIX } from '@app/searchV2/utils/constants';

export default function useViewAllResultsOptions(query: string, shouldShow?: boolean) {
    return useMemo(() => {
        if (query === '' || !shouldShow) return [];

        return [
            {
                value: `${EXACT_SEARCH_PREFIX}${query}`,
                label: <ViewAllResults searchText={query} dataTestId="view-all-results" />,
                type: EXACT_AUTOCOMPLETE_OPTION_TYPE,
            },
        ];
    }, [query, shouldShow]);
}
