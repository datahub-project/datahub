import React, { useMemo } from 'react';
import { EXACT_SEARCH_PREFIX } from '../../utils/constants';
import ViewAllResults from '../components/ViewAllResults';
import { EXACT_AUTOCOMPLETE_OPTION_TYPE } from '../constants';

export default function useViewAllResultsOptions(query: string, shouldShow?: boolean) {
    return useMemo(() => {
        if (query === '' || !shouldShow) return [];

        return [
            {
                value: `${EXACT_SEARCH_PREFIX}${query}`,
                label: <ViewAllResults searchText={query} />,
                type: EXACT_AUTOCOMPLETE_OPTION_TYPE,
            },
        ];
    }, [query, shouldShow]);
}
