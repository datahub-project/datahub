import React, { useMemo } from 'react';
<<<<<<< HEAD

import ViewAllResults from '@app/searchV2/searchBarV2/components/ViewAllResults';
import { EXACT_AUTOCOMPLETE_OPTION_TYPE } from '@app/searchV2/searchBarV2/constants';
import { EXACT_SEARCH_PREFIX } from '@app/searchV2/utils/constants';
=======
import { EXACT_SEARCH_PREFIX } from '../../utils/constants';
import ViewAllResults from '../components/ViewAllResults';
import { EXACT_AUTOCOMPLETE_OPTION_TYPE } from '../constants';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
