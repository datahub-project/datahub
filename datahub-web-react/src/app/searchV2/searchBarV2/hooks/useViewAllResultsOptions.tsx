/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
