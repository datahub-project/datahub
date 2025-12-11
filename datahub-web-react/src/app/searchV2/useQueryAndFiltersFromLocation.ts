/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';
import { useMemo } from 'react';
import { useLocation } from 'react-router';

import useFilters from '@app/searchV2/utils/useFilters';
import { PageRoutes } from '@conf/Global';

import { FacetFilterInput } from '@types';

const isSearchResultPage = (path: string) => {
    return path.startsWith(PageRoutes.SEARCH);
};

export default function useQueryAndFiltersFromLocation() {
    const location = useLocation();
    const params = useMemo(() => QueryString.parse(location.search, { arrayFormat: 'comma' }), [location.search]);
    const paramFilters: Array<FacetFilterInput> = useFilters(params);
    const filters = useMemo(
        () => (isSearchResultPage(location.pathname) ? paramFilters : []),
        [location.pathname, paramFilters],
    );
    const query: string = useMemo(
        () =>
            isSearchResultPage(location.pathname)
                ? decodeURIComponent(params.query ? (params.query as string) : '')
                : '',
        [location.pathname, params.query],
    );

    return { filters, query };
}
