/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';

import { UnionType } from '@app/searchV2/utils/constants';
import filtersToQueryStringParams from '@app/searchV2/utils/filtersToQueryStringParams';
import { PageRoutes } from '@conf/Global';

import { FacetFilterInput } from '@types';

export const navigateToSearchUrl = ({
    query,
    filters,
    page = 1,
    unionType = UnionType.AND,
    selectedSortOption,
    history,
}: {
    query?: string;
    page?: number;
    filters?: Array<FacetFilterInput>;
    history: RouteComponentProps['history'];
    selectedSortOption?: string;
    unionType?: UnionType;
}) => {
    const search = QueryString.stringify(
        {
            ...filtersToQueryStringParams(filters || []),
            query: encodeURIComponent(query || ''),
            page,
            unionType,
            sortOption: selectedSortOption,
        },
        { arrayFormat: 'comma' },
    );

    history.push({
        pathname: `${PageRoutes.SEARCH}`,
        search,
    });
};
