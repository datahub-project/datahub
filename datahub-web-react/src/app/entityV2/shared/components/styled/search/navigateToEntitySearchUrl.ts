/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router';

import { UnionType } from '@app/search/utils/constants';
import filtersToQueryStringParams from '@app/search/utils/filtersToQueryStringParams';

import { EntityType, FacetFilterInput } from '@types';

export const navigateToEntitySearchUrl = ({
    baseUrl,
    baseParams,
    type: newType,
    query: newQuery,
    page: newPage = 1,
    filters: newFilters,
    history,
    unionType,
}: {
    baseUrl: string;
    baseParams: Record<string, string | boolean>;
    type?: EntityType;
    query?: string;
    page?: number;
    filters?: Array<FacetFilterInput>;
    history: RouteComponentProps['history'];
    unionType: UnionType;
}) => {
    const constructedFilters = newFilters || [];
    if (newType) {
        constructedFilters.push({ field: 'entity', values: [newType] });
    }

    const search = QueryString.stringify(
        {
            ...baseParams,
            ...filtersToQueryStringParams(constructedFilters),
            query: newQuery,
            page: newPage,
            unionType,
        },
        { arrayFormat: 'comma' },
    );

    history.replace({
        pathname: `${baseUrl}`,
        search,
    });
};
