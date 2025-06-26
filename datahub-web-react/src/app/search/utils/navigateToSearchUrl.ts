import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';

import { UnionType } from '@app/search/utils/constants';
import filtersToQueryStringParams from '@app/search/utils/filtersToQueryStringParams';
import { PageRoutes } from '@conf/Global';

import { EntityType, FacetFilterInput } from '@types';

export const navigateToSearchUrl = ({
    type: newType,
    query: newQuery,
    page: newPage = 1,
    filters: newFilters,
    unionType = UnionType.AND,
    selectedSortOption,
    history,
}: {
    type?: EntityType;
    query?: string;
    page?: number;
    filters?: Array<FacetFilterInput>;
    history: RouteComponentProps['history'];
    selectedSortOption?: string;
    unionType?: UnionType;
}) => {
    const constructedFilters = newFilters || [];
    if (newType) {
        constructedFilters.push({ field: 'entity', values: [newType] });
    }

    const search = QueryString.stringify(
        {
            ...filtersToQueryStringParams(constructedFilters),
            query: encodeURIComponent(newQuery || ''),
            page: newPage,
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
