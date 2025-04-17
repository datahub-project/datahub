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
