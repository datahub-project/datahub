import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';

import { SHOW_SIBLINGS_URL_PARAM } from '@app/searchV2/siblings/useShowSiblings';
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
    // "Show siblings" is carried over from the current URL, so it survives paging, filtering and new
    // searches. Pages other than search don't have the param, so navigating from them leaves it off.
    const keepShowSiblings =
        new URLSearchParams(history.location?.search || '').get(SHOW_SIBLINGS_URL_PARAM) === 'true';
    const search = QueryString.stringify(
        {
            ...filtersToQueryStringParams(filters || []),
            query: encodeURIComponent(query || ''),
            page,
            unionType,
            sortOption: selectedSortOption,
            [SHOW_SIBLINGS_URL_PARAM]: keepShowSiblings ? 'true' : undefined,
        },
        { arrayFormat: 'comma' },
    );

    history.push({
        pathname: `${PageRoutes.SEARCH}`,
        search,
    });
};
