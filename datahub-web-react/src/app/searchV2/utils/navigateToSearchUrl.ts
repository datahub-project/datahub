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
    showSiblings,
}: {
    query?: string;
    page?: number;
    filters?: Array<FacetFilterInput>;
    history: RouteComponentProps['history'];
    selectedSortOption?: string;
    unionType?: UnionType;
    /** "Show siblings" on the search page; defaults to the current URL's value, so it survives paging and filtering. */
    showSiblings?: boolean;
}) => {
    const keepShowSiblings =
        showSiblings ?? new URLSearchParams(history.location?.search || '').get(SHOW_SIBLINGS_URL_PARAM) === 'true';
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
