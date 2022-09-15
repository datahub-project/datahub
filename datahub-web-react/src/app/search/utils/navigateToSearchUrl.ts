import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';

import filtersToQueryStringParams from './filtersToQueryStringParams';
import { EntityType, FacetFilterInput } from '../../../types.generated';
import { PageRoutes } from '../../../conf/Global';
import { UnionType } from './constants';

export const navigateToSearchUrl = ({
    type: newType,
    query: newQuery,
    page: newPage = 1,
    filters: newFilters,
    unionType = UnionType.AND,
    history,
}: {
    type?: EntityType;
    query?: string;
    page?: number;
    filters?: Array<FacetFilterInput>;
    history: RouteComponentProps['history'];
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
        },
        { arrayFormat: 'comma' },
    );

    history.push({
        pathname: `${PageRoutes.SEARCH}`,
        search,
    });
};
