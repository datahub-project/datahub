import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';

import filtersToQueryStringParams from './filtersToQueryStringParams';
import { EntityType, FacetFilterInput } from '../../../types.generated';
import { PageRoutes } from '../../../conf/Global';

export const navigateToSearchUrl = ({
    type: newType,
    query: newQuery,
    page: newPage = 1,
    filters: newFilters,
    history,
}: {
    type?: EntityType;
    query?: string;
    page?: number;
    filters?: Array<FacetFilterInput>;
    history: RouteComponentProps['history'];
}) => {
    const constructedFilters = newFilters || [];
    if (newType) {
        constructedFilters.push({ field: 'entity', value: newType });
    }

    const search = QueryString.stringify(
        {
            ...filtersToQueryStringParams(constructedFilters),
            query: encodeURIComponent(newQuery || ''),
            page: newPage,
        },
        { arrayFormat: 'comma' },
    );

    history.push({
        pathname: `${PageRoutes.SEARCH}`,
        search,
    });
};

export const navigateToSearchLineageUrl = ({
    entityUrl,
    query: newQuery,
    page: newPage = 1,
    filters: newFilters,
    history,
}: {
    entityUrl: string;
    query?: string;
    page?: number;
    filters?: Array<FacetFilterInput>;
    history: RouteComponentProps['history'];
}) => {
    const constructedFilters = newFilters || [];

    const search = QueryString.stringify(
        {
            ...filtersToQueryStringParams(constructedFilters),
            query: encodeURIComponent(newQuery || ''),
            page: newPage,
        },
        { arrayFormat: 'comma' },
    );

    history.push({
        pathname: entityUrl,
        search,
    });
};
