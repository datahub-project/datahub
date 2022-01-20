import { RouteComponentProps } from 'react-router';
import * as QueryString from 'query-string';
import { EntityType, FacetFilterInput } from '../../../../../../types.generated';
import filtersToQueryStringParams from '../../../../../search/utils/filtersToQueryStringParams';

export const navigateToEntitySearchUrl = ({
    baseUrl,
    type: newType,
    query: newQuery,
    page: newPage = 1,
    filters: newFilters,
    history,
}: {
    baseUrl: string;
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
            query: newQuery,
            page: newPage,
        },
        { arrayFormat: 'comma' },
    );

    history.push({
        pathname: `${baseUrl}`,
        search,
    });
};
