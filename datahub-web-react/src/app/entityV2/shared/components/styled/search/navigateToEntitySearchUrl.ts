import { RouteComponentProps } from 'react-router';
import * as QueryString from 'query-string';
import { EntityType, FacetFilterInput } from '../../../../../../types.generated';
import filtersToQueryStringParams from '../../../../../search/utils/filtersToQueryStringParams';
import { UnionType } from '../../../../../search/utils/constants';

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
