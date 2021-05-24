import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';

import filtersToQueryStringParams from './filtersToQueryStringParams';
import { EntityType, FacetFilterInput } from '../../../types.generated';
import { PageRoutes } from '../../../conf/Global';
import EntityRegistry from '../../entity/EntityRegistry';

export const navigateToSearchUrl = ({
    type: newType,
    query: newQuery,
    page: newPage = 1,
    filters: newFilters,
    history,
    entityRegistry,
}: {
    type?: EntityType;
    query?: string;
    page?: number;
    filters?: Array<FacetFilterInput>;
    history: RouteComponentProps['history'];
    entityRegistry: EntityRegistry;
}) => {
    const updatedQuery = newQuery && newQuery.startsWith('ExploreEntity-') ? newQuery.split('-')[2] : newQuery;
    const updatedType =
        newQuery && newQuery.startsWith('ExploreEntity-') ? (newQuery.split('-')[1] as EntityType) : newType;
    const search = QueryString.stringify(
        {
            ...filtersToQueryStringParams(newFilters),
            query: updatedQuery,
            page: newPage,
        },
        { arrayFormat: 'comma' },
    );

    history.push({
        pathname: `${PageRoutes.SEARCH}/${updatedType ? entityRegistry.getPathName(updatedType) : ''}`,
        search,
    });
};
