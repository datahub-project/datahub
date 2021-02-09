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
    const search = QueryString.stringify(
        {
            ...filtersToQueryStringParams(newFilters),
            query: newQuery,
            page: newPage,
        },
        { arrayFormat: 'comma' },
    );

    history.push({
        pathname: `${PageRoutes.SEARCH}/${newType ? entityRegistry.getPathName(newType) : ''}`,
        search,
    });
};
