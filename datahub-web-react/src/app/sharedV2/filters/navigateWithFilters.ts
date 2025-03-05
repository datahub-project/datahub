import { FacetFilterInput } from '@src/types.generated';
import * as QueryString from 'query-string';
import filtersToQueryStringParams from '../../searchV2/utils/filtersToQueryStringParams';

export const navigateWithFilters = ({
    filters,
    page = 1,
    history,
    location,
}: {
    filters?: Array<FacetFilterInput>;
    page?: number;
    history: any;
    location: any;
}) => {
    const search = QueryString.stringify(
        {
            ...filtersToQueryStringParams(filters || []),
            page,
        },
        { arrayFormat: 'comma' },
    );

    history.replace({
        pathname: location.pathname, // Keep the current pathname
        search,
    });
};
