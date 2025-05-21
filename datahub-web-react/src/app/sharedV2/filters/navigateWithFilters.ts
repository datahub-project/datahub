import * as QueryString from 'query-string';

import filtersToQueryStringParams from '@app/searchV2/utils/filtersToQueryStringParams';
import { FacetFilterInput } from '@src/types.generated';

export const navigateWithFilters = ({
    filters,
    history,
    location,
}: {
    filters?: Array<FacetFilterInput>;
    history: any;
    location: any;
}) => {
    const search = QueryString.stringify(
        {
            ...filtersToQueryStringParams(filters || []),
        },
        { arrayFormat: 'comma' },
    );

    history.replace({
        pathname: location.pathname, // Keep the current pathname
        search,
    });
};
