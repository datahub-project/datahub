/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
