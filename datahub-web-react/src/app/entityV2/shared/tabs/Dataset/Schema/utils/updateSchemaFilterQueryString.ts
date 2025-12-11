/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';
import { useHistory, useLocation } from 'react-router';
import { useDebounce } from 'react-use';

import { SchemaFilterType } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows';

export default function useUpdateSchemaFilterQueryString(
    filterText: string,
    expandedDrawerFieldPath: string | null,
    schemaFilterTypes: SchemaFilterType[],
) {
    const location = useLocation();
    const history = useHistory();
    const parsedParams = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const newParams = {
        ...parsedParams,
        schemaFilter: filterText,
        highlightedPath: expandedDrawerFieldPath,
        schemaFilterTypes: schemaFilterTypes.length === 4 ? [] : schemaFilterTypes,
    };
    const stringifiedParams = QueryString.stringify(newParams, { arrayFormat: 'comma' });

    useDebounce(
        () => {
            history.replace({
                pathname: location.pathname,
                search: stringifiedParams,
            });
        },
        500,
        [filterText, history, location.pathname, stringifiedParams, expandedDrawerFieldPath, schemaFilterTypes],
    );
}
