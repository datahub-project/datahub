/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';
import { useEffect } from 'react';
import { useHistory, useLocation } from 'react-router';

export default function useUpdateSchemaFilterQueryString(filterText: string) {
    const location = useLocation();
    const history = useHistory();
    const parsedParams = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const newParams = {
        ...parsedParams,
        schemaFilter: filterText,
    };
    const stringifiedParams = QueryString.stringify(newParams, { arrayFormat: 'comma' });

    useEffect(() => {
        history.replace({
            pathname: location.pathname,
            search: stringifiedParams,
        });
    }, [filterText, history, location.pathname, stringifiedParams]);
}
