/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { History, Location } from 'history';
import * as QueryString from 'query-string';

type QueryParam = {
    [key: string]: string | undefined;
};

// Doesn't support the newParams with special characters
export default function updateQueryParams(newParams: QueryParam, location: Location, history: History) {
    const parsedParams = QueryString.parse(location.search, { arrayFormat: 'comma', decode: false });
    const updatedParams = {
        ...parsedParams,
        ...newParams,
    };
    const stringifiedParams = QueryString.stringify(updatedParams, { arrayFormat: 'comma', encode: false });

    history.replace({
        pathname: location.pathname,
        search: stringifiedParams,
    });
}
