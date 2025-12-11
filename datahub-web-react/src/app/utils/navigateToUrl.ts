/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';

interface Args {
    location: {
        search: string;
        pathname: string;
    };
    history: RouteComponentProps['history'];
    urlParam: string;
    value: string;
}

export default function navigateToUrl({ location, history, urlParam, value }: Args) {
    const parsedSearch = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const newSearch = { ...parsedSearch, [urlParam]: value };

    history.push({
        pathname: location.pathname,
        search: QueryString.stringify(newSearch, { arrayFormat: 'comma' }),
    });
}
