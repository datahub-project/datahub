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

export const navigateToVersionedDatasetUrl = ({
    location,
    history,
    datasetVersion,
}: {
    location: {
        search: string;
        pathname: string;
    };
    history: RouteComponentProps['history'];
    datasetVersion: string;
}) => {
    const parsedSearch = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const newSearch = {
        ...parsedSearch,
        semantic_version: datasetVersion,
    };
    const newSearchStringified = QueryString.stringify(newSearch, { arrayFormat: 'comma' });

    history.push({
        pathname: location.pathname,
        search: newSearchStringified,
    });
};
