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
