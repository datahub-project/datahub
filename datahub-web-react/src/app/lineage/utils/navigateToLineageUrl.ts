import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router-dom';

export const navigateToLineageUrl = ({
    location,
    history,
    isLineageMode,
}: {
    location: {
        search: string;
        pathname: string;
    };
    history: RouteComponentProps['history'];
    isLineageMode: boolean;
}) => {
    const parsedSearch = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const newSearch = {
        ...parsedSearch,
        is_lineage_mode: isLineageMode,
    };
    const newSearchStringified = QueryString.stringify(newSearch, { arrayFormat: 'comma' });

    history.push({
        pathname: location.pathname,
        search: newSearchStringified,
    });
};
