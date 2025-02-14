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
