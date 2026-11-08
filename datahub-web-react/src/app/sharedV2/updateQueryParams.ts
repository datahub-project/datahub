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

    const newSearch = stringifiedParams ? `?${stringifiedParams}` : '';
    if (location.search === newSearch) {
        return;
    }

    history.replace(
        {
            pathname: location.pathname,
            search: stringifiedParams,
        },
        location.state,
    );
}
