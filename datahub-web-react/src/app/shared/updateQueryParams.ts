import { History, Location } from 'history';
import * as QueryString from 'query-string';

type QueryParam = {
    [key: string]: string | undefined;
};

export default function updateQueryParams(newParams: QueryParam, location: Location, history: History) {
    const searchParams = new URLSearchParams(location.search);

    // Merge in new params
    Object.entries(newParams).forEach(([key, value]) => {
        if (value === undefined) {
            searchParams.delete(key);
        } else {
            searchParams.set(key, value);
        }
    });

    history.replace({
        pathname: location.pathname,
        search: searchParams.toString(),
    });
}
