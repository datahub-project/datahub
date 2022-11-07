import * as QueryString from 'query-string';
import { Location, History } from 'history';

type QueryParam = {
    [key: string]: string | undefined;
};

export default function updateQueryParams(newParams: QueryParam, location: Location, history: History) {
    const parsedParams = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const updatedParams = {
        ...parsedParams,
        ...newParams,
    };
    const stringifiedParams = QueryString.stringify(updatedParams, { arrayFormat: 'comma' });

    history.push({
        pathname: location.pathname,
        search: stringifiedParams,
    });
}
