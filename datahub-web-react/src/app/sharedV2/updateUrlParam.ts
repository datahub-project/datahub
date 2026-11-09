import * as QueryString from 'query-string';
import { RouteComponentProps } from 'react-router';

/** Sets {@code key} to {@code value} in the current URL, or removes it when {@code value} is null. */
export function updateUrlParam(
    history: RouteComponentProps['history'],
    key: string,
    value: string | null,
    state?: any,
) {
    // Parse existing params without decoding so percent-encoded values (e.g. filter_degree=3%2B) are
    // preserved byte-for-byte. The native URL.searchParams API would decode %2B → + and re-encode it
    // as %20 (space), which breaks the GMS degree filter validation.
    const parsedParams = QueryString.parse(history.location.search, { arrayFormat: 'comma', decode: false });
    const updatedParams = { ...parsedParams };
    if (value === null) {
        delete updatedParams[key];
    } else {
        updatedParams[key] = encodeURIComponent(value);
    }
    const newSearch = QueryString.stringify(updatedParams, { arrayFormat: 'comma', encode: false });
    history.replace(`?${newSearch}`, state);
}
